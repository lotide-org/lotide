use serde_derive::Deserialize;
use std::sync::Arc;

pub fn route_api() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child(
        "unstable",
        crate::RouteNode::new()
            .with_child(
                "actors:lookup",
                crate::RouteNode::new().with_child_str(
                    crate::RouteNode::new().with_handler_async("GET", route_unstable_actors_lookup),
                ),
            )
            .with_child(
                "logins",
                crate::RouteNode::new().with_handler_async("POST", route_unstable_logins_create),
            )
            .with_child(
                "communities",
                crate::RouteNode::new().with_child_parse::<i64, _>(
                    crate::RouteNode::new().with_child(
                        "follow",
                        crate::RouteNode::new()
                            .with_handler_async("POST", route_unstable_communities_follow),
                    ),
                ),
            ),
    )
}

async fn route_unstable_actors_lookup(
    params: (String,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (query,) = params;
    println!("lookup {}", query);

    let db = ctx.db_pool.get().await?;

    let uri = query.parse::<hyper::Uri>()?;

    let res = ctx
        .http_client
        .request(
            hyper::Request::get(uri)
                .header(hyper::header::ACCEPT, crate::apub_util::ACTIVITY_TYPE)
                .body(Default::default())?,
        )
        .await?;

    let body = hyper::body::to_bytes(res.into_body()).await?;

    let group: activitystreams::ext::Ext<
        activitystreams::actor::Group,
        activitystreams::actor::properties::ApActorProperties,
    > = serde_json::from_slice(&body)?;

    let name = group.as_ref().get_name_xsd_string();
    let ap_inbox = group.extension.get_inbox();

    if let Some(name) = name {
        db.execute(
            "INSERT INTO community (name, local, ap_id, ap_inbox) VALUES ($1, FALSE, $2, $3)",
            &[&name.as_str(), &query, &ap_inbox.as_str()],
        )
        .await?;
    }

    Ok(crate::simple_response(
        hyper::StatusCode::ACCEPTED,
        "accepted",
    ))
}

async fn route_unstable_logins_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct LoginsCreateBody<'a> {
        username: &'a str,
        password: &'a str,
    }

    let body: LoginsCreateBody<'_> = serde_json::from_slice(&body)?;

    let row = db
        .query_opt(
            "SELECT id, passhash FROM person WHERE username=$1 AND local",
            &[&body.username],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "No local user found by that name",
            ))
        })?;

    let id: i64 = row.get(0);
    let passhash: Option<String> = row.get(1);

    let passhash = passhash.ok_or_else(|| {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "No password set for this user",
        ))
    })?;

    let req_password = body.password.to_owned();

    let correct =
        tokio::task::spawn_blocking(move || bcrypt::verify(req_password, &passhash)).await??;

    if correct {
        let token = uuid::Uuid::new_v4();
        db.execute(
            "INSERT INTO login (token, person, created) VALUES ($1, $2, localtimestamp)",
            &[&token, &id],
        )
        .await?;

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&serde_json::json!({"token": token.to_string()}))?.into())?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            "Incorrect password",
        ))
    }
}

async fn route_unstable_communities_follow(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community,) = params;
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let row_count = db.execute("INSERT INTO community_follow (community, follower) VALUES ($1, $2) ON CONFLICT DO NOTHING", &[&community, &user]).await?;

    if row_count > 0 {
        crate::spawn_task(crate::apub_util::send_community_follow(
            community, user, ctx,
        ));
    }

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}
