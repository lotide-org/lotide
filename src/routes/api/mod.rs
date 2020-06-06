use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Serialize)]
struct RespMinimalAuthorInfo<'a> {
    id: i64,
    username: &'a str,
    local: bool,
    host: Cow<'a, str>,
}

#[derive(Serialize)]
struct RespMinimalCommunityInfo<'a> {
    id: i64,
    name: &'a str,
    local: bool,
    host: Cow<'a, str>,
}

#[derive(Serialize)]
struct RespPostListPost<'a> {
    id: i64,
    title: &'a str,
    href: &'a str,
    author: Option<&'a RespMinimalAuthorInfo<'a>>,
    created: &'a str,
    community: &'a RespMinimalCommunityInfo<'a>,
}

fn hostname(url: &str) -> Option<String> {
    url::Url::parse(url).ok().and_then(|url| {
        url.host_str()
            .map(|host| {
                match url.port() {
                    Some(port) => format!("{}:{}", host, port),
                    None => host.to_owned(),
                }
            })
    })
}

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
            )
            .with_child(
                "posts",
                crate::RouteNode::new().with_handler_async("POST", route_unstable_posts_create),
            )
            .with_child(
                "users",
                crate::RouteNode::new()
                .with_child(
                    "me",
                    crate::RouteNode::new()
                    .with_child(
                        "following:posts",
                        crate::RouteNode::new().with_handler_async("GET", route_unstable_users_me_following_posts_list),
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
            "INSERT INTO login (token, person, created) VALUES ($1, $2, current_timestamp)",
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

async fn route_unstable_posts_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct PostsCreateBody {
        community: i64,
        href: String,
        title: String,
    }

    let body: PostsCreateBody = serde_json::from_slice(&body)?;

    // TODO validate permissions to post

    let res_row = db.query_one(
        "INSERT INTO post (author, href, title, created, community, local) VALUES ($1, $2, $3, current_timestamp, $4, TRUE) RETURNING id, created",
        &[&user, &body.href, &body.title, &body.community],
    ).await?;

    tokio::spawn(async move {
        let id = res_row.get(0);
        let created = res_row.get(1);

        let post = crate::PostInfo {
            id,
            author: Some(user),
            href: &body.href,
            title: &body.title,
            created: &created,
            community: body.community,
        };

        crate::apub_util::send_post_to_community(post, ctx).await
    });

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn route_unstable_users_me_following_posts_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use futures::stream::TryStreamExt;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let limit: i64 = 10; // TODO make configurable

    let values: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&user, &limit];

    let stream = db.query_raw(
        "SELECT post.id, post.author, post.href, post.title, post.created, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND community.id IN (SELECT community FROM community_follow WHERE follower=$1) ORDER BY created DESC LIMIT $2",
        values.iter().map(|s| *s as _)
    ).await?;

    let local_hostname = hostname(&ctx.host_url_apub).unwrap();

    let posts: Vec<serde_json::Value> = stream.map_err(crate::Error::from).and_then(|row| {
        let id: i64 = row.get(0);
        let author_id: Option<i64> = row.get(1);
        let href: &str = row.get(2);
        let title: &str = row.get(3);
        let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);
        let community_id: i64 = row.get(5);
        let community_name: &str = row.get(6);
        let community_local: bool = row.get(7);
        let community_ap_id: Option<&str> = row.get(8);

        let author = author_id.map(|id| {
            let author_name: &str = row.get(9);
            let author_local: bool = row.get(10);
            let author_ap_id: Option<&str> = row.get(11);
            RespMinimalAuthorInfo {
                id,
                username: author_name,
                local: author_local,
                host: if author_local {
                    (&local_hostname).into()
                } else {
                    match author_ap_id.and_then(hostname) {
                        Some(host) => host.into(),
                        None => "[unknown]".into(),
                    }
                },
            }
        });

        let community = RespMinimalCommunityInfo {
            id: community_id,
            name: community_name,
            local: community_local,
            host: if community_local {
                (&local_hostname).into()
            } else {
                match community_ap_id.and_then(hostname) {
                    Some(host) => host.into(),
                    None => "[unknown]".into(),
                }
            },
        };

        let post = RespPostListPost {
            id,
            title,
            href,
            author: author.as_ref(),
            created: &created.to_rfc3339(),
            community: &community,
        };

        futures::future::ready(serde_json::to_value(&post).map_err(Into::into))
    }).try_collect().await?;

    let body = serde_json::to_vec(&posts)?;

    Ok(hyper::Response::builder()
       .header(hyper::header::CONTENT_TYPE, "application/json")
       .body(body.into())?)
}
