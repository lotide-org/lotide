use activitystreams::ext::Extensible;
use std::sync::Arc;

pub fn route_apub() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child(
            "users",
            crate::RouteNode::new().with_child_parse::<i64, _>(
                crate::RouteNode::new().with_handler_async("GET", handler_users_get),
            ),
        )
        .with_child(
            "communities",
            crate::RouteNode::new().with_child_parse::<i64, _>(
                crate::RouteNode::new().with_handler_async("GET", handler_communities_get),
            ),
        )
}

async fn handler_users_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT username, local FROM person WHERE id=$1",
            &[&user_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such user",
        )),
        Some(row) => {
            let username: String = row.get(0);
            let local: bool = row.get(1);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested user is not owned by this instance",
                )));
            }

            let mut info = activitystreams::actor::Person::new();
            info.as_mut()
                .set_id(crate::apub_util::get_local_person_apub_id(
                    user_id,
                    &ctx.host_url_apub,
                ))?
                .set_name_xsd_string(username)?;

            let mut actor_props = activitystreams::actor::properties::ApActorProperties::default();

            actor_props.set_inbox(format!("{}/users/{}/inbox", ctx.host_url_apub, user_id))?;

            let info = info.extend(actor_props);

            let mut resp = hyper::Response::new(serde_json::to_vec(&info)?.into());
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("application/activity+json"),
            );

            Ok(resp)
        }
    }
}

async fn handler_communities_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT name, local FROM community WHERE id=$1",
            &[&community_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such community",
        )),
        Some(row) => {
            let name: String = row.get(0);
            let local: bool = row.get(1);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested community does not owned by this instance",
                )));
            }

            let mut info = activitystreams::actor::Group::new();
            info.as_mut()
                .set_id(crate::apub_util::get_local_community_apub_id(
                    community_id,
                    &ctx.host_url_apub,
                ))?
                .set_name_xsd_string(name)?;

            let mut actor_props = activitystreams::actor::properties::ApActorProperties::default();

            actor_props.set_inbox(format!(
                "{}/communities/{}/inbox",
                ctx.host_url_apub, community_id
            ))?;

            let info = info.extend(actor_props);

            let mut resp = hyper::Response::new(serde_json::to_vec(&info)?.into());
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("application/activity+json"),
            );

            Ok(resp)
        }
    }
}
