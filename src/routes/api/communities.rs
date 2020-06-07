use crate::routes::api::{RespMinimalAuthorInfo, RespMinimalCommunityInfo, RespPostListPost};
use std::sync::Arc;

async fn route_unstable_communities_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt(
            "SELECT name, local, ap_id FROM community WHERE id=$1",
            &[&community_id],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "No such community",
            ))
        })?;

    let community_local = row.get(1);

    let info = RespMinimalCommunityInfo {
        id: community_id,
        name: row.get(0),
        local: community_local,
        host: if community_local {
            crate::get_url_host(&ctx.host_url_apub).unwrap().into()
        } else {
            match row.get::<_, Option<&str>>(2).and_then(crate::get_url_host) {
                Some(host) => host.into(),
                None => "[unknown]".into(),
            }
        },
    };

    let body = serde_json::to_vec(&info)?;

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
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

async fn route_unstable_communities_posts_list(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    use futures::stream::TryStreamExt;

    let db = ctx.db_pool.get().await?;

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    let community_row = db
        .query_opt(
            "SELECT name, local, ap_id FROM community WHERE id=$1",
            &[&community_id],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "No such community",
            ))
        })?;

    let community = {
        let row = &community_row;
        let community_local = row.get(1);

        RespMinimalCommunityInfo {
            id: community_id,
            name: row.get(0),
            local: community_local,
            host: if community_local {
                (&local_hostname).into()
            } else {
                match row.get::<_, Option<&str>>(2).and_then(crate::get_url_host) {
                    Some(host) => host.into(),
                    None => "[unknown]".into(),
                }
            },
        }
    };

    let limit: i64 = 10; // TODO make configurable

    let values: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&community_id, &limit];

    let stream = db.query_raw(
        "SELECT post.id, post.author, post.href, post.title, post.created, person.username, person.local, person.ap_id FROM post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = $1 ORDER BY created DESC LIMIT $2",
        values.iter().map(|s| *s as _)
    ).await?;

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    let posts: Vec<serde_json::Value> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id: i64 = row.get(0);
            let author_id: Option<i64> = row.get(1);
            let href: &str = row.get(2);
            let title: &str = row.get(3);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);

            let author = author_id.map(|id| {
                let author_name: &str = row.get(5);
                let author_local: bool = row.get(6);
                let author_ap_id: Option<&str> = row.get(7);
                RespMinimalAuthorInfo {
                    id,
                    username: author_name,
                    local: author_local,
                    host: if author_local {
                        (&local_hostname).into()
                    } else {
                        match author_ap_id.and_then(crate::get_url_host) {
                            Some(host) => host.into(),
                            None => "[unknown]".into(),
                        }
                    },
                }
            });

            let post = RespPostListPost {
                id,
                title,
                href,
                author: author.as_ref(),
                created: &created.to_rfc3339(),
                community: &community,
            };

            futures::future::ready(serde_json::to_value(&post).map_err(Into::into))
        })
        .try_collect()
        .await?;

    let body = serde_json::to_vec(&posts)?;

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

pub fn route_communities() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child_parse::<i64, _>(
        crate::RouteNode::new()
            .with_handler_async("GET", route_unstable_communities_get)
            .with_child(
                "follow",
                crate::RouteNode::new()
                    .with_handler_async("POST", route_unstable_communities_follow),
            )
            .with_child(
                "posts",
                crate::RouteNode::new()
                    .with_handler_async("GET", route_unstable_communities_posts_list),
            ),
    )
}
