use crate::routes::api::{
    MaybeIncludeYour, RespMinimalAuthorInfo, RespMinimalCommunityInfo, RespPostListPost,
};
use serde_derive::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize)]
struct RespCommunityInfo<'a> {
    #[serde(flatten)]
    base: &'a RespMinimalCommunityInfo<'a>,
    your_follow: Option<Option<RespYourFollowInfo>>,
}

#[derive(Serialize)]
struct RespYourFollowInfo {
    accepted: bool,
}

async fn route_unstable_communities_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let rows = db
        .query("SELECT id, local, ap_id, name FROM community", &[])
        .await?;

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    let output: Vec<_> = rows
        .iter()
        .map(|row| {
            let id = row.get(0);
            let name = row.get(3);
            let local = row.get(1);
            let host = crate::get_actor_host_or_unknown(local, row.get(2), &local_hostname);

            RespMinimalCommunityInfo {
                id,
                name,
                local,
                host,
            }
        })
        .collect();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&output)?.into())?)
}

async fn route_unstable_communities_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunitiesCreateBody<'a> {
        name: &'a str,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommunitiesCreateBody<'_> = serde_json::from_slice(&body)?;

    for ch in body.name.chars() {
        if !super::USERNAME_ALLOWED_CHARS.contains(&ch) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Community name contains disallowed characters",
            )));
        }
    }

    let rsa = openssl::rsa::Rsa::generate(crate::KEY_BITS)?;
    let private_key = rsa.private_key_to_pem()?;
    let public_key = rsa.public_key_to_pem()?;

    let row = db
        .query_one(
            "INSERT INTO community (name, local, private_key, public_key) VALUES ($1, TRUE, $2, $3) RETURNING id",
            &[&body.name, &private_key, &public_key],
        )
        .await?;

    let community_id: i64 = row.get(0);

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::to_vec(&serde_json::json!({"community": {"id": community_id}}))?.into(),
        )?)
}

async fn route_unstable_communities_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let query: MaybeIncludeYour = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let db = ctx.db_pool.get().await?;

    let row = {
        (if query.include_your {
            let user = crate::require_login(&req, &db).await?;
            db.query_opt(
                "SELECT name, local, ap_id, (SELECT accepted FROM community_follow WHERE community=community.id AND follower=$2) FROM community WHERE id=$1",
                &[&community_id, &user],
            ).await?
        } else {
            db.query_opt(
                "SELECT name, local, ap_id FROM community WHERE id=$1",
                &[&community_id],
            ).await?
        })
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "No such community",
            ))
        })?
    };

    let community_local = row.get(1);

    let info = RespCommunityInfo {
        base: &RespMinimalCommunityInfo {
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
        },
        your_follow: if query.include_your {
            Some(match row.get(3) {
                Some(accepted) => Some(RespYourFollowInfo { accepted }),
                None => None,
            })
        } else {
            None
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

    #[derive(Deserialize)]
    struct CommunitiesFollowBody {
        #[serde(default)]
        try_wait_for_accept: bool,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommunitiesFollowBody = serde_json::from_slice(&body)?;

    let row = db
        .query_opt("SELECT local FROM community WHERE id=$1", &[&community])
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                "No such community",
            ))
        })?;

    let community_local: bool = row.get(0);

    let row_count = db.execute("INSERT INTO community_follow (community, follower, local, accepted) VALUES ($1, $2, TRUE, $3) ON CONFLICT DO NOTHING", &[&community, &user, &community_local]).await?;

    let output =
        if community_local {
            RespYourFollowInfo { accepted: true }
        } else {
            if row_count > 0 {
                crate::apub_util::spawn_enqueue_send_community_follow(community, user, ctx);

                if body.try_wait_for_accept {
                    tokio::time::delay_for(std::time::Duration::from_millis(500)).await;

                    let row = db.query_one(
                    "SELECT accepted FROM community_follow WHERE community=$1 AND follower=$2",
                    &[&community, &user],
                ).await?;

                    RespYourFollowInfo {
                        accepted: row.get(0),
                    }
                } else {
                    RespYourFollowInfo { accepted: false }
                }
            } else {
                let row = db
                    .query_one(
                        "SELECT accepted FROM community_follow WHERE community=$1 AND follower=$2",
                        &[&community, &user],
                    )
                    .await?;

                RespYourFollowInfo {
                    accepted: row.get(0),
                }
            }
        };

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&output)?.into())?)
}

async fn route_unstable_communities_unfollow(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community,) = params;
    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let new_undo = {
        let trans = db.transaction().await?;

        let row_count = trans
            .execute(
                "DELETE FROM community_follow WHERE community=$1 AND follower=$2",
                &[&community, &user],
            )
            .await?;

        if row_count > 0 {
            let id = uuid::Uuid::new_v4();
            trans.execute(
                "INSERT INTO local_community_follow_undo (id, community, follower) VALUES ($1, $2, $3)",
                &[&id, &community, &user],
            ).await?;

            trans.commit().await?;

            Some(id)
        } else {
            None
        }
    };

    if let Some(new_undo) = new_undo {
        crate::apub_util::spawn_enqueue_send_community_follow_undo(new_undo, community, user, ctx);
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
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, post.content_html, person.username, person.local, person.ap_id FROM post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = $1 AND post.deleted=FALSE ORDER BY hot_rank((SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author), post.created) DESC LIMIT $2",
        values.iter().map(|s| *s as _)
    ).await?;

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    let posts: Vec<serde_json::Value> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id: i64 = row.get(0);
            let author_id: Option<i64> = row.get(1);
            let href: Option<&str> = row.get(2);
            let content_text: Option<&str> = row.get(3);
            let content_html: Option<&str> = row.get(6);
            let title: &str = row.get(4);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(5);

            let author = author_id.map(|id| {
                let author_name: &str = row.get(7);
                let author_local: bool = row.get(8);
                let author_ap_id: Option<&str> = row.get(9);
                RespMinimalAuthorInfo {
                    id,
                    username: author_name.into(),
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
                content_text,
                content_html,
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
    crate::RouteNode::new()
        .with_handler_async("GET", route_unstable_communities_list)
        .with_handler_async("POST", route_unstable_communities_create)
        .with_child_parse::<i64, _>(
            crate::RouteNode::new()
                .with_handler_async("GET", route_unstable_communities_get)
                .with_child(
                    "follow",
                    crate::RouteNode::new()
                        .with_handler_async("POST", route_unstable_communities_follow),
                )
                .with_child(
                    "unfollow",
                    crate::RouteNode::new()
                        .with_handler_async("POST", route_unstable_communities_unfollow),
                )
                .with_child(
                    "posts",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_communities_posts_list),
                ),
        )
}
