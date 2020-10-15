use crate::routes::api::{
    MaybeIncludeYour, RespAvatarInfo, RespMinimalAuthorInfo, RespMinimalCommunityInfo,
    RespPostListPost,
};
use crate::{CommunityLocalID, PostLocalID, UserLocalID};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Serialize)]
struct RespCommunityInfo<'a> {
    #[serde(flatten)]
    base: &'a RespMinimalCommunityInfo<'a>,

    description: &'a str,

    #[serde(skip_serializing_if = "Option::is_none")]
    you_are_moderator: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

    let output: Vec<_> = rows
        .iter()
        .map(|row| {
            let id = CommunityLocalID(row.get(0));
            let local = row.get(1);
            let ap_id = row.get(2);
            let name = row.get(3);

            let host = crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname);

            RespMinimalCommunityInfo {
                id,
                name,
                local,
                host,
                remote_url: ap_id,
            }
        })
        .collect();

    crate::json_response(&output)
}

async fn route_unstable_communities_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);

    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

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
                lang.tr("community_name_disallowed_chars", None)
                    .into_owned(),
            )));
        }
    }

    let rsa = openssl::rsa::Rsa::generate(crate::KEY_BITS)?;
    let private_key = rsa.private_key_to_pem()?;
    let public_key = rsa.public_key_to_pem()?;

    let community_id = {
        let trans = db.transaction().await?;

        trans
            .execute(
                "INSERT INTO local_actor_name (name) VALUES ($1)",
                &[&body.name],
            )
            .await
            .map_err(|err| {
                if err.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                    crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr("name_in_use", None).into_owned(),
                    ))
                } else {
                    err.into()
                }
            })?;

        let row = trans
            .query_one(
                "INSERT INTO community (name, local, private_key, public_key, created_by) VALUES ($1, TRUE, $2, $3, $4) RETURNING id",
                &[&body.name, &private_key, &public_key, &user.raw()],
            )
            .await?;

        let community_id = CommunityLocalID(row.get(0));

        trans
            .execute(
                "INSERT INTO community_moderator (community, person) VALUES ($1, $2)",
                &[&community_id, &user],
            )
            .await?;

        trans.commit().await?;

        community_id
    };

    crate::json_response(&serde_json::json!({"community": {"id": community_id}}))
}

async fn route_unstable_communities_get(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let query: MaybeIncludeYour = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let row = {
        (if query.include_your {
            let user = crate::require_login(&req, &db).await?;
            db.query_opt(
                "SELECT name, local, ap_id, description, (SELECT accepted FROM community_follow WHERE community=community.id AND follower=$2), EXISTS(SELECT 1 FROM community_moderator WHERE community=community.id AND person=$2) FROM community WHERE id=$1",
                &[&community_id.raw(), &user.raw()],
            ).await?
        } else {
            db.query_opt(
                "SELECT name, local, ap_id, description FROM community WHERE id=$1",
                &[&community_id.raw()],
            ).await?
        })
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr("no_such_community", None).into_owned(),
            ))
        })?
    };

    let community_local = row.get(1);
    let community_ap_id: Option<&str> = row.get(2);

    let info = RespCommunityInfo {
        base: &RespMinimalCommunityInfo {
            id: community_id,
            name: row.get(0),
            local: community_local,
            host: if community_local {
                (&ctx.local_hostname).into()
            } else {
                match community_ap_id.and_then(crate::get_url_host_from_str) {
                    Some(host) => host.into(),
                    None => "[unknown]".into(),
                }
            },
            remote_url: community_ap_id,
        },
        description: row.get(3),
        you_are_moderator: if query.include_your {
            Some(row.get(5))
        } else {
            None
        },
        your_follow: if query.include_your {
            Some(match row.get(4) {
                Some(accepted) => Some(RespYourFollowInfo { accepted }),
                None => None,
            })
        } else {
            None
        },
    };

    crate::json_response(&info)
}

async fn route_unstable_communities_patch(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunitiesEditBody<'a> {
        description: Option<Cow<'a, str>>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommunitiesEditBody = serde_json::from_slice(&body)?;

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr("community_edit_denied", None).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    if let Some(description) = body.description {
        db.execute(
            "UPDATE community SET description=$1 WHERE id=$2",
            &[&description, &community_id],
        )
        .await?;

        crate::apub_util::spawn_enqueue_send_new_community_update(community_id, ctx);
    }

    Ok(crate::empty_response())
}

async fn route_unstable_communities_follow(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community,) = params;

    let lang = crate::get_lang_for_req(&req);
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
                lang.tr("no_such_community", None).into_owned(),
            ))
        })?;

    let community_local: bool = row.get(0);

    let row_count = db.execute("INSERT INTO community_follow (community, follower, local, accepted) VALUES ($1, $2, TRUE, $3) ON CONFLICT DO NOTHING", &[&community, &user.raw(), &community_local]).await?;

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
                    &[&community, &user.raw()],
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
                        &[&community, &user.raw()],
                    )
                    .await?;

                RespYourFollowInfo {
                    accepted: row.get(0),
                }
            }
        };

    crate::json_response(&output)
}

async fn route_unstable_communities_moderators_list(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let lang = crate::get_lang_for_req(&req);

    let db = ctx.db_pool.get().await?;

    ({
        let row = db
            .query_opt("SELECT 1 FROM community WHERE id=$1", &[&community_id])
            .await?;

        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr("no_such_community", None).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    let rows = db.query(
        "SELECT id, username, local, ap_id, avatar FROM person WHERE id IN (SELECT person FROM community_moderator WHERE community=$1)",
        &[&community_id],
    ).await?;

    let output: Vec<_> = rows
        .iter()
        .map(|row| {
            let local = row.get(2);
            let ap_id = row.get(3);

            RespMinimalAuthorInfo {
                id: UserLocalID(row.get(0)),
                username: Cow::Borrowed(row.get(1)),
                local,
                host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                remote_url: ap_id.map(|x| x.into()),
                avatar: row
                    .get::<_, Option<&str>>(4)
                    .map(|url| RespAvatarInfo { url: url.into() }),
            }
        })
        .collect();

    crate::json_response(&output)
}

async fn route_unstable_communities_moderators_add(
    params: (CommunityLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, user_id) = params;

    let db = ctx.db_pool.get().await?;

    let lang = crate::get_lang_for_req(&req);
    let login_user = crate::require_login(&req, &db).await?;

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &login_user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr("must_be_moderator", None).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    ({
        let row = db
            .query_opt("SELECT local FROM person WHERE id=$1", &[&user_id])
            .await?;

        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr("no_such_user", None).into_owned(),
            ))),
            Some(row) => {
                let local: bool = row.get(0);

                if local {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        lang.tr("moderators_only_local", None).into_owned(),
                    )))
                }
            }
        }
    })?;

    db.execute(
        "INSERT INTO community_moderator (community, person) VALUES ($1, $2)",
        &[&community_id, &user_id],
    )
    .await?;

    Ok(crate::empty_response())
}

async fn route_unstable_communities_moderators_remove(
    params: (CommunityLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, user_id) = params;

    let db = ctx.db_pool.get().await?;

    let lang = crate::get_lang_for_req(&req);
    let login_user = crate::require_login(&req, &db).await?;

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &login_user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr("must_be_moderator", None).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    db.execute(
        "DELETE FROM community_moderator WHERE community=$1 AND person=$2",
        &[&community_id, &user_id],
    )
    .await?;

    Ok(crate::empty_response())
}

async fn route_unstable_communities_unfollow(
    params: (CommunityLocalID,),
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
                &[&community, &user.raw()],
            )
            .await?;

        if row_count > 0 {
            let id = uuid::Uuid::new_v4();
            trans.execute(
                "INSERT INTO local_community_follow_undo (id, community, follower) VALUES ($1, $2, $3)",
                &[&id, &community, &user.raw()],
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
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    fn default_sort() -> super::SortType {
        super::SortType::Hot
    };

    #[derive(Deserialize)]
    struct Query {
        #[serde(default = "default_sort")]
        sort: super::SortType,
        #[serde(default)]
        include_your: bool,
    }

    let query: Query = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    use futures::stream::TryStreamExt;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let include_your_for = if query.include_your {
        let user = crate::require_login(&req, &db).await?;
        Some(user)
    } else {
        None
    };

    let community_row = db
        .query_opt(
            "SELECT name, local, ap_id FROM community WHERE id=$1",
            &[&community_id],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr("no_such_community", None).into_owned(),
            ))
        })?;

    let community = {
        let row = &community_row;
        let community_local = row.get(1);
        let community_ap_id: Option<&str> = row.get(2);

        RespMinimalCommunityInfo {
            id: community_id,
            name: row.get(0),
            local: community_local,
            host: if community_local {
                (&ctx.local_hostname).into()
            } else {
                match community_ap_id.and_then(crate::get_url_host_from_str) {
                    Some(host) => host.into(),
                    None => "[unknown]".into(),
                }
            },
            remote_url: community_ap_id,
        }
    };

    let limit: i64 = 30; // TODO make configurable

    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&community_id, &limit];
    let sql: &str = &format!(
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, post.content_html, person.username, person.local, person.ap_id, person.avatar, (SELECT COUNT(*) FROM post_like WHERE post_like.post = post.id){} FROM post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = $1 AND post.approved=TRUE AND post.deleted=FALSE ORDER BY {} LIMIT $2",
        if let Some(user) = &include_your_for {
            values.push(user);
            ", EXISTS(SELECT 1 FROM post_like WHERE post=post.id AND person=$3)"
        } else {
            ""
        },
        query.sort.post_sort_sql(),
    );

    let stream = db.query_raw(sql, values.iter().map(|s| *s as _)).await?;

    let posts: Vec<serde_json::Value> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id = PostLocalID(row.get(0));
            let author_id = row.get::<_, Option<_>>(1).map(UserLocalID);
            let href: Option<&str> = row.get(2);
            let content_text: Option<&str> = row.get(3);
            let content_html: Option<&str> = row.get(6);
            let title: &str = row.get(4);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(5);

            let author = author_id.map(|id| {
                let author_name: &str = row.get(7);
                let author_local: bool = row.get(8);
                let author_ap_id: Option<&str> = row.get(9);
                let author_avatar: Option<&str> = row.get(10);
                RespMinimalAuthorInfo {
                    id,
                    username: author_name.into(),
                    local: author_local,
                    host: if author_local {
                        (&ctx.local_hostname).into()
                    } else {
                        match author_ap_id.and_then(crate::get_url_host_from_str) {
                            Some(host) => host.into(),
                            None => "[unknown]".into(),
                        }
                    },
                    remote_url: author_ap_id.map(From::from),
                    avatar: author_avatar.map(|url| RespAvatarInfo { url: url.into() }),
                }
            });

            let post = RespPostListPost {
                id,
                title,
                href: ctx.process_href_opt(href, id),
                content_text,
                content_html_safe: content_html.map(|html| ammonia::clean(&html)),
                author: author.as_ref(),
                created: &created.to_rfc3339(),
                community: &community,
                score: row.get(11),
                your_vote: if include_your_for.is_some() {
                    Some(if row.get(12) {
                        Some(crate::Empty {})
                    } else {
                        None
                    })
                } else {
                    None
                },
            };

            futures::future::ready(serde_json::to_value(&post).map_err(Into::into))
        })
        .try_collect()
        .await?;

    crate::json_response(&posts)
}

async fn route_unstable_communities_posts_patch(
    params: (CommunityLocalID, PostLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, post_id) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunityPostEditBody {
        approved: Option<bool>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommunityPostEditBody = serde_json::from_slice(&body)?;

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr("community_edit_denied", None).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    let old_row = db
        .query_opt(
            "SELECT community, approved, local, ap_id FROM post WHERE id=$1",
            &[&post_id],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr("no_such_post", None).into_owned(),
            ))
        })?;

    if community_id != CommunityLocalID(old_row.get(0)) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr("post_not_in_community", None).into_owned(),
        )));
    }

    let old_approved: bool = old_row.get(1);

    let post_ap_id = if old_row.get(2) {
        crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub).into()
    } else {
        std::str::FromStr::from_str(old_row.get(3))?
    };

    if let Some(approved) = body.approved {
        db.execute(
            "UPDATE post SET approved=$1 WHERE id=$2",
            &[&approved, &post_id],
        )
        .await?;

        if approved != old_approved {
            if approved {
                crate::apub_util::spawn_announce_community_post(
                    community_id,
                    post_id,
                    post_ap_id,
                    ctx,
                );
            } else {
                crate::apub_util::spawn_enqueue_send_community_post_announce_undo(
                    community_id,
                    post_id,
                    post_ap_id,
                    ctx,
                );
            }
        }
    }

    Ok(crate::empty_response())
}

pub fn route_communities() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_handler_async("GET", route_unstable_communities_list)
        .with_handler_async("POST", route_unstable_communities_create)
        .with_child_parse::<CommunityLocalID, _>(
            crate::RouteNode::new()
                .with_handler_async("GET", route_unstable_communities_get)
                .with_handler_async("PATCH", route_unstable_communities_patch)
                .with_child(
                    "follow",
                    crate::RouteNode::new()
                        .with_handler_async("POST", route_unstable_communities_follow),
                )
                .with_child(
                    "moderators",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_communities_moderators_list)
                        .with_child_parse::<UserLocalID, _>(
                            crate::RouteNode::new()
                                .with_handler_async(
                                    "PUT",
                                    route_unstable_communities_moderators_add,
                                )
                                .with_handler_async(
                                    "DELETE",
                                    route_unstable_communities_moderators_remove,
                                ),
                        ),
                )
                .with_child(
                    "unfollow",
                    crate::RouteNode::new()
                        .with_handler_async("POST", route_unstable_communities_unfollow),
                )
                .with_child(
                    "posts",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_communities_posts_list)
                        .with_child_parse::<PostLocalID, _>(
                            crate::RouteNode::new().with_handler_async(
                                "PATCH",
                                route_unstable_communities_posts_patch,
                            ),
                        ),
                ),
        )
}
