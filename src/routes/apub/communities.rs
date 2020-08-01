use crate::{CommentLocalID, CommunityLocalID, PostLocalID, UserLocalID};
use activitystreams::prelude::*;
use std::borrow::Cow;
use std::ops::Deref;
use std::sync::Arc;

pub fn route_communities() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child_parse::<CommunityLocalID, _>(
        crate::RouteNode::new()
            .with_handler_async("GET", handler_communities_get)
            .with_child(
                "comments",
                crate::RouteNode::new().with_child_parse::<CommentLocalID, _>(
                    crate::RouteNode::new().with_child(
                        "announce",
                        crate::RouteNode::new()
                            .with_handler_async("GET", handler_communities_comments_announce_get),
                    ),
                ),
            )
            .with_child(
                "followers",
                crate::RouteNode::new()
                    .with_handler_async("GET", handler_communities_followers_list)
                    .with_child_parse::<UserLocalID, _>(
                        crate::RouteNode::new()
                            .with_handler_async("GET", handler_communities_followers_get)
                            .with_child(
                                "accept",
                                crate::RouteNode::new().with_handler_async(
                                    "GET",
                                    handler_communities_followers_accept_get,
                                ),
                            ),
                    ),
            )
            .with_child(
                "inbox",
                crate::RouteNode::new().with_handler_async("POST", handler_communities_inbox_post),
            )
            .with_child(
                "outbox",
                crate::RouteNode::new()
                    .with_handler_async("GET", handler_communities_outbox_get)
                    .with_child(
                        "page",
                        crate::RouteNode::new().with_child_parse::<crate::TimestampOrLatest, _>(
                            crate::RouteNode::new()
                                .with_handler_async("GET", handler_communities_outbox_page_get),
                        ),
                    ),
            )
            .with_child(
                "posts",
                crate::RouteNode::new().with_child_parse::<PostLocalID, _>(
                    crate::RouteNode::new().with_child(
                        "announce",
                        crate::RouteNode::new()
                            .with_handler_async("GET", handler_communities_posts_announce_get)
                            .with_child(
                                "undos",
                                crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                                    crate::RouteNode::new().with_handler_async(
                                        "GET",
                                        handler_communities_posts_announce_undos_get,
                                    ),
                                ),
                            ),
                    ),
                ),
            )
            .with_child(
                "updates",
                crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                    crate::RouteNode::new()
                        .with_handler_async("GET", handler_communities_updates_get),
                ),
            ),
    )
}

async fn handler_communities_get(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT name, local, public_key, description FROM community WHERE id=$1",
            &[&community_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such community",
        )),
        Some(row) => {
            let local: bool = row.get(1);
            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested community is not owned by this instance",
                )));
            }

            let name: String = row.get(0);
            let public_key =
                row.get::<_, Option<&[u8]>>(2)
                    .and_then(|bytes| match std::str::from_utf8(bytes) {
                        Ok(key) => Some(key),
                        Err(err) => {
                            eprintln!("Warning: public_key is not UTF-8: {:?}", err);
                            None
                        }
                    });
            let description: &str = row.get(3);

            let community_ap_id =
                crate::apub_util::get_local_community_apub_id(community_id, &ctx.host_url_apub);

            let mut info = activitystreams::actor::Group::new();
            info.set_many_contexts(vec![
                activitystreams::context(),
                activitystreams::security(),
            ])
            .set_id(community_ap_id.deref().clone())
            .set_name(name.as_ref())
            .set_summary(description);

            let inbox = {
                let mut res = community_ap_id.clone();
                res.path_segments_mut().push("inbox");
                res
            };

            let mut info = activitystreams::actor::ApActor::new(inbox.into(), info);

            info.set_outbox(
                crate::apub_util::get_local_community_outbox_apub_id(
                    community_id,
                    &ctx.host_url_apub,
                )
                .into(),
            )
            .set_followers({
                let mut res = community_ap_id.clone();
                res.path_segments_mut().push("followers");
                res.into()
            })
            .set_preferred_username(name);

            let key_id = format!(
                "{}/communities/{}#main-key",
                ctx.host_url_apub, community_id
            );

            let body = if let Some(public_key) = public_key {
                let public_key_ext = crate::apub_util::PublicKeyExtension {
                    public_key: Some(crate::apub_util::PublicKey {
                        id: (&key_id).into(),
                        owner: community_ap_id.as_str().into(),
                        public_key_pem: public_key.into(),
                        signature_algorithm: Some(crate::apub_util::SIGALG_RSA_SHA256.into()),
                    }),
                };

                let info = activitystreams_ext::Ext1::new(info, public_key_ext);

                serde_json::to_vec(&info)
            } else {
                serde_json::to_vec(&info)
            }?;

            let mut resp = hyper::Response::new(body.into());
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("application/activity+json"),
            );

            Ok(resp)
        }
    }
}

async fn handler_communities_comments_announce_get(
    params: (CommunityLocalID, CommentLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, comment_id) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT reply.id, reply.local, reply.ap_id, community.local FROM reply, post, community WHERE reply.post = post.id AND post.community = community.id AND reply.id = $1 AND community.id = $2",
            &[&comment_id, &community_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such publish",
        )),
        Some(row) => {
            let community_local: bool = row.get(3);
            if !community_local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested community is not owned by this instance",
                )));
            }

            let comment_local_id = CommentLocalID(row.get(0));
            let comment_ap_id = if row.get(1) {
                crate::apub_util::get_local_comment_apub_id(comment_local_id, &ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(2))?
            };

            let body = crate::apub_util::local_community_comment_announce_ap(community_id, comment_local_id, comment_ap_id.into(), &ctx.host_url_apub)?;
            let body = serde_json::to_vec(&body)?;

            Ok(hyper::Response::builder()
               .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
               .body(body.into())?)
        }
    }
}

async fn handler_communities_followers_list(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_one(
            "SELECT COUNT(*) FROM community_follow WHERE community=$1",
            &[&community_id],
        )
        .await?;
    let count: i64 = row.get(0);

    let body = serde_json::to_vec(&serde_json::json!({
        "type": "Collection",
        "totalItems": count,
    }))?
    .into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
        .body(body)?)
}

async fn handler_communities_followers_get(
    params: (CommunityLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, user_id) = params;

    let db = ctx.db_pool.get().await?;

    let row = db.query_opt(
        "SELECT person.local, community.local, community.ap_id FROM community_follow, community, person WHERE community.id=$1 AND community.id = community_follow.community AND person.id = community_follow.follower AND person.id = $2",
        &[&community_id.raw(), &user_id.raw()],
    ).await?;
    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such follow",
        )),
        Some(row) => {
            let follower_local: bool = row.get(0);
            if !follower_local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested follow is not owned by this instance",
                )));
            }

            let community_local: bool = row.get(1);

            let community_ap_id = if community_local {
                crate::apub_util::get_local_community_apub_id(community_id, &ctx.host_url_apub)
            } else {
                let community_ap_id: Option<&str> = row.get(2);
                std::str::FromStr::from_str(community_ap_id.ok_or_else(|| {
                    crate::Error::InternalStr(format!(
                        "Missing ap_id for community {}",
                        community_id
                    ))
                })?)?
            };

            let person_ap_id =
                crate::apub_util::get_local_person_apub_id(user_id, &ctx.host_url_apub);

            let mut follow =
                activitystreams::activity::Follow::new(person_ap_id, community_ap_id.clone());

            follow
                .set_context(activitystreams::context())
                .set_id({
                    let mut res = crate::apub_util::get_local_community_apub_id(
                        community_id,
                        &ctx.host_url_apub,
                    );
                    res.path_segments_mut()
                        .extend(&["followers", &user_id.to_string()]);
                    res.into()
                })
                .set_to(community_ap_id);

            let body = serde_json::to_vec(&follow)?.into();

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                .body(body)?)
        }
    }
}

async fn handler_communities_followers_accept_get(
    params: (CommunityLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, user_id) = params;
    let db = ctx.db_pool.get().await?;

    let row = db.query_opt(
        "SELECT community.local, community_follow.ap_id, person.id, person.local FROM community_follow, community, person WHERE community_follow.community = community.id AND community_follow.follower = person.id AND community.id = $1 AND person.id = $2",
        &[&community_id, &user_id],
    ).await?;

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such follow",
        )),
        Some(row) => {
            let community_local: bool = row.get(0);
            if !community_local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested community is not owned by this instance",
                )));
            }

            let follower_local = row.get(3);
            let follow_ap_id = if follower_local {
                crate::apub_util::get_local_follow_apub_id(
                    community_id,
                    UserLocalID(row.get(2)),
                    &ctx.host_url_apub,
                )
            } else {
                let follow_ap_id: Option<&str> = row.get(1);
                follow_ap_id
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!(
                            "Missing ap_id for follow ({} / {})",
                            community_id, user_id
                        ))
                    })?
                    .parse()?
            };

            let body = crate::apub_util::community_follow_accept_to_ap(
                crate::apub_util::get_local_community_apub_id(community_id, &ctx.host_url_apub),
                user_id,
                follow_ap_id.into(),
            )?;
            let body = serde_json::to_vec(&body)?.into();

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                .body(body)?)
        }
    }
}

async fn handler_communities_inbox_post(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use crate::apub_util::{KnownObject, Verified};

    let (community_id,) = params;
    let db = ctx.db_pool.get().await?;

    let object = crate::apub_util::verify_incoming_object(
        req,
        &db,
        &ctx.http_client,
        ctx.apub_proxy_rewrites,
    )
    .await?;

    match object.into_inner() {
        KnownObject::Create(create) => {
            super::inbox_common_create(Verified(create), ctx).await?;
        }
        KnownObject::Follow(follow) => {
            let follow = Verified(follow);
            let follower_ap_id = follow.actor_unchecked().as_single_id();
            let target_community = follow.object().as_single_id();

            if let Some(follower_ap_id) = follower_ap_id {
                let activity_ap_id = follow
                    .id_unchecked()
                    .ok_or(crate::Error::InternalStrStatic("Missing activitity ID"))?;

                crate::apub_util::require_containment(activity_ap_id, follower_ap_id)?;
                let follow = crate::apub_util::Contained(Cow::Borrowed(&follow));

                let follower_local_id = crate::apub_util::get_or_fetch_user_local_id(
                    follower_ap_id,
                    &db,
                    &ctx.host_url_apub,
                    &ctx.http_client,
                )
                .await?;

                if let Some(target_community) = target_community {
                    if target_community
                        == crate::apub_util::get_local_community_apub_id(
                            community_id,
                            &ctx.host_url_apub,
                        )
                        .deref()
                    {
                        let row = db
                            .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
                            .await?;
                        if let Some(row) = row {
                            let local: bool = row.get(0);
                            if local {
                                db.execute("INSERT INTO community_follow (community, follower, local, ap_id, accepted) VALUES ($1, $2, FALSE, $3, TRUE) ON CONFLICT (community, follower) DO NOTHING", &[&community_id, &follower_local_id, &activity_ap_id.as_str()]).await?;

                                crate::apub_util::spawn_enqueue_send_community_follow_accept(
                                    community_id,
                                    follower_local_id,
                                    follow.with_owned(),
                                    ctx,
                                );
                            }
                        } else {
                            eprintln!("Warning: recieved follow for unknown community");
                        }
                    } else {
                        eprintln!("Warning: recieved follow for wrong community");
                    }
                }
            }
        }
        KnownObject::Delete(activity) => {
            crate::apub_util::handle_delete(Verified(activity), ctx).await?;
        }
        KnownObject::Like(activity) => {
            crate::apub_util::handle_like(Verified(activity), ctx).await?;
        }
        KnownObject::Undo(activity) => {
            crate::apub_util::handle_undo(Verified(activity), ctx).await?;
        }
        _ => {}
    }

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn handler_communities_outbox_get(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;
    let page_ap_id = crate::apub_util::get_local_community_outbox_page_apub_id(
        community_id,
        &crate::TimestampOrLatest::Latest,
        &ctx.host_url_apub,
    );

    let collection = serde_json::json!({
        "@context": activitystreams::context(),
        "type": activitystreams::collection::kind::OrderedCollectionType::OrderedCollection,
        "id": crate::apub_util::get_local_community_outbox_apub_id(community_id, &ctx.host_url_apub),
        "first": &page_ap_id,
        "current": &page_ap_id
    });

    let body = serde_json::to_vec(&collection)?.into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
        .body(body)?)
}

async fn handler_communities_outbox_page_get(
    params: (CommunityLocalID, crate::TimestampOrLatest),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use crate::TimestampOrLatest;

    let (community_id, page) = params;

    let db = ctx.db_pool.get().await?;

    let limit: i64 = 30;

    let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&community_id, &limit];

    let extra_condition = match &page {
        TimestampOrLatest::Latest => "",
        TimestampOrLatest::Timestamp(ts) => {
            values.push(ts);
            " AND post.created < $3"
        }
    };

    let sql: &str = &format!("SELECT post.id, post.local, post.ap_id, post.created FROM post WHERE community=$1{} ORDER BY created DESC LIMIT $2", extra_condition);

    let rows = db.query(sql, &values[..]).await?;

    let last_created = rows.last().map(|row| {
        let created: chrono::DateTime<chrono::offset::FixedOffset> = row.get(3);
        created
    });

    let items: Result<Vec<activitystreams::activity::Announce>, _> = rows
        .into_iter()
        .map(|row| {
            let post_id = PostLocalID(row.get(0));
            let post_ap_id = if row.get(1) {
                crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(2))?
            };

            crate::apub_util::local_community_post_announce_ap(
                community_id,
                post_id,
                post_ap_id.into(),
                &ctx.host_url_apub,
            )
        })
        .collect();

    let items = items?;

    let next = match last_created {
        Some(ts) => Some(crate::apub_util::get_local_community_outbox_page_apub_id(
            community_id,
            &crate::TimestampOrLatest::Timestamp(ts),
            &ctx.host_url_apub,
        )),
        None => None,
    };

    let info = serde_json::json!({
        "@context": activitystreams::context(),
        "type": activitystreams::collection::kind::OrderedCollectionPageType::OrderedCollectionPage,
        "partOf": crate::apub_util::get_local_community_outbox_apub_id(community_id, &ctx.host_url_apub),
        "orderedItems": items,
        "next": next,
    });

    let body = serde_json::to_vec(&info)?.into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
        .body(body)?)
}

async fn handler_communities_posts_announce_get(
    params: (CommunityLocalID, PostLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, post_id) = params;
    let db = ctx.db_pool.get().await?;

    match db.query_opt(
        "SELECT post.id, post.local, post.ap_id, community.local FROM post, community WHERE post.community = community.id AND id=$1 AND community=$2 AND approved",
        &[&post_id, &community_id],
    ).await? {
        None => {
            Ok(crate::simple_response(
                    hyper::StatusCode::NOT_FOUND,
                    "No such publish",
            ))
        },
        Some(row) => {
            let community_local: Option<bool> = row.get(3);
            match community_local {
                None => Ok(crate::simple_response(
                        hyper::StatusCode::NOT_FOUND,
                        "No such community",
                        )),
                Some(false) => Ok(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        "Requested community is not owned by this instance",
                    )),
                Some(true) => {
                    let post_local_id = PostLocalID(row.get(0));
                    let post_ap_id = if row.get(1) {
                        crate::apub_util::get_local_post_apub_id(post_local_id, &ctx.host_url_apub)
                    } else {
                        std::str::FromStr::from_str(row.get(2))?
                    };

                    let body = crate::apub_util::local_community_post_announce_ap(
                        community_id,
                        post_local_id,
                        post_ap_id.into(),
                        &ctx.host_url_apub,
                    )?;
                    let body = serde_json::to_vec(&body)?;

                    Ok(hyper::Response::builder()
                       .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                       .body(body.into())?)
                }
            }
        },
    }
}

async fn handler_communities_posts_announce_undos_get(
    params: (CommunityLocalID, PostLocalID, uuid::Uuid),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, post_id, undo_id) = params;
    let db = ctx.db_pool.get().await?;

    match db.query_opt(
        "SELECT post.local, post.ap_id, community.local FROM post, community WHERE post.community = community.id AND post.id = $1 AND community.id = $2 AND NOT post.approved",
        &[&post_id, &community_id],
    ).await? {
        None => {
            Ok(crate::simple_response(
                    hyper::StatusCode::NOT_FOUND,
                    "No such undo",
            ))
        },
        Some(row) => {
            let community_local = row.get(2);
            if community_local {
                let post_ap_id = if row.get(0) {
                    crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub).into()
                } else {
                    std::str::FromStr::from_str(row.get(1))?
                };
                let body = crate::apub_util::local_community_post_announce_undo_ap(community_id, post_id, post_ap_id, &undo_id, &ctx.host_url_apub)?;
                let body = serde_json::to_vec(&body)?;

                Ok(hyper::Response::builder()
                   .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                   .body(body.into())?)
            } else {
                Ok(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested community is not owned by this instance",
                ))
            }
        }
    }
}

async fn handler_communities_updates_get(
    params: (CommunityLocalID, uuid::Uuid),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, update_id) = params;
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
        .await?;
    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such community",
        )),
        Some(row) => {
            let local: bool = row.get(0);
            if local {
                let body = crate::apub_util::local_community_update_to_ap(
                    community_id,
                    update_id,
                    &ctx.host_url_apub,
                )?;
                let body = serde_json::to_vec(&body)?;

                Ok(hyper::Response::builder()
                    .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                    .body(body.into())?)
            } else {
                Ok(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested community is not owned by this instance",
                ))
            }
        }
    }
}
