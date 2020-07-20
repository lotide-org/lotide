use crate::{CommentLocalID, CommunityLocalID, PostLocalID, UserLocalID};
use activitystreams::ext::Extensible;
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::sync::Arc;

mod communities;

pub fn route_apub() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child(
            "users",
            crate::RouteNode::new().with_child_parse::<UserLocalID, _>(
                crate::RouteNode::new()
                    .with_handler_async("GET", handler_users_get)
                    .with_child(
                        "inbox",
                        crate::RouteNode::new()
                            .with_handler_async("POST", handler_users_inbox_post),
                    ),
            ),
        )
        .with_child(
            "comments",
            crate::RouteNode::new().with_child_parse::<CommentLocalID, _>(
                crate::RouteNode::new()
                    .with_handler_async("GET", handler_comments_get)
                    .with_child(
                        "create",
                        crate::RouteNode::new()
                            .with_handler_async("GET", handler_comments_create_get),
                    )
                    .with_child(
                        "delete",
                        crate::RouteNode::new()
                            .with_handler_async("GET", handler_comments_delete_get),
                    )
                    .with_child(
                        "likes",
                        crate::RouteNode::new().with_child_parse::<UserLocalID, _>(
                            crate::RouteNode::new()
                                .with_handler_async("GET", handler_comments_likes_get),
                        ),
                    ),
            ),
        )
        .with_child(
            "comment_like_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new().with_handler_async("GET", handler_comment_like_undos_get),
            ),
        )
        .with_child("communities", communities::route_communities())
        .with_child(
            "community_follow_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new()
                    .with_handler_async("GET", handler_community_follow_undos_get),
            ),
        )
        .with_child("inbox", route_inbox())
        .with_child(
            "posts",
            crate::RouteNode::new().with_child_parse::<PostLocalID, _>(
                crate::RouteNode::new()
                    .with_handler_async("GET", handler_posts_get)
                    .with_child(
                        "create",
                        crate::RouteNode::new().with_handler_async("GET", handler_posts_create_get),
                    )
                    .with_child(
                        "delete",
                        crate::RouteNode::new().with_handler_async("GET", handler_posts_delete_get),
                    )
                    .with_child(
                        "likes",
                        crate::RouteNode::new().with_child_parse::<UserLocalID, _>(
                            crate::RouteNode::new()
                                .with_handler_async("GET", handler_posts_likes_get),
                        ),
                    ),
            ),
        )
        .with_child(
            "post_like_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new().with_handler_async("GET", handler_post_like_undos_get),
            ),
        )
}

fn get_object_id(
    aao_props: &activitystreams::activity::properties::ActorAndObjectProperties,
) -> Result<Option<Cow<'_, activitystreams::primitives::XsdAnyUri>>, crate::Error> {
    match aao_props.get_object_xsd_any_uri() {
        Some(uri) => Ok(Some(Cow::Borrowed(uri))),
        None => match aao_props.get_object_base_box() {
            Some(base_box) => {
                #[derive(Deserialize)]
                struct WithMaybeID {
                    id: Option<activitystreams::primitives::XsdAnyUri>,
                }

                impl activitystreams::Base for WithMaybeID {}

                let value: WithMaybeID = base_box.clone().into_concrete()?;
                match value.id {
                    Some(id) => Ok(Some(Cow::Owned(id))),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        },
    }
}

pub fn route_inbox() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_handler_async("POST", handler_inbox_post)
}

async fn handler_users_get(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT username, local, public_key, description FROM person WHERE id=$1",
            &[&user_id.raw()],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such user",
        )),
        Some(row) => {
            let local: bool = row.get(1);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested user is not owned by this instance",
                )));
            }

            let username: String = row.get(0);
            let public_key: Option<&str> =
                row.get::<_, Option<&[u8]>>(2)
                    .and_then(|bytes| match std::str::from_utf8(bytes) {
                        Ok(key) => Some(key),
                        Err(err) => {
                            eprintln!("Warning: public_key is not UTF-8: {:?}", err);
                            None
                        }
                    });

            let description: &str = row.get(3);

            let user_ap_id =
                crate::apub_util::get_local_person_apub_id(user_id, &ctx.host_url_apub);

            let mut info = activitystreams::actor::Person::new();
            info.object_props.set_many_context_xsd_any_uris(vec![
                activitystreams::context(),
                activitystreams::security(),
            ])?;
            info.as_mut()
                .set_id(user_ap_id.as_ref())?
                .set_name_xsd_string(username.as_ref())?
                .set_summary_xsd_string(description)?;

            let mut endpoints = activitystreams::endpoint::EndpointProperties::default();
            endpoints.set_shared_inbox(format!("{}/inbox", ctx.host_url_apub))?;

            let mut actor_props = activitystreams::actor::properties::ApActorProperties::default();
            actor_props.set_inbox(format!("{}/users/{}/inbox", ctx.host_url_apub, user_id))?;
            actor_props.set_endpoints(endpoints)?;
            actor_props.set_preferred_username(username)?;

            let info = info.extend(actor_props);

            let key_id = format!("{}/users/{}#main-key", ctx.host_url_apub, user_id);

            let body = if let Some(public_key) = public_key {
                let public_key_ext = crate::apub_util::PublicKeyExtension {
                    public_key: Some(crate::apub_util::PublicKey {
                        id: (&key_id).into(),
                        owner: (&user_ap_id).into(),
                        public_key_pem: public_key.into(),
                    }),
                };

                let info = info.extend(public_key_ext);

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

async fn inbox_common(
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let activity = crate::apub_util::verify_incoming_activity(
        req,
        &db,
        &ctx.http_client,
        ctx.apub_proxy_rewrites,
    )
    .await?;

    match activity.kind() {
        Some("Accept") => {
            let activity = activity
                .into_concrete::<activitystreams::activity::Accept>()
                .unwrap();

            let activity_id = activity
                .object_props
                .get_id()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let actor_ap_id = activity
                .accept_props
                .get_actor_xsd_any_uri()
                .ok_or(crate::Error::InternalStrStatic("Missing actor for Accept"))?;

            crate::apub_util::require_containment(activity_id.as_url(), actor_ap_id.as_url())?;

            let actor_ap_id = actor_ap_id.as_str();

            let community_local_id: Option<CommunityLocalID> = {
                db.query_opt("SELECT id FROM community WHERE ap_id=$1", &[&actor_ap_id])
                    .await?
                    .map(|row| CommunityLocalID(row.get(0)))
            };

            if let Some(community_local_id) = community_local_id {
                let object_id = get_object_id(&activity.accept_props)?
                    .ok_or(crate::Error::InternalStrStatic("Missing object for Accept"))?;

                let object_id = object_id.as_str();

                if object_id.starts_with(&ctx.host_url_apub) {
                    let remaining = &object_id[ctx.host_url_apub.len()..];
                    if remaining.starts_with("/communities/") {
                        let remaining = &remaining[13..];
                        let next_expected = format!("{}/followers/", community_local_id);
                        if remaining.starts_with(&next_expected) {
                            let remaining = &remaining[next_expected.len()..];
                            let follower_local_id: UserLocalID = remaining.parse()?;

                            db.execute(
                                "UPDATE community_follow SET accepted=TRUE WHERE community=$1 AND follower=$2",
                                &[&community_local_id, &follower_local_id],
                            ).await?;
                        }
                    }
                }
            }
        }
        Some("Announce") => {
            let activity = activity
                .into_concrete::<activitystreams::activity::Announce>()
                .unwrap();
            let activity_id = activity
                .object_props
                .get_id()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let community_ap_id = activity.announce_props.get_actor_xsd_any_uri().ok_or(
                crate::Error::InternalStrStatic("Missing actor for Announce"),
            )?;

            let community_local_info = db
                .query_opt(
                    "SELECT id, local FROM community WHERE ap_id=$1",
                    &[&community_ap_id.as_str()],
                )
                .await?
                .map(|row| (CommunityLocalID(row.get(0)), row.get(1)));

            if let Some((community_local_id, community_is_local)) = community_local_info {
                crate::apub_util::require_containment(
                    activity_id.as_url(),
                    community_ap_id.as_url(),
                )?;

                let object_id = {
                    if let activitystreams::activity::properties::ActorAndObjectOptTargetPropertiesObjectEnum::Term(
                        req_obj,
                    ) = activity.announce_props.object
                    {
                        match req_obj {
                            activitystreams::activity::properties::ActorAndObjectOptTargetPropertiesObjectTermEnum::XsdAnyUri(id) => Some(id),
                            activitystreams::activity::properties::ActorAndObjectOptTargetPropertiesObjectTermEnum::BaseBox(req_obj) => {
                                match req_obj.kind() {
                                    Some("Page") => {
                                        let req_obj = req_obj.into_concrete::<activitystreams::object::Page>().unwrap();

                                        Some(req_obj.object_props.id.ok_or_else(|| crate::Error::UserError(crate::simple_response(hyper::StatusCode::BAD_REQUEST, "Missing id in object")))?)
                                    },
                                    Some("Note") => {
                                        let req_obj = req_obj.into_concrete::<activitystreams::object::Note>().unwrap();

                                        Some(req_obj.object_props.id.ok_or_else(|| crate::Error::UserError(crate::simple_response(hyper::StatusCode::BAD_REQUEST, "Missing id in object")))?)
                                    },
                                    _ => None,
                                }
                            }
                        }
                    } else {
                        None
                    }
                };

                if let Some(object_id) = object_id {
                    if !object_id.as_str().starts_with(&ctx.host_url_apub) {
                        // don't need announces for local objects
                        let body =
                            crate::apub_util::fetch_ap_object(object_id.as_str(), &ctx.http_client)
                                .await?;
                        let obj: activitystreams::object::ObjectBox = serde_json::from_value(body)?;
                        crate::apub_util::handle_recieved_object_for_community(
                            community_local_id,
                            community_is_local,
                            obj,
                            ctx,
                        )
                        .await?;
                    }
                }
            }
        }
        Some("Create") => {
            let activity = activity
                .into_concrete::<activitystreams::activity::Create>()
                .unwrap();
            inbox_common_create(activity, ctx).await?;
        }
        Some("Delete") => {
            let activity = activity
                .into_concrete::<activitystreams::activity::Delete>()
                .unwrap();

            crate::apub_util::handle_delete(activity, ctx).await?;
        }
        Some("Like") => {
            let activity = activity
                .into_concrete::<activitystreams::activity::Like>()
                .unwrap();
            crate::apub_util::handle_like(activity, ctx).await?;
        }
        Some("Undo") => {
            let activity = activity
                .into_concrete::<activitystreams::activity::Undo>()
                .unwrap();
            crate::apub_util::handle_undo(activity, ctx).await?;
        }
        Some("Update") => {
            let activity = activity
                .into_concrete::<activitystreams::activity::Update>()
                .unwrap();

            let activity_id = activity
                .object_props
                .get_id()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let object_id = activity
                .update_props
                .get_object_xsd_any_uri()
                .ok_or(crate::Error::InternalStrStatic("Missing object for Update"))?;

            crate::apub_util::require_containment(activity_id.as_url(), object_id.as_url())?;

            let object_id = object_id.as_str().to_owned();

            crate::spawn_task(async move {
                let row = db
                    .query_opt(
                        "SELECT 1 FROM community WHERE ap_id=$1 LIMIT 1",
                        &[&object_id],
                    )
                    .await?;
                if row.is_some() {
                    ctx.enqueue_task(&crate::tasks::FetchActor {
                        actor_ap_id: object_id.into(),
                    })
                    .await?;
                }

                Ok(())
            });
        }
        _ => {}
    }

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

pub async fn inbox_common_create(
    activity: activitystreams::activity::Create,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let req_obj = activity.create_props.object;
    if let activitystreams::activity::properties::ActorAndObjectPropertiesObjectEnum::Term(
        req_obj,
    ) = req_obj
    {
        let object_id = match req_obj {
            activitystreams::activity::properties::ActorAndObjectPropertiesObjectTermEnum::XsdAnyUri(id) => Some(id),
            activitystreams::activity::properties::ActorAndObjectPropertiesObjectTermEnum::BaseBox(req_obj) => {
                match req_obj.kind() {
                    Some("Page") => {
                        let req_obj = req_obj.into_concrete::<activitystreams::object::Page>().unwrap();

                        Some(req_obj.object_props.id.ok_or_else(|| crate::Error::UserError(crate::simple_response(hyper::StatusCode::BAD_REQUEST, "Missing id in object")))?)
                    },
                    Some("Note") => {
                        let req_obj = req_obj.into_concrete::<activitystreams::object::Note>().unwrap();

                        Some(req_obj.object_props.id.ok_or_else(|| crate::Error::UserError(crate::simple_response(hyper::StatusCode::BAD_REQUEST, "Missing id in object")))?)
                    },
                    _ => None,
                }
            }
        };
        if let Some(object_id) = object_id {
            let body =
                crate::apub_util::fetch_ap_object(object_id.as_str(), &ctx.http_client).await?;
            let obj: activitystreams::object::ObjectBox = serde_json::from_value(body)?;

            crate::apub_util::handle_recieved_object_for_local_community(obj, ctx).await?;
        }
    }

    Ok(())
}

async fn handler_users_inbox_post(
    _: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    inbox_common(ctx, req).await
}

async fn handler_comments_get(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT reply.author, reply.content_text, reply.post, reply.created, reply.local, reply.parent, post.local, post.ap_id, post.community, community.local, community.ap_id, reply_parent.local, reply_parent.ap_id, post_author.id, post_author.local, post_author.ap_id, reply_parent_author.id, reply_parent_author.local, reply_parent_author.ap_id, reply.deleted, reply.content_markdown, reply.content_html FROM reply LEFT OUTER JOIN post ON (post.id = reply.post) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) LEFT OUTER JOIN community ON (community.id = post.community) LEFT OUTER JOIN reply AS reply_parent ON (reply_parent.id = reply.parent) LEFT OUTER JOIN person AS reply_parent_author ON (reply_parent_author.id = reply_parent.author) WHERE reply.id=$1",
            &[&comment_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        )),
        Some(row) => {
            let local: bool = row.get(4);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested comment is not owned by this instance",
                )));
            }

            if row.get(19) {
                let mut body = activitystreams::object::Tombstone::new();
                body.tombstone_props.set_former_type_xsd_string("Note")?;
                body.object_props.set_context_xsd_any_uri(activitystreams::context())?;
                body.object_props.set_id(crate::apub_util::get_local_comment_apub_id(comment_id, &ctx.host_url_apub))?;

                let body = serde_json::to_vec(&body)?.into();

                let mut resp = hyper::Response::new(body);
                *resp.status_mut() = hyper::StatusCode::GONE;
                resp.headers_mut().insert(
                    hyper::header::CONTENT_TYPE,
                    hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
                );

                return Ok(resp);
            }

            let post_local_id = PostLocalID(row.get(2));

            let community_local_id = CommunityLocalID(row.get(8));

            let community_ap_id = if row.get(9) {
                crate::apub_util::get_local_community_apub_id(community_local_id, &ctx.host_url_apub)
            } else {
                row.get(10)
            };

            let post_ap_id = if row.get(6) {
                crate::apub_util::get_local_post_apub_id(post_local_id, &ctx.host_url_apub)
            } else {
                row.get(7)
            };

            let parent_local_id = row.get::<_, Option<_>>(5).map(CommentLocalID);

            let content_text = row.get(1);
            let content_markdown = row.get(20);
            let content_html = row.get(21);

            let info = crate::CommentInfo {
                author: Some(UserLocalID(row.get(0))),
                created: row.get(3),
                content_text,
                content_markdown,
                content_html,
                id: comment_id,
                post: post_local_id,
                parent: parent_local_id,
                ap_id: crate::APIDOrLocal::Local,
            };

            let parent_ap_id = match row.get(11) {
                None => None,
                Some(true) => Some(crate::apub_util::get_local_comment_apub_id(parent_local_id.unwrap(), &ctx.host_url_apub)),
                Some(false) => row.get(12),
            };

            let post_or_parent_author_ap_id = match parent_local_id {
                None => {
                    // no parent comment, use post
                    match row.get(14) {
                        Some(post_author_local) => {
                            if post_author_local {
                                Some(crate::apub_util::get_local_person_apub_id(UserLocalID(row.get(13)), &ctx.host_url_apub))
                            } else {
                                row.get(15)
                            }
                        },
                        None => None,
                    }
                },
                Some(_) => {
                    match row.get(17) {
                        Some(parent_author_local) => {
                            if parent_author_local {
                                Some(crate::apub_util::get_local_person_apub_id(UserLocalID(row.get(16)), &ctx.host_url_apub))
                            } else {
                                row.get(18)
                            }
                        },
                        None => None,
                    }
                },
            };

            let body = crate::apub_util::local_comment_to_ap(&info, &post_ap_id, parent_ap_id.as_deref(), post_or_parent_author_ap_id.as_deref(), &community_ap_id, &ctx.host_url_apub)?;

            let body = serde_json::to_vec(&body)?.into();

            let mut resp = hyper::Response::new(body);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        },
    }
}

async fn handler_comments_create_get(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT reply.author, reply.content_text, reply.post, reply.created, reply.local, reply.parent, post.local, post.ap_id, post.community, community.local, community.ap_id, reply_parent.local, reply_parent.ap_id, post_author.id, post_author.local, post_author.ap_id, reply_parent_author.id, reply_parent_author.local, reply_parent_author.ap_id, reply.deleted, reply.content_markdown, reply.content_html FROM reply LEFT OUTER JOIN post ON (post.id = reply.post) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) LEFT OUTER JOIN community ON (community.id = post.community) LEFT OUTER JOIN reply AS reply_parent ON (reply_parent.id = reply.parent) LEFT OUTER JOIN person AS reply_parent_author ON (reply_parent_author.id = reply_parent.author) WHERE reply.id=$1",
            &[&comment_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        )),
        Some(row) => {
            let local: bool = row.get(4);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested comment is not owned by this instance",
                )));
            }

            if row.get(19) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::GONE,
                    "Comment has been deleted",
                )));
            }

            let post_local_id = PostLocalID(row.get(2));

            let community_local_id = CommunityLocalID(row.get(8));

            let community_ap_id = if row.get(9) {
                crate::apub_util::get_local_community_apub_id(community_local_id, &ctx.host_url_apub)
            } else {
                row.get(10)
            };

            let post_ap_id = if row.get(6) {
                crate::apub_util::get_local_post_apub_id(post_local_id, &ctx.host_url_apub)
            } else {
                row.get(7)
            };

            let parent_local_id = row.get::<_, Option<_>>(5).map(CommentLocalID);

            let content_text = row.get(1);
            let content_markdown = row.get(20);
            let content_html = row.get(21);

            let info = crate::CommentInfo {
                author: Some(UserLocalID(row.get(0))),
                created: row.get(3),
                content_text,
                content_markdown,
                content_html,
                id: comment_id,
                post: post_local_id,
                parent: parent_local_id,
                ap_id: crate::APIDOrLocal::Local,
            };

            let parent_ap_id = match row.get(11) {
                None => None,
                Some(true) => Some(crate::apub_util::get_local_comment_apub_id(parent_local_id.unwrap(), &ctx.host_url_apub)),
                Some(false) => row.get(12),
            };

            let post_or_parent_author_ap_id = match parent_local_id {
                None => {
                    // no parent comment, use post
                    match row.get(14) {
                        Some(post_author_local) => {
                            if post_author_local {
                                Some(crate::apub_util::get_local_person_apub_id(UserLocalID(row.get(13)), &ctx.host_url_apub))
                            } else {
                                row.get(15)
                            }
                        },
                        None => None,
                    }
                },
                Some(_) => {
                    match row.get(17) {
                        Some(parent_author_local) => {
                            if parent_author_local {
                                Some(crate::apub_util::get_local_person_apub_id(UserLocalID(row.get(16)), &ctx.host_url_apub))
                            } else {
                                row.get(18)
                            }
                        },
                        None => None,
                    }
                },
            };

            let body = crate::apub_util::local_comment_to_create_ap(&info, &post_ap_id, parent_ap_id.as_deref(), post_or_parent_author_ap_id.as_deref(), &community_ap_id, &ctx.host_url_apub)?;

            let body = serde_json::to_vec(&body)?.into();

            let mut resp = hyper::Response::new(body);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        },
    }
}

async fn handler_comments_delete_get(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT author, local, deleted FROM reply WHERE id=$1",
            &[&comment_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        )),
        Some(row) => {
            let local: bool = row.get(1);
            let deleted: bool = row.get(2);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested comment is not owned by this instance",
                )));
            }
            if !deleted {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::NOT_FOUND,
                    "That comment is not deleted",
                )));
            }

            let author = UserLocalID(row.get(0));

            let body = crate::apub_util::local_comment_delete_to_ap(
                comment_id,
                author,
                &ctx.host_url_apub,
            )?;

            let body = serde_json::to_vec(&body)?.into();

            let mut resp = hyper::Response::new(body);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        }
    }
}

async fn handler_comments_likes_get(
    params: (CommentLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id, user_id) = params;

    let db = ctx.db_pool.get().await?;

    let like_row = db
        .query_opt(
            "SELECT local FROM reply_like WHERE reply=$1 AND person=$2",
            &[&comment_id, &user_id.raw()],
        )
        .await?;
    if let Some(like_row) = like_row {
        let local = like_row.get(0);

        if local {
            let row = db
                .query_one("SELECT local, ap_id FROM reply WHERE id=$1", &[&comment_id])
                .await?;
            let comment_local = row.get(0);
            let comment_ap_id = if comment_local {
                Cow::Owned(crate::apub_util::get_local_comment_apub_id(
                    comment_id,
                    &ctx.host_url_apub,
                ))
            } else {
                Cow::Borrowed(row.get(1))
            };

            let like = crate::apub_util::local_comment_like_to_ap(
                comment_id,
                &comment_ap_id,
                user_id,
                &ctx.host_url_apub,
            )?;
            let body = serde_json::to_vec(&like)?.into();

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                .body(body)?)
        } else {
            Ok(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Requested like is not owned by this instance",
            ))
        }
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such like",
        ))
    }
}

async fn handler_comment_like_undos_get(
    params: (uuid::Uuid,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (undo_id,) = params;

    let db = ctx.db_pool.get().await?;

    let undo_row = db
        .query_opt(
            "SELECT reply, person FROM local_reply_like_undo WHERE id=$1",
            &[&undo_id],
        )
        .await?;

    if let Some(undo_row) = undo_row {
        let comment_id = CommentLocalID(undo_row.get(0));
        let user_id = UserLocalID(undo_row.get(1));

        let undo = crate::apub_util::local_comment_like_undo_to_ap(
            undo_id,
            comment_id,
            user_id,
            &ctx.host_url_apub,
        )?;
        let body = serde_json::to_vec(&undo)?.into();

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
            .body(body)?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such unlike",
        ))
    }
}

async fn handler_community_follow_undos_get(
    params: (uuid::Uuid,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (undo_id,) = params;

    let db = ctx.db_pool.get().await?;

    let undo_row = db
        .query_opt(
            "SELECT community, follower FROM local_community_follow_undo WHERE id=$1",
            &[&undo_id],
        )
        .await?;

    if let Some(undo_row) = undo_row {
        let community_id = CommunityLocalID(undo_row.get(0));
        let user_id = UserLocalID(undo_row.get(1));

        let undo = crate::apub_util::local_community_follow_undo_to_ap(
            undo_id,
            community_id,
            user_id,
            &ctx.host_url_apub,
        )?;
        let body = serde_json::to_vec(&undo)?.into();

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
            .body(body)?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such unlike",
        ))
    }
}

// sharedInbox
async fn handler_inbox_post(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    inbox_common(ctx, req).await
}

async fn handler_posts_get(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT author, href, title, created, community, local, deleted, had_href, content_text, content_markdown, content_html, (SELECT ap_id FROM community WHERE id=post.community) AS community_ap_id FROM post WHERE id=$1",
            &[&post_id.raw()],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such post",
        )),
        Some(row) => {
            let local: bool = row.get(5);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested post is not owned by this instance",
                )));
            }

            if row.get(6) {
                let had_href: Option<bool> = row.get(7);

                let mut body = activitystreams::object::Tombstone::new();
                body.tombstone_props.set_former_type_xsd_string(if had_href == Some(true) { "Page" } else { "Note" })?;
                body.object_props.set_context_xsd_any_uri(activitystreams::context())?;
                body.object_props.set_id(crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub))?;

                let body = serde_json::to_vec(&body)?.into();

                let mut resp = hyper::Response::new(body);
                *resp.status_mut() = hyper::StatusCode::GONE;
                resp.headers_mut().insert(
                    hyper::header::CONTENT_TYPE,
                    hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
                );

                return Ok(resp);
            }

            let community_local_id = CommunityLocalID(row.get(4));

            let community_ap_id = match row.get(11) {
                Some(ap_id) => ap_id,
                None => {
                    // assume local (might be a problem?)
                    crate::apub_util::get_local_community_apub_id(community_local_id, &ctx.host_url_apub)
                }
            };

            let post_info = crate::PostInfo {
                author: Some(UserLocalID(row.get(0))),
                community: community_local_id,
                created: &row.get(3),
                href: row.get(1),
                content_text: row.get(8),
                content_markdown: row.get(9),
                content_html: row.get(10),
                id: post_id,
                title: row.get(2),
            };

            let body = crate::apub_util::post_to_ap(&post_info, &community_ap_id, &ctx.host_url_apub)?;

            let body = serde_json::to_vec(&body)?.into();

            let mut resp = hyper::Response::new(body);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        },
    }
}

async fn handler_posts_create_get(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT author, href, title, created, community, local, deleted, content_text, content_markdown, content_html, (SELECT ap_id FROM community WHERE id=post.community) AS community_ap_id FROM post WHERE id=$1",
            &[&post_id.raw()],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such post",
        )),
        Some(row) => {
            let local: bool = row.get(5);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested post is not owned by this instance",
                )));
            }

            if row.get(6) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::GONE,
                    "Post has been deleted",
                )));
            }

            let community_local_id = CommunityLocalID(row.get(4));

            let community_ap_id = match row.get(10) {
                Some(ap_id) => ap_id,
                None => {
                    // assume local (might be a problem?)
                    crate::apub_util::get_local_community_apub_id(community_local_id, &ctx.host_url_apub)
                }
            };

            let post_info = crate::PostInfo {
                author: Some(UserLocalID(row.get(0))),
                community: community_local_id,
                created: &row.get(3),
                href: row.get(1),
                content_text: row.get(7),
                content_markdown: row.get(8),
                content_html: row.get(9),
                id: post_id,
                title: row.get(3),
            };

            let body = crate::apub_util::local_post_to_create_ap(&post_info, &community_ap_id, &ctx.host_url_apub)?;

            let body = serde_json::to_vec(&body)?.into();

            let mut resp = hyper::Response::new(body);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        },
    }
}

async fn handler_posts_delete_get(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT author, local, deleted FROM post WHERE id=$1",
            &[&post_id.raw()],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such post",
        )),
        Some(row) => {
            let local: bool = row.get(1);
            let deleted: bool = row.get(2);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested post is not owned by this instance",
                )));
            }
            if !deleted {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::NOT_FOUND,
                    "That post is not deleted",
                )));
            }

            let author = UserLocalID(row.get(0));

            let body =
                crate::apub_util::local_post_delete_to_ap(post_id, author, &ctx.host_url_apub)?;

            let body = serde_json::to_vec(&body)?.into();

            let mut resp = hyper::Response::new(body);
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(crate::apub_util::ACTIVITY_TYPE),
            );

            Ok(resp)
        }
    }
}

async fn handler_posts_likes_get(
    params: (PostLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id, user_id) = params;

    let db = ctx.db_pool.get().await?;

    let like_row = db
        .query_opt(
            "SELECT local FROM post_like WHERE post=$1 AND person=$2",
            &[&post_id.raw(), &user_id],
        )
        .await?;
    if let Some(like_row) = like_row {
        let local = like_row.get(0);

        if local {
            let row = db
                .query_one(
                    "SELECT local, ap_id FROM post WHERE id=$1",
                    &[&post_id.raw()],
                )
                .await?;
            let post_local = row.get(0);
            let post_ap_id = if post_local {
                Cow::Owned(crate::apub_util::get_local_post_apub_id(
                    post_id,
                    &ctx.host_url_apub,
                ))
            } else {
                Cow::Borrowed(row.get(1))
            };

            let like = crate::apub_util::local_post_like_to_ap(
                post_id,
                &post_ap_id,
                user_id,
                &ctx.host_url_apub,
            )?;
            let body = serde_json::to_vec(&like)?.into();

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                .body(body)?)
        } else {
            Ok(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Requested like is not owned by this instance",
            ))
        }
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such like",
        ))
    }
}

async fn handler_post_like_undos_get(
    params: (uuid::Uuid,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (undo_id,) = params;

    let db = ctx.db_pool.get().await?;

    let undo_row = db
        .query_opt(
            "SELECT post, person FROM local_post_like_undo WHERE id=$1",
            &[&undo_id],
        )
        .await?;

    if let Some(undo_row) = undo_row {
        let post_id = PostLocalID(undo_row.get(0));
        let user_id = UserLocalID(undo_row.get(1));

        let undo = crate::apub_util::local_post_like_undo_to_ap(
            undo_id,
            post_id,
            user_id,
            &ctx.host_url_apub,
        )?;
        let body = serde_json::to_vec(&undo)?.into();

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
            .body(body)?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such unlike",
        ))
    }
}
