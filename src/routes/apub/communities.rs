use activitystreams::ext::Extensible;
use std::borrow::Cow;
use std::sync::Arc;

pub fn route_communities() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child_parse::<i64, _>(
        crate::RouteNode::new()
            .with_handler_async("GET", handler_communities_get)
            .with_child(
                "comments",
                crate::RouteNode::new().with_child_parse::<i64, _>(
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
                    .with_child_parse::<i64, _>(
                        crate::RouteNode::new()
                            .with_handler_async("GET", handler_communities_followers_get),
                    ),
            )
            .with_child(
                "inbox",
                crate::RouteNode::new().with_handler_async("POST", handler_communities_inbox_post),
            )
            .with_child(
                "posts",
                crate::RouteNode::new().with_child_parse::<i64, _>(
                    crate::RouteNode::new().with_child(
                        "announce",
                        crate::RouteNode::new()
                            .with_handler_async("GET", handler_communities_posts_announce_get),
                    ),
                ),
            ),
    )
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
            "SELECT name, local, public_key FROM community WHERE id=$1",
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
            let public_key =
                row.get::<_, Option<&[u8]>>(2)
                    .and_then(|bytes| match std::str::from_utf8(bytes) {
                        Ok(key) => Some(key),
                        Err(err) => {
                            eprintln!("Warning: public_key is not UTF-8: {:?}", err);
                            None
                        }
                    });

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested community is not owned by this instance",
                )));
            }

            let community_ap_id =
                crate::apub_util::get_local_community_apub_id(community_id, &ctx.host_url_apub);

            let mut info = activitystreams::actor::Group::new();
            info.as_mut()
                .set_id(community_ap_id.as_ref())?
                .set_name_xsd_string(name.as_ref())?;

            let mut actor_props = activitystreams::actor::properties::ApActorProperties::default();

            actor_props.set_inbox(format!(
                "{}/communities/{}/inbox",
                ctx.host_url_apub, community_id
            ))?;
            actor_props.set_followers(format!(
                "{}/communities/{}/followers",
                ctx.host_url_apub, community_id
            ))?;
            actor_props.set_preferred_username(name)?;

            let info = info.extend(actor_props);

            let key_id = format!(
                "{}/communities/{}#main-key",
                ctx.host_url_apub, community_id
            );

            let body = if let Some(public_key) = public_key {
                let public_key_ext = crate::apub_util::PublicKeyExtension {
                    public_key: Some(crate::apub_util::PublicKey {
                        id: (&key_id).into(),
                        owner: (&community_ap_id).into(),
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

async fn handler_communities_comments_announce_get(
    params: (i64, i64),
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

            let comment_local_id = row.get(0);
            let comment_ap_id = if row.get(1) {
                Cow::Owned(crate::apub_util::get_local_comment_apub_id(comment_local_id, &ctx.host_url_apub))
            } else {
                Cow::Borrowed(row.get(2))
            };

            let body = crate::apub_util::local_community_comment_announce_ap(community_id, comment_local_id, &comment_ap_id, &ctx.host_url_apub)?;
            let body = serde_json::to_vec(&body)?;

            Ok(hyper::Response::builder()
               .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
               .body(body.into())?)
        }
    }
}

async fn handler_communities_followers_list(
    params: (i64,),
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
    params: (i64, i64),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, user_id) = params;
    let db = ctx.db_pool.get().await?;

    let row = db.query_opt(
        "SELECT person.local, community.local, community.ap_id FROM community_follow, community, person WHERE community.id=$1 AND community.id = community_follow.community AND person.id = community_follow.follower AND person.id = $2",
        &[&community_id, &user_id],
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
                let community_ap_id: Option<String> = row.get(2);
                community_ap_id.ok_or_else(|| {
                    crate::Error::InternalStr(format!(
                        "Missing ap_id for community {}",
                        community_id
                    ))
                })?
            };

            let mut follow = activitystreams::activity::Follow::new();

            follow.object_props.set_id(format!(
                "{}/communities/{}/followers/{}",
                ctx.host_url_apub, community_id, user_id
            ))?;

            let person_ap_id =
                crate::apub_util::get_local_person_apub_id(user_id, &ctx.host_url_apub);

            follow.follow_props.set_actor_xsd_any_uri(person_ap_id)?;

            follow
                .follow_props
                .set_object_xsd_any_uri(community_ap_id.as_ref())?;
            follow.object_props.set_to_xsd_any_uri(community_ap_id)?;

            let body = serde_json::to_vec(&follow)?.into();

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
                .body(body)?)
        }
    }
}

async fn handler_communities_inbox_post(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;
    let db = ctx.db_pool.get().await?;

    let activity = crate::apub_util::verify_incoming_activity(req, &db, &ctx.http_client).await?;

    match activity.kind() {
        Some("Create") => {
            let activity = activity
                .into_concrete::<activitystreams::activity::Create>()
                .unwrap();
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
                    let res = crate::res_to_error(
                        ctx.http_client
                            .request(
                                hyper::Request::get(object_id.as_str())
                                    .header(hyper::header::ACCEPT, crate::apub_util::ACTIVITY_TYPE)
                                    .body(Default::default())?,
                            )
                            .await?,
                    )
                    .await?;

                    let body = hyper::body::to_bytes(res.into_body()).await?;

                    let obj: activitystreams::object::ObjectBox = serde_json::from_slice(&body)?;

                    let community_is_local = {
                        let row = db
                            .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
                            .await?;
                        match row {
                            None => false,
                            Some(row) => row.get(0),
                        }
                    };

                    crate::apub_util::handle_recieved_object(
                        community_id,
                        community_is_local,
                        object_id.as_str(),
                        obj,
                        ctx,
                    )
                    .await?;
                }
            }
        }
        Some("Follow") => {
            let follow = activity
                .into_concrete::<activitystreams::activity::Follow>()
                .unwrap();

            let follower_ap_id = follow.follow_props.get_actor_xsd_any_uri();
            let target_community = follow.follow_props.get_object_xsd_any_uri();

            if let Some(follower_ap_id) = follower_ap_id {
                let follower_local_id = crate::apub_util::get_or_fetch_user_local_id(
                    follower_ap_id.as_str(),
                    &db,
                    &ctx.host_url_apub,
                    &ctx.http_client,
                )
                .await?;

                if let Some(target_community) = target_community {
                    if target_community.as_str()
                        == crate::apub_util::get_local_community_apub_id(
                            community_id,
                            &ctx.host_url_apub,
                        )
                    {
                        let row = db
                            .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
                            .await?;
                        if let Some(row) = row {
                            let local: bool = row.get(0);
                            if local {
                                db.execute("INSERT INTO community_follow (community, follower) VALUES ($1, $2) ON CONFLICT (community, follower) DO NOTHING", &[&community_id, &follower_local_id]).await?;

                                crate::spawn_task(crate::apub_util::send_community_follow_accept(
                                    community_id,
                                    follower_local_id,
                                    follow,
                                    ctx,
                                ));
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
        Some("Delete") => {
            let activity = activity
                .into_concrete::<activitystreams::activity::Delete>()
                .unwrap();

            crate::apub_util::handle_delete(activity, ctx).await?;
        }
        _ => {}
    }

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn handler_communities_posts_announce_get(
    params: (i64, i64),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, post_id) = params;
    let db = ctx.db_pool.get().await?;

    match db.query_opt(
        "SELECT post.id, post.local, post.ap_id, community.local FROM post, community WHERE post.community = community.id AND id=$1 AND community=$2",
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
                    let post_local_id = row.get(0);
                    let post_ap_id = if row.get(1) {
                        Cow::Owned(crate::apub_util::get_local_post_apub_id(post_local_id, &ctx.host_url_apub))
                    } else {
                        Cow::Borrowed(row.get(2))
                    };

                    let body = crate::apub_util::local_community_post_announce_ap(
                        community_id,
                        post_local_id,
                        &post_ap_id,
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
