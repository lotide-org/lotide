use activitystreams::ext::Extensible;
use std::sync::Arc;

pub fn route_apub() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child(
            "users",
            crate::RouteNode::new().with_child_parse::<i64, _>(
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
            "communities",
            crate::RouteNode::new().with_child_parse::<i64, _>(
                crate::RouteNode::new()
                    .with_handler_async("GET", handler_communities_get)
                    .with_child(
                        "followers",
                        crate::RouteNode::new().with_child_parse::<i64, _>(
                            crate::RouteNode::new()
                                .with_handler_async("GET", handler_communities_followers_get),
                        ),
                    )
                    .with_child(
                        "inbox",
                        crate::RouteNode::new()
                            .with_handler_async("POST", handler_communities_inbox_post),
                    )
                    .with_child(
                        "posts",
                        crate::RouteNode::new().with_child_parse::<i64, _>(
                            crate::RouteNode::new().with_child(
                                "announce",
                                crate::RouteNode::new().with_handler_async(
                                    "GET",
                                    handler_communities_posts_announce_get,
                                ),
                            ),
                        ),
                    ),
            ),
        )
        .with_child(
            "posts",
            crate::RouteNode::new().with_child_parse::<i64, _>(
                crate::RouteNode::new().with_handler_async("GET", handler_posts_get),
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

async fn handler_users_inbox_post(
    _: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let req_activity: activitystreams::activity::ActivityBox = {
        let body = hyper::body::to_bytes(req.into_body()).await?;

        serde_json::from_slice(&body)?
    };

    match req_activity.kind() {
        Some("Announce") => {
            let req_activity = req_activity
                .into_concrete::<activitystreams::activity::Announce>()
                .unwrap();

            let activity_id = req_activity.object_props.id.ok_or_else(|| {
                crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Missing id in object",
                ))
            })?;

            let activity = {
                let res = crate::res_to_error(
                    ctx.http_client
                        .request(
                            hyper::Request::get(activity_id.as_str())
                                .header(hyper::header::ACCEPT, crate::apub_util::ACTIVITY_TYPE)
                                .body(Default::default())?,
                        )
                        .await?,
                )
                .await?;

                let body = hyper::body::to_bytes(res.into_body()).await?;
                let body: activitystreams::activity::Announce = serde_json::from_slice(&body)?;
                body
            };

            let community_ap_id = activity.announce_props.get_actor_xsd_any_uri();

            // TODO verify that this announce is actually from the community

            let community_local_id: Option<i64> = {
                match community_ap_id {
                    None => None,
                    Some(community_ap_id) => db
                        .query_opt(
                            "SELECT id FROM community WHERE ap_id=$1",
                            &[&community_ap_id.as_str()],
                        )
                        .await?
                        .map(|row| row.get(0)),
                }
            };

            if let Some(community_local_id) = community_local_id {
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
                                    _ => None,
                                }
                            }
                        }
                    } else {
                        None
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

                    match obj.kind() {
                        Some("Page") => {
                            let obj: activitystreams::object::Page = obj.into_concrete().unwrap();
                            let title = obj
                                .as_ref()
                                .get_summary_xsd_string()
                                .map(|x| x.as_str())
                                .unwrap_or("");
                            let href = obj.as_ref().get_url_xsd_any_uri().map(|x| x.as_str());
                            let created = obj.as_ref().get_published().map(|x| x.as_datetime());
                            // TODO support objects here?
                            let author = obj.as_ref().get_attributed_to_xsd_any_uri();
                            // TODO verify that this post is intended to go to this community
                            // TODO verify this post actually came from the specified author

                            let author = match author {
                                Some(author) => Some(
                                    crate::apub_util::get_or_fetch_user_local_id(
                                        author.as_str(),
                                        &db,
                                        &ctx.host_url_apub,
                                        &ctx.http_client,
                                    )
                                    .await?,
                                ),
                                None => None,
                            };

                            db.execute(
                                "INSERT INTO post (author, href, title, created, community, local, ap_id) VALUES ($1, $2, $3, COALESCE($4, current_timestamp), $5, FALSE, $6)",
                                &[&author, &href, &title, &created, &community_local_id, &object_id.as_str()],
                                ).await?;
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => {}
    }

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
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
                    "Requested community is not owned by this instance",
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

    let req_activity: activitystreams::activity::ActivityBox = {
        let body = hyper::body::to_bytes(req.into_body()).await?;

        serde_json::from_slice(&body)?
    };

    match req_activity.kind() {
        Some("Create") => {
            let req_activity = req_activity
                .into_concrete::<activitystreams::activity::Create>()
                .unwrap();
            let req_obj = req_activity.create_props.object;
            if let activitystreams::activity::properties::ActorAndObjectPropertiesObjectEnum::Term(
                req_obj,
            ) = req_obj
            {
                let obj_id = match req_obj {
                    activitystreams::activity::properties::ActorAndObjectPropertiesObjectTermEnum::XsdAnyUri(id) => Some(id),
                    activitystreams::activity::properties::ActorAndObjectPropertiesObjectTermEnum::BaseBox(req_obj) => {
                        match req_obj.kind() {
                            Some("Page") => {
                                let req_obj = req_obj.into_concrete::<activitystreams::object::Page>().unwrap();

                                Some(req_obj.object_props.id.ok_or_else(|| crate::Error::UserError(crate::simple_response(hyper::StatusCode::BAD_REQUEST, "Missing id in object")))?)
                            },
                            _ => None,
                        }
                    }
                };
                if let Some(obj_id) = obj_id {
                    let res = ctx
                        .http_client
                        .request(
                            hyper::Request::get(obj_id.as_str())
                                .header(hyper::header::ACCEPT, crate::apub_util::ACTIVITY_TYPE)
                                .body(Default::default())?,
                        )
                        .await?;

                    let body = hyper::body::to_bytes(res.into_body()).await?;

                    let obj: activitystreams::object::Page = serde_json::from_slice(&body)?;

                    let title = obj
                        .as_ref()
                        .get_summary_xsd_string()
                        .map(|x| x.as_str())
                        .unwrap_or("");
                    let href = obj.as_ref().get_url_xsd_any_uri().map(|x| x.as_str());
                    let created = obj.as_ref().get_published().map(|x| x.as_datetime());
                    // TODO support objects here?
                    let author = obj.as_ref().get_attributed_to_xsd_any_uri();
                    // TODO verify that this post is intended to go to this community
                    // TODO verify this post actually came from the specified author

                    let author = match author {
                        Some(author) => Some(
                            crate::apub_util::get_or_fetch_user_local_id(
                                author.as_str(),
                                &db,
                                &ctx.host_url_apub,
                                &ctx.http_client,
                            )
                            .await?,
                        ),
                        None => None,
                    };

                    db.execute(
                        "INSERT INTO post (author, href, title, created, community, local, ap_id) VALUES ($1, $2, $3, COALESCE($4, current_timestamp), $5, FALSE, $6)",
                        &[&author, &href, &title, &created, &community_id, &obj_id.as_str()],
                    ).await?;
                }
            }
        }
        Some("Follow") => {
            let req_follow = req_activity
                .into_concrete::<activitystreams::activity::Follow>()
                .unwrap();

            let activity_id = req_follow.object_props.id.ok_or_else(|| {
                crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Missing id in object",
                ))
            })?;

            let res = crate::res_to_error(
                ctx.http_client
                    .request(
                        hyper::Request::get(activity_id.as_str())
                            .header(hyper::header::ACCEPT, crate::apub_util::ACTIVITY_TYPE)
                            .body(Default::default())?,
                    )
                    .await?,
            )
            .await?;

            let body = hyper::body::to_bytes(res.into_body()).await?;

            let follow: activitystreams::activity::Follow = serde_json::from_slice(&body)?;

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
                                let row_count = db.execute("INSERT INTO community_follow (community, follower) VALUES ($1, $2)", &[&community_id, &follower_local_id]).await?;

                                if row_count > 0 {
                                    crate::spawn_task(
                                        crate::apub_util::send_community_follow_accept(
                                            community_id,
                                            follower_local_id,
                                            follow,
                                            ctx,
                                        ),
                                    );
                                }
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
        "SELECT author, href, title, created, local, (SELECT local FROM community WHERE id=post.community) FROM post WHERE id=$1 AND community=$2",
        &[&post_id, &community_id],
    ).await? {
        None => {
            Ok(crate::simple_response(
                    hyper::StatusCode::NOT_FOUND,
                    "No such publish",
            ))
        },
        Some(row) => {
            let community_local: Option<bool> = row.get(5);
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
                    let post = crate::PostInfo {
                        author: row.get(0),
                        href: row.get(1),
                        title: row.get(2),
                        created: &row.get(3),

                        id: post_id,
                        community: community_id,
                    };

                    let body = crate::apub_util::local_community_post_to_announce_ap(
                        &post,
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

async fn handler_posts_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT author, href, title, created, community, local, (SELECT ap_id FROM community WHERE id=post.community) AS community_ap_id FROM post WHERE id=$1",
            &[&post_id],
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

            let community_local_id = row.get(4);

            let community_ap_id = match row.get(6) {
                Some(ap_id) => ap_id,
                None => {
                    // assume local (might be a problem?)
                    crate::apub_util::get_local_community_apub_id(community_local_id, &ctx.host_url_apub)
                }
            };

            let post_info = crate::PostInfo {
                author: row.get(0),
                community: community_local_id,
                created: &row.get(3),
                href: row.get(1),
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
