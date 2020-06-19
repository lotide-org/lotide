use activitystreams::ext::Extensible;
use serde_derive::Serialize;
use std::sync::Arc;

mod communities;

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
            "comments",
            crate::RouteNode::new().with_child_parse::<i64, _>(
                crate::RouteNode::new().with_handler_async("GET", handler_comments_get),
            ),
        )
        .with_child("communities", communities::route_communities())
        .with_child("inbox", route_inbox())
        .with_child(
            "posts",
            crate::RouteNode::new().with_child_parse::<i64, _>(
                crate::RouteNode::new()
                    .with_handler_async("GET", handler_posts_get)
                    .with_child(
                        "create",
                        crate::RouteNode::new().with_handler_async("GET", handler_posts_create_get),
                    ),
            ),
        )
}

pub fn route_inbox() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_handler_async("POST", handler_inbox_post)
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicKey<'a> {
    id: &'a str,
    owner: &'a str,
    public_key_pem: &'a str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicKeyExtension<'a> {
    public_key: PublicKey<'a>,
}

impl<'a, T: activitystreams::actor::Actor> activitystreams::ext::Extension<T>
    for PublicKeyExtension<'a>
{
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
            "SELECT username, local, public_key FROM person WHERE id=$1",
            &[&user_id],
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

            let user_ap_id =
                crate::apub_util::get_local_person_apub_id(user_id, &ctx.host_url_apub);

            let mut info = activitystreams::actor::Person::new();
            info.as_mut()
                .set_id(user_ap_id.as_ref())?
                .set_name_xsd_string(username)?;

            let mut endpoints = activitystreams::endpoint::EndpointProperties::default();
            endpoints.set_shared_inbox(format!("{}/inbox", ctx.host_url_apub))?;

            let mut actor_props = activitystreams::actor::properties::ApActorProperties::default();
            actor_props.set_inbox(format!("{}/users/{}/inbox", ctx.host_url_apub, user_id))?;
            actor_props.set_endpoints(endpoints)?;

            let info = info.extend(actor_props);

            let body = if let Some(public_key) = public_key {
                let public_key_ext = PublicKeyExtension {
                    public_key: PublicKey {
                        id: &format!("{}/users/{}#main-key", ctx.host_url_apub, user_id),
                        owner: &user_ap_id,
                        public_key_pem: public_key,
                    },
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
                    crate::apub_util::handle_recieved_object(
                        community_local_id,
                        object_id.as_str(),
                        obj,
                        &db,
                        &ctx.host_url_apub,
                        &ctx.http_client,
                    )
                    .await?;
                }
            }
        }
        _ => {}
    }

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn handler_users_inbox_post(
    _: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    inbox_common(ctx, req).await
}

async fn handler_comments_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT reply.author, reply.content_text, reply.post, reply.created, reply.local, reply.parent, post.local, post.ap_id, post.community, community.local, community.ap_id, reply_parent.local, reply_parent.ap_id FROM reply LEFT OUTER JOIN post ON (post.id = reply.post) LEFT OUTER JOIN community ON (community.id = post.community) LEFT OUTER JOIN reply AS reply_parent ON (reply_parent.id = reply.parent) WHERE reply.id=$1",
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

            let post_local_id: i64 = row.get(2);

            let community_local_id: i64 = row.get(8);

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

            let parent_local_id = row.get(5);

            let info = crate::CommentInfo {
                author: row.get(0),
                created: row.get(3),
                content_text: row.get(1),
                id: comment_id,
                post: post_local_id,
                parent: parent_local_id,
            };

            let parent_ap_id = match row.get(11) {
                None => None,
                Some(true) => Some(crate::apub_util::get_local_comment_apub_id(parent_local_id.unwrap(), &ctx.host_url_apub)),
                Some(false) => row.get(12),
            };

            let body = crate::apub_util::local_comment_to_ap(&info, &post_ap_id, parent_ap_id.as_deref(), &community_ap_id, &ctx.host_url_apub)?;

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

// sharedInbox
async fn handler_inbox_post(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    inbox_common(ctx, req).await
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
            "SELECT author, href, content_text, title, created, community, local, (SELECT ap_id FROM community WHERE id=post.community) AS community_ap_id FROM post WHERE id=$1",
            &[&post_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such post",
        )),
        Some(row) => {
            let local: bool = row.get(6);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested post is not owned by this instance",
                )));
            }

            let community_local_id = row.get(5);

            let community_ap_id = match row.get(7) {
                Some(ap_id) => ap_id,
                None => {
                    // assume local (might be a problem?)
                    crate::apub_util::get_local_community_apub_id(community_local_id, &ctx.host_url_apub)
                }
            };

            let post_info = crate::PostInfo {
                author: row.get(0),
                community: community_local_id,
                created: &row.get(4),
                href: row.get(1),
                content_text: row.get(2),
                id: post_id,
                title: row.get(3),
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
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;
    let db = ctx.db_pool.get().await?;

    match db
        .query_opt(
            "SELECT author, href, content_text, title, created, community, local, (SELECT ap_id FROM community WHERE id=post.community) AS community_ap_id FROM post WHERE id=$1",
            &[&post_id],
        )
        .await?
    {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such post",
        )),
        Some(row) => {
            let local: bool = row.get(6);

            if !local {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "Requested post is not owned by this instance",
                )));
            }

            let community_local_id = row.get(5);

            let community_ap_id = match row.get(7) {
                Some(ap_id) => ap_id,
                None => {
                    // assume local (might be a problem?)
                    crate::apub_util::get_local_community_apub_id(community_local_id, &ctx.host_url_apub)
                }
            };

            let post_info = crate::PostInfo {
                author: row.get(0),
                community: community_local_id,
                created: &row.get(4),
                href: row.get(1),
                content_text: row.get(2),
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
