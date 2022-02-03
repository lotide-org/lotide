use crate::{CommentLocalID, CommunityLocalID, PollOptionLocalID, PostLocalID, UserLocalID};
use activitystreams::prelude::*;
use std::borrow::Cow;
use std::ops::Deref;
use std::sync::Arc;

mod communities;
mod posts;

pub fn route_apub() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child(
            "users",
            crate::RouteNode::new().with_child_parse::<UserLocalID, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_users_get)
                    .with_child(
                        "inbox",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::POST, handler_users_inbox_post),
                    )
                    .with_child(
                        "outbox",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_users_outbox_get)
                            .with_child(
                                "page",
                                crate::RouteNode::new()
                                    .with_child_parse::<crate::TimestampOrLatest, _>(
                                        crate::RouteNode::new().with_handler_async(
                                            hyper::Method::GET,
                                            handler_users_outbox_page_get,
                                        ),
                                    ),
                            ),
                    ),
            ),
        )
        .with_child(
            "comments",
            crate::RouteNode::new().with_child_parse::<CommentLocalID, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_comments_get)
                    .with_child(
                        "create",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_comments_create_get),
                    )
                    .with_child(
                        "delete",
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, handler_comments_delete_get),
                    )
                    .with_child(
                        "likes",
                        crate::RouteNode::new().with_child_parse::<UserLocalID, _>(
                            crate::RouteNode::new()
                                .with_handler_async(hyper::Method::GET, handler_comments_likes_get),
                        ),
                    ),
            ),
        )
        .with_child(
            "comment_like_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_comment_like_undos_get),
            ),
        )
        .with_child("communities", communities::route_communities())
        .with_child(
            "community_follow_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_community_follow_undos_get),
            ),
        )
        .with_child("inbox", route_inbox())
        .with_child("posts", posts::route_posts())
        .with_child(
            "post_like_undos",
            crate::RouteNode::new().with_child_parse::<uuid::Uuid, _>(
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_post_like_undos_get),
            ),
        )
}

pub fn route_inbox() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_handler_async(hyper::Method::POST, handler_inbox_post)
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
            "SELECT username, local, public_key, description, description_html, avatar, is_bot FROM person WHERE id=$1",
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
                            log::error!("Warning: public_key is not UTF-8: {:?}", err);
                            None
                        }
                    });

            let description = match row.get(4) {
                Some(description_html) => Some(crate::clean_html(description_html)),
                None => row.get::<_, Option<_>>(3).map(|x| v_htmlescape::escape(x).to_string()),
            };

            let avatar: Option<&str> = row.get(5);

            let is_bot: bool = row.get(6);

            fn format_user<T, K: serde::Serialize + activitystreams::base::AsBase<T> + activitystreams::object::AsObject<T> + activitystreams::markers::Actor>(mut info: K, user_id: UserLocalID, ctx: &crate::RouteContext, username: String, description: Option<String>, avatar: Option<&str>, public_key: Option<&str>) -> Result<Vec<u8>, crate::Error> {
                let user_ap_id =
                    crate::apub_util::get_local_person_apub_id(user_id, &ctx.host_url_apub);

                info.set_many_contexts(vec![
                    activitystreams::context(),
                    activitystreams::security(),
                ]);
                info.set_id(user_ap_id.deref().clone())
                    .set_name(username.as_ref());

                if let Some(description) = description {
                    info.set_summary(description);
                }

                if let Some(avatar) = avatar {
                    let mut attachment = activitystreams::object::Image::new();
                    attachment.set_url(ctx.process_avatar_href(avatar, user_id).into_owned());

                    info.set_icon(attachment.into_any_base()?);
                }

                let endpoints = activitystreams::actor::Endpoints {
                    shared_inbox: Some(
                        crate::apub_util::get_local_shared_inbox(&ctx.host_url_apub).into(),
                    ),
                    ..Default::default()
                };

                let mut info = activitystreams::actor::ApActor::new(
                    {
                        let mut res = user_ap_id.clone();
                        res.path_segments_mut().push("inbox");
                        res.into()
                    },
                    info,
                );
                info.set_outbox(
                    crate::apub_util::get_local_person_outbox_apub_id(user_id, &ctx.host_url_apub)
                        .into(),
                )
                .set_endpoints(endpoints)
                .set_preferred_username(username);

                let key_id = format!("{}/users/{}#main-key", ctx.host_url_apub, user_id);

                let body = if let Some(public_key) = public_key {
                    let public_key_ext = crate::apub_util::PublicKeyExtension {
                        public_key: Some(crate::apub_util::PublicKey {
                            id: (&key_id).into(),
                            owner: user_ap_id.as_str().into(),
                            public_key_pem: public_key.into(),
                            signature_algorithm: Some(crate::apub_util::SIGALG_RSA_SHA256.into()),
                        }),
                    };

                    let info = activitystreams_ext::Ext1::new(info, public_key_ext);

                    serde_json::to_vec(&info)
                } else {
                    serde_json::to_vec(&info)
                }?;

                Ok(body)
            }

            let body = if is_bot {
                format_user(activitystreams::actor::Service::new(), user_id, &ctx, username, description, avatar, public_key)
            } else {
                format_user(activitystreams::actor::Person::new(), user_id, &ctx, username, description, avatar, public_key)
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

    let object = crate::apub_util::verify_incoming_object(req, &db, &ctx).await?;

    crate::apub_util::ingest::ingest_object(
        object,
        crate::apub_util::ingest::FoundFrom::Other,
        ctx,
    )
    .await?;

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn handler_users_inbox_post(
    _: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    inbox_common(ctx, req).await
}

async fn handler_users_outbox_get(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user,) = params;
    let page_ap_id = crate::apub_util::get_local_person_outbox_page_apub_id(
        user,
        &crate::TimestampOrLatest::Latest,
        &ctx.host_url_apub,
    );

    let collection = serde_json::json!({
        "@context": activitystreams::context(),
        "type": activitystreams::collection::kind::OrderedCollectionType::OrderedCollection,
        "id": crate::apub_util::get_local_person_outbox_apub_id(user, &ctx.host_url_apub),
        "first": &page_ap_id,
        "current": &page_ap_id
    });

    let body = serde_json::to_vec(&collection)?.into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
        .body(body)?)
}

async fn handler_users_outbox_page_get(
    params: (UserLocalID, crate::TimestampOrLatest),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use crate::TimestampOrLatest;

    let (user, page) = params;

    let db = ctx.db_pool.get().await?;

    let limit: i64 = 30;

    let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&user, &limit];

    let (extra_conditions_posts, extra_conditions_comments) = match &page {
        TimestampOrLatest::Latest => ("", ""),
        TimestampOrLatest::Timestamp(ts) => {
            values.push(ts);
            (" AND post.created < $3", " AND reply.created < $3")
        }
    };

    let sql: &str = &format!("(SELECT TRUE, post.id, post.href, post.title, post.created, post.content_text, post.content_markdown, post.content_html, community.id, community.local, community.ap_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, community.ap_outbox, community.ap_followers, poll.multiple, (SELECT array_agg(jsonb_build_array(id, name, (SELECT COUNT(*) FROM poll_vote WHERE poll_id = poll.id AND option_id = poll_option.id)) ORDER BY position ASC) FROM poll_option WHERE poll_id=poll.id), poll.closed_at, post.sensitive FROM post INNER JOIN community ON (post.community = community.id) LEFT OUTER JOIN poll ON (poll.id = post.poll_id) WHERE post.author = $1 AND NOT post.deleted{}) UNION ALL (SELECT FALSE, reply.id, reply.content_text, reply.content_html, reply.created, parent_or_post_author.ap_id, reply.content_markdown, parent_reply.ap_id, post.id, post.local, post.ap_id, parent_reply.id, parent_reply.local, parent_or_post_author.id, parent_or_post_author.local, community.id, community.local, community.ap_id, reply.attachment_href, community.ap_outbox, community.ap_followers, NULL, NULL, NULL, reply.sensitive FROM reply INNER JOIN post ON (post.id = reply.post) INNER JOIN community ON (post.community = community.id) LEFT OUTER JOIN reply AS parent_reply ON (parent_reply.id = reply.parent) LEFT OUTER JOIN person AS parent_or_post_author ON (parent_or_post_author.id = COALESCE(parent_reply.author, post.author)) WHERE reply.author = $1 AND NOT reply.deleted{}) ORDER BY created DESC LIMIT $2", extra_conditions_posts, extra_conditions_comments);

    let rows = db.query(sql, &values[..]).await?;

    let mut last_created = None;

    let items: Result<Vec<activitystreams::activity::Create>, _> = rows
        .into_iter()
        .map(|row| {
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);

            if row.get(0) {
                let community_id = CommunityLocalID(row.get(8));
                let community_local = row.get(9);
                let community_ap_id = if community_local {
                    crate::apub_util::get_local_community_apub_id(community_id, &ctx.host_url_apub)
                } else {
                    row.get::<_, &str>(10).parse()?
                };
                let community_ap_outbox = if community_local {
                    Some(crate::apub_util::get_local_community_outbox_apub_id(
                        community_id,
                        &ctx.host_url_apub,
                    ))
                } else {
                    row.get::<_, Option<&str>>(19)
                        .map(|x| x.parse())
                        .transpose()?
                };

                let community_ap_followers = if community_local {
                    Some(crate::apub_util::get_local_community_followers_apub_id(
                        community_id,
                        &ctx.host_url_apub,
                    ))
                } else {
                    row.get::<_, Option<&str>>(20)
                        .map(|x| x.parse())
                        .transpose()?
                };

                let closed_at: Option<chrono::DateTime<chrono::FixedOffset>>;

                let poll = if let Some(multiple) = row.get(21) {
                    Some({
                        let options: Vec<_> = row
                            .get::<_, Vec<postgres_types::Json<(i64, &str, i64)>>>(22)
                            .into_iter()
                            .map(|x| x.0)
                            .map(|(id, name, votes): (i64, &str, i64)| crate::PollOption {
                                id: PollOptionLocalID(id),
                                name,
                                votes: votes as u32,
                            })
                            .collect();

                        closed_at = row.get(23);

                        Cow::Owned(crate::PollInfo {
                            multiple,
                            options: Cow::Owned(options),
                            closed_at: closed_at.as_ref(),
                        })
                    })
                } else {
                    None
                };

                let post_info = crate::PostInfo {
                    id: PostLocalID(row.get(1)),
                    author: Some(user),
                    href: row.get(2),
                    content_text: row.get(5),
                    content_markdown: row.get(6),
                    content_html: row.get(7),
                    title: row.get(3),
                    created: &created,
                    community: community_id,
                    poll,
                    sensitive: row.get(24),
                };

                let res = crate::apub_util::local_post_to_create_ap(
                    &post_info,
                    community_ap_id.into(),
                    community_ap_outbox.map(Into::into),
                    community_ap_followers.map(Into::into),
                    &ctx,
                );
                last_created = Some(created);
                res
            } else {
                let id = CommentLocalID(row.get(1));
                let post_id = PostLocalID(row.get(8));
                let parent_id = row.get::<_, Option<_>>(11).map(CommentLocalID);

                let comment_info = crate::CommentInfo {
                    id,
                    author: Some(user),
                    post: post_id,
                    parent: parent_id,
                    content_text: row.get::<_, Option<_>>(2).map(Cow::Borrowed),
                    content_markdown: row.get::<_, Option<_>>(6).map(Cow::Borrowed),
                    content_html: row.get::<_, Option<_>>(3).map(Cow::Borrowed),
                    created,
                    ap_id: crate::APIDOrLocal::Local,
                    attachment_href: row.get::<_, Option<_>>(18).map(Cow::Borrowed),
                    sensitive: row.get(24),
                };

                let res = crate::apub_util::local_comment_to_create_ap(
                    &comment_info,
                    &(if row.get(9) {
                        crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub).into()
                    } else {
                        std::str::FromStr::from_str(row.get(10))?
                    }),
                    match row.get(12) {
                        Some(true) => Some(
                            crate::apub_util::get_local_comment_apub_id(id, &ctx.host_url_apub)
                                .into(),
                        ),
                        Some(false) => Some(std::str::FromStr::from_str(row.get(7))?),
                        None => None,
                    },
                    match row.get(14) {
                        Some(true) => Some(
                            crate::apub_util::get_local_person_apub_id(
                                UserLocalID(row.get(13)),
                                &ctx.host_url_apub,
                            )
                            .into(),
                        ),
                        Some(false) => Some(std::str::FromStr::from_str(row.get(5))?),
                        None => None,
                    },
                    if row.get(16) {
                        crate::apub_util::get_local_community_apub_id(
                            CommunityLocalID(row.get(15)),
                            &ctx.host_url_apub,
                        )
                        .into()
                    } else {
                        std::str::FromStr::from_str(row.get(17))?
                    },
                    &ctx,
                );

                last_created = Some(created);
                res
            }
        })
        .collect();

    let items = items?;

    let next = last_created.map(|ts| {
        crate::apub_util::get_local_person_outbox_page_apub_id(
            user,
            &crate::TimestampOrLatest::Timestamp(ts),
            &ctx.host_url_apub,
        )
    });

    let info = serde_json::json!({
        "@context": activitystreams::context(),
        "type": activitystreams::collection::kind::OrderedCollectionPageType::OrderedCollectionPage,
        "partOf": crate::apub_util::get_local_person_outbox_apub_id(user, &ctx.host_url_apub),
        "orderedItems": items,
        "next": next,
    });

    let body = serde_json::to_vec(&info)?.into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
        .body(body)?)
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
            "SELECT reply.author, reply.content_text, reply.post, reply.created, reply.local, reply.parent, post.local, post.ap_id, post.community, community.local, community.ap_id, reply_parent.local, reply_parent.ap_id, post_author.id, post_author.local, post_author.ap_id, reply_parent_author.id, reply_parent_author.local, reply_parent_author.ap_id, reply.deleted, reply.content_markdown, reply.content_html, reply.attachment_href, reply.sensitive FROM reply LEFT OUTER JOIN post ON (post.id = reply.post) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) LEFT OUTER JOIN community ON (community.id = post.community) LEFT OUTER JOIN reply AS reply_parent ON (reply_parent.id = reply.parent) LEFT OUTER JOIN person AS reply_parent_author ON (reply_parent_author.id = reply_parent.author) WHERE reply.id=$1",
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
                body
                    .set_former_type("Note".to_owned())
                    .set_context(activitystreams::context())
                    .set_id(crate::apub_util::get_local_comment_apub_id(comment_id, &ctx.host_url_apub).into());

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
                std::str::FromStr::from_str(row.get(10))?
            };

            let post_ap_id = if row.get(6) {
                crate::apub_util::get_local_post_apub_id(post_local_id, &ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(7))?
            };

            let parent_local_id = row.get::<_, Option<_>>(5).map(CommentLocalID);

            let content_text = row.get::<_, Option<_>>(1).map(Cow::Borrowed);
            let content_markdown = row.get::<_, Option<_>>(20).map(Cow::Borrowed);
            let content_html = row.get::<_, Option<_>>(21).map(Cow::Borrowed);

            let attachment_href = row.get::<_, Option<_>>(22).map(Cow::Borrowed);

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
                attachment_href,
                sensitive: row.get(23),
            };

            let parent_ap_id = match row.get(11) {
                None => None,
                Some(true) => Some(crate::apub_util::get_local_comment_apub_id(parent_local_id.unwrap(), &ctx.host_url_apub)),
                Some(false) => row.get::<_, Option<&str>>(12).map(|x| x.parse()).transpose()?,
            };

            let post_or_parent_author_ap_id = match parent_local_id {
                None => {
                    // no parent comment, use post
                    match row.get(14) {
                        Some(post_author_local) => {
                            if post_author_local {
                                Some(crate::apub_util::get_local_person_apub_id(UserLocalID(row.get(13)), &ctx.host_url_apub))
                            } else {
                                Some(std::str::FromStr::from_str(row.get(15))?)
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
                                Some(std::str::FromStr::from_str(row.get(18))?)
                            }
                        },
                        None => None,
                    }
                },
            };

            let body = crate::apub_util::local_comment_to_ap(&info, &post_ap_id, parent_ap_id.map(From::from), post_or_parent_author_ap_id.map(From::from), community_ap_id.into(), &ctx)?;

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
            "SELECT reply.author, reply.content_text, reply.post, reply.created, reply.local, reply.parent, post.local, post.ap_id, post.community, community.local, community.ap_id, reply_parent.local, reply_parent.ap_id, post_author.id, post_author.local, post_author.ap_id, reply_parent_author.id, reply_parent_author.local, reply_parent_author.ap_id, reply.deleted, reply.content_markdown, reply.content_html, reply.attachment_href, reply.sensitive FROM reply LEFT OUTER JOIN post ON (post.id = reply.post) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) LEFT OUTER JOIN community ON (community.id = post.community) LEFT OUTER JOIN reply AS reply_parent ON (reply_parent.id = reply.parent) LEFT OUTER JOIN person AS reply_parent_author ON (reply_parent_author.id = reply_parent.author) WHERE reply.id=$1",
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
                std::str::FromStr::from_str(row.get(10))?
            };

            let post_ap_id = if row.get(6) {
                crate::apub_util::get_local_post_apub_id(post_local_id, &ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(7))?
            };

            let parent_local_id = row.get::<_, Option<_>>(5).map(CommentLocalID);

            let content_text = row.get::<_, Option<_>>(1).map(Cow::Borrowed);
            let content_markdown = row.get::<_, Option<_>>(20).map(Cow::Borrowed);
            let content_html = row.get::<_, Option<_>>(21).map(Cow::Borrowed);

            let attachment_href = row.get::<_, Option<_>>(22).map(Cow::Borrowed);

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
                attachment_href,
                sensitive: row.get(23),
            };

            let parent_ap_id = match row.get(11) {
                None => None,
                Some(true) => Some(crate::apub_util::get_local_comment_apub_id(parent_local_id.unwrap(), &ctx.host_url_apub)),
                Some(false) => row.get::<_, Option<&str>>(12).map(std::str::FromStr::from_str).transpose()?,
            };

            let post_or_parent_author_ap_id = match parent_local_id {
                None => {
                    // no parent comment, use post
                    match row.get(14) {
                        Some(post_author_local) => {
                            if post_author_local {
                                Some(crate::apub_util::get_local_person_apub_id(UserLocalID(row.get(13)), &ctx.host_url_apub))
                            } else {
                                Some(std::str::FromStr::from_str(row.get(15))?)
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
                                Some(std::str::FromStr::from_str(row.get(18))?)
                            }
                        },
                        None => None,
                    }
                },
            };

            let body = crate::apub_util::local_comment_to_create_ap(&info, &post_ap_id, parent_ap_id.map(From::from), post_or_parent_author_ap_id.map(From::from), community_ap_id.into(), &ctx)?;

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
                crate::apub_util::get_local_comment_apub_id(comment_id, &ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(1))?
            };

            let like = crate::apub_util::local_comment_like_to_ap(
                comment_id,
                comment_ap_id,
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
