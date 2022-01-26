use crate::{CommunityLocalID, PollOptionLocalID, PostLocalID, UserLocalID};
use activitystreams::prelude::*;
use std::borrow::Cow;
use std::sync::Arc;

pub fn route_posts() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child_parse::<PostLocalID, _>(
        crate::RouteNode::new()
            .with_handler_async(hyper::Method::GET, handler_posts_get)
            .with_child(
                "create",
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_posts_create_get),
            )
            .with_child(
                "delete",
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, handler_posts_delete_get),
            )
            .with_child(
                "likes",
                crate::RouteNode::new().with_child_parse::<UserLocalID, _>(
                    crate::RouteNode::new()
                        .with_handler_async(hyper::Method::GET, handler_posts_likes_get),
                ),
            ),
    )
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
            "SELECT post.author, post.href, post.title, post.created, post.community, post.local, post.deleted, post.had_href, post.content_text, post.content_markdown, post.content_html, community.ap_id, community.ap_outbox, community.local, community.ap_followers, poll.multiple, (SELECT array_agg(jsonb_build_array(id, name, (SELECT COUNT(*) FROM poll_vote WHERE poll_id = poll.id AND option_id = poll_option.id)) ORDER BY position ASC) FROM poll_option WHERE poll_id=poll.id) FROM post INNER JOIN community ON (post.community = community.id) LEFT OUTER JOIN poll ON (poll.id = post.poll_id) WHERE post.id=$1",
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
                body
                    .set_former_type(
                        (if had_href == Some(true) { "Page" } else { "Note" }).to_owned()
                    )
                    .set_context(activitystreams::context())
                    .set_id(crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub).into());

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
            let community_local: bool = row.get(13);

            let community_ap_id = match row.get(11) {
                Option::<&str>::Some(ap_id) => ap_id.parse()?,
                None => {
                    if community_local {
                        crate::apub_util::get_local_community_apub_id(community_local_id, &ctx.host_url_apub)
                    } else {
                        return Err(crate::Error::InternalStrStatic("Missing community AP id"));
                    }
                }
            };

            let community_ap_outbox = match row.get(12) {
                Option::<&str>::Some(ap_outbox) => Some(ap_outbox.parse()?),
                None => {
                    if community_local {
                        Some(crate::apub_util::get_local_community_outbox_apub_id(community_local_id, &ctx.host_url_apub))
                    } else {
                        None
                    }
                }
            };

            let community_ap_followers = match row.get(14) {
                Option::<&str>::Some(ap_followers) => Some(ap_followers.parse()?),
                None => {
                    if community_local {
                        Some(crate::apub_util::get_local_community_followers_apub_id(community_local_id, &ctx.host_url_apub))
                    } else {
                        None
                    }
                }
            };

            let poll = if let Some(multiple) = row.get(15) {
                Some({
                    let options: Vec<_> = row.get::<_, Vec<postgres_types::Json<(i64, &str, i64)>>>(16)
                        .into_iter()
                        .map(|x| x.0)
                        .map(|(id, name, votes): (i64, &str, i64)| {
                            crate::PollOption {
                                id: PollOptionLocalID(id),
                                name,
                                votes: votes as u32,
                            }
                        })
                        .collect();

                    Cow::Owned(crate::PollInfo {
                        multiple,
                        options: Cow::Owned(options),
                    })
                })
            } else {
                None
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
                poll,
            };

            let body = crate::apub_util::post_to_ap(&post_info, community_ap_id.into(), community_ap_outbox.map(Into::into), community_ap_followers.map(Into::into), &ctx)?;

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
            "SELECT post.author, post.href, post.title, post.created, post.community, post.local, post.deleted, post.content_text, post.content_markdown, post.content_html, community.ap_id, community.ap_outbox, community.local, community.ap_followers, poll.multiple, (SELECT array_agg(jsonb_build_array(id, name, (SELECT COUNT(*) FROM poll_vote WHERE poll_id = poll.id AND option_id = poll_option.id)) ORDER BY position ASC) FROM poll_option WHERE poll_id=poll.id) FROM post INNER JOIN community ON (community.id = post.community) LEFT OUTER JOIN poll ON (poll.id = post.poll_id) WHERE post.id=$1",
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
            let community_local: bool = row.get(12);

            let community_ap_id = match row.get(10) {
                Option::<&str>::Some(ap_id) => ap_id.parse()?,
                None => {
                    if community_local {
                        crate::apub_util::get_local_community_apub_id(community_local_id, &ctx.host_url_apub)
                    } else {
                        return Err(crate::Error::InternalStrStatic("Missing community AP id"));
                    }
                }
            };

            let community_ap_outbox = match row.get(11) {
                Option::<&str>::Some(ap_outbox) => Some(ap_outbox.parse()?),
                None => {
                    if community_local {
                        Some(crate::apub_util::get_local_community_outbox_apub_id(community_local_id, &ctx.host_url_apub))
                    } else {
                        None
                    }
                }
            };

            let community_ap_followers = match row.get(13) {
                Option::<&str>::Some(ap_followers) => Some(ap_followers.parse()?),
                None => {
                    if community_local {
                        Some(crate::apub_util::get_local_community_followers_apub_id(community_local_id, &ctx.host_url_apub))
                    } else {
                        None
                    }
                }
            };

            let poll = if let Some(multiple) = row.get(14) {
                Some({
                    let options: Vec<_> = row.get::<_, Vec<postgres_types::Json<(i64, &str, i64)>>>(15)
                        .into_iter()
                        .map(|x| x.0)
                        .map(|(id, name, votes): (i64, &str, i64)| {
                            crate::PollOption {
                                id: PollOptionLocalID(id),
                                name,
                                votes: votes as u32,
                            }
                        })
                        .collect();

                    Cow::Owned(crate::PollInfo {
                        multiple,
                        options: Cow::Owned(options),
                    })
                })
            } else {
                None
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
                title: row.get(2),
                poll,
            };

            let body = crate::apub_util::local_post_to_create_ap(&post_info, community_ap_id.into(), community_ap_outbox.map(Into::into), community_ap_followers.map(Into::into), &ctx)?;

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
                crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub)
            } else {
                std::str::FromStr::from_str(row.get(1))?
            };

            let like = crate::apub_util::local_post_like_to_ap(
                post_id,
                post_ap_id,
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
