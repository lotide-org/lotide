use super::{
    JustURL, RespAvatarInfo, RespList, RespMinimalAuthorInfo, RespMinimalCommentInfo,
    RespPostCommentInfo,
};
use crate::lang;
use crate::types::{
    ActorLocalRef, CommentLocalID, CommunityLocalID, JustID, JustUser, MaybeIncludeYour,
    PostLocalID, RespCommentInfo, RespMinimalPostInfo, UserLocalID,
};
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

async fn route_unstable_comments_get(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use futures::future::TryFutureExt;

    let query: MaybeIncludeYour = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let (comment_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let include_your_for = if query.include_your {
        let user = crate::require_login(&req, &db).await?;
        Some(user)
    } else {
        None
    };

    let (row, your_vote) = futures::future::try_join(
        db.query_opt(
            "SELECT reply.author, reply.post, reply.content_text, reply.created, reply.local, reply.content_html, person.username, person.local, person.ap_id, post.title, reply.deleted, reply.parent, person.avatar, reply.attachment_href, (SELECT COUNT(*) FROM reply_like WHERE reply = reply.id), EXISTS(SELECT 1 FROM reply AS r2 WHERE r2.parent = reply.id), reply.content_markdown, person.is_bot, post.ap_id, post.local, reply.ap_id, post.sensitive, reply.sensitive FROM reply INNER JOIN post ON (reply.post = post.id) LEFT OUTER JOIN person ON (reply.author = person.id) WHERE reply.id = $1",
            &[&comment_id],
        )
        .map_err(crate::Error::from),
        async {
            Ok(if let Some(user) = include_your_for {
                let row = db.query_opt(
                    "SELECT 1 FROM reply_like WHERE reply=$1 AND person=$2",
                    &[&comment_id, &user],
                ).await?;

                Some(row.map(|_| crate::types::Empty {}))
            } else {
                None
            })
        },
    ).await?;

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr(&lang::no_such_comment()).into_owned(),
        )),
        Some(row) => {
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);
            let ap_id: Option<&str> = row.get(20);
            let local: bool = row.get(4);

            let remote_url = if local {
                Some(Cow::Owned(String::from(
                    crate::apub_util::LocalObjectRef::Comment(comment_id)
                        .to_local_uri(&ctx.host_url_apub),
                )))
            } else {
                ap_id.map(Cow::Borrowed)
            };

            let author = match row.get(6) {
                Some(author_username) => {
                    let author_id = UserLocalID(row.get(0));
                    let author_local = row.get(7);
                    let author_ap_id: Option<&str> = row.get(8);
                    let author_avatar: Option<&str> = row.get(12);

                    let author_remote_url = if author_local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::User(author_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        author_ap_id.map(Cow::Borrowed)
                    };

                    Some(RespMinimalAuthorInfo {
                        id: author_id,
                        username: Cow::Borrowed(author_username),
                        local: author_local,
                        host: crate::get_actor_host_or_unknown(
                            author_local,
                            author_ap_id,
                            &ctx.local_hostname,
                        ),
                        remote_url: author_remote_url,
                        is_bot: row.get(17),
                        avatar: author_avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, author_id),
                        }),
                    })
                }
                None => None,
            };

            let post = match row.get(9) {
                Some(post_title) => {
                    let post_id = PostLocalID(row.get(1));
                    let post_ap_id: Option<&str> = row.get(18);
                    let post_local: bool = row.get(19);
                    let post_sensitive: bool = row.get(21);

                    let post_remote_url = if post_local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::Post(post_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        post_ap_id.map(Cow::Borrowed)
                    };

                    Some(RespMinimalPostInfo {
                        id: post_id,
                        title: post_title,
                        remote_url: post_remote_url,
                        sensitive: post_sensitive,
                    })
                }
                None => None,
            };

            let output = RespCommentInfo {
                base: RespPostCommentInfo {
                    base: RespMinimalCommentInfo {
                        id: comment_id,
                        remote_url,
                        content_text: row.get::<_, Option<&str>>(2).map(Cow::Borrowed),
                        content_html_safe: row
                            .get::<_, Option<&str>>(5)
                            .map(|html| crate::clean_html(html)),
                        sensitive: row.get(22),
                    },

                    attachments: match ctx.process_attachments_inner(
                        row.get::<_, Option<_>>(13).map(Cow::Borrowed),
                        comment_id,
                    ) {
                        None => vec![],
                        Some(href) => vec![JustURL { url: href }],
                    },
                    author,
                    content_markdown: row.get::<_, Option<&str>>(16).map(Cow::Borrowed),
                    created: created.to_rfc3339(),
                    deleted: row.get(10),
                    local,
                    replies: if row.get(15) {
                        None
                    } else {
                        Some(RespList::empty())
                    },
                    score: row.get(14),
                    your_vote,
                },
                parent: row.get::<_, Option<_>>(11).map(|id| JustID {
                    id: CommentLocalID(id),
                }),
                post,
            };

            crate::json_response(&output)
        }
    }
}

async fn route_unstable_comments_delete(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let mut db = ctx.db_pool.get().await?;

    let login_user = crate::require_login(&req, &db).await?;

    let row = db
        .query_opt(
            "SELECT author, (SELECT community FROM post WHERE id=reply.post), local FROM reply WHERE id=$1 AND deleted=FALSE",
            &[&comment_id],
        )
        .await?;
    match row {
        None => Ok(crate::empty_response()), // already gone
        Some(row) => {
            let author = row.get::<_, Option<_>>(0).map(UserLocalID);
            let is_mod_action = if author != Some(login_user) {
                if row.get(2) && crate::is_site_admin(&db, login_user).await? {
                    // still ok
                    true
                } else {
                    return Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        lang.tr(&lang::comment_not_yours()).into_owned(),
                    )));
                }
            } else {
                false
            };

            let actor = author.unwrap_or(login_user);

            {
                let trans = db.transaction().await?;

                trans.execute(
                    "UPDATE reply SET content_text='[deleted]', content_markdown=NULL, content_html=NULL, deleted=TRUE WHERE id=$1",
                    &[&comment_id],
                )
                .await?;

                if is_mod_action {
                    trans.execute("INSERT INTO modlog_event (time, by_person, action, reply) VALUES (current_timestamp, $1, 'delete_reply', $2)", &[&login_user, &comment_id]).await?;
                }

                trans.commit().await?;
            }

            crate::spawn_task(async move {
                let community = row.get::<_, Option<_>>(1).map(CommunityLocalID);
                if let Some(community) = community {
                    let delete_ap = crate::apub_util::local_comment_delete_to_ap(
                        comment_id,
                        actor,
                        &ctx.host_url_apub,
                    )?;
                    let row = db.query_one("SELECT local, ap_id, COALESCE(ap_shared_inbox, ap_inbox) FROM community WHERE id=$1", &[&community]).await?;

                    let body = serde_json::to_string(&delete_ap)?;

                    let local = row.get(0);
                    if local {
                        crate::spawn_task(
                            crate::apub_util::enqueue_forward_to_community_followers(
                                community, body, ctx,
                            ),
                        );
                    } else {
                        let community_inbox: Option<String> = row.get(2);

                        if let Some(community_inbox) = community_inbox {
                            crate::spawn_task(async move {
                                ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                                    inbox: Cow::Owned(community_inbox.parse()?),
                                    sign_as: Some(ActorLocalRef::Person(actor)),
                                    object: body,
                                })
                                .await
                            });
                        }
                    }
                }

                Ok(())
            });

            Ok(crate::empty_response())
        }
    }
}

async fn route_unstable_comments_like(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let row_count = db.execute(
        "INSERT INTO reply_like (reply, person, local) VALUES ($1, $2, TRUE) ON CONFLICT (reply, person) DO NOTHING",
        &[&comment_id, &user],
    ).await?;

    if row_count > 0 {
        crate::spawn_task(async move {
            let row = db.query_opt(
                "SELECT reply.local, reply.ap_id, community.id, community.local, community.ap_id, COALESCE(community.ap_shared_inbox, community.ap_inbox), COALESCE(comment_author.ap_shared_inbox, comment_author.ap_inbox), comment_author.id, comment_author.ap_id FROM reply LEFT OUTER JOIN post ON (reply.post = post.id) LEFT OUTER JOIN community ON (post.community = community.id) LEFT OUTER JOIN person AS comment_author ON (comment_author.id = reply.author) WHERE reply.id = $1",
                &[&comment_id],
            ).await?;
            if let Some(row) = row {
                let comment_local = row.get(0);
                let comment_ap_id = if comment_local {
                    crate::apub_util::LocalObjectRef::Comment(comment_id)
                        .to_local_uri(&ctx.host_url_apub)
                } else {
                    let src: &str = row.get(1);
                    src.parse()?
                };

                let mut inboxes = HashSet::new();

                if !comment_local {
                    let author_inbox: Option<&str> = row.get(6);
                    if let Some(inbox) = author_inbox {
                        inboxes.insert(inbox);
                    }
                }

                let community_local: Option<bool> = row.get(3);

                if community_local == Some(false) {
                    if let Some(inbox) = row.get(5) {
                        inboxes.insert(inbox);
                    }
                }

                let author_ap_id = if comment_local {
                    Some(
                        crate::apub_util::LocalObjectRef::User(UserLocalID(row.get(7)))
                            .to_local_uri(&ctx.host_url_apub)
                            .into(),
                    )
                } else {
                    row.get::<_, Option<&str>>(8)
                        .map(|x| x.parse())
                        .transpose()?
                };

                let like = crate::apub_util::local_comment_like_to_ap(
                    comment_id,
                    comment_ap_id,
                    author_ap_id,
                    user,
                    &ctx.host_url_apub,
                )?;

                let body = serde_json::to_string(&like)?;

                for inbox in inboxes {
                    ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                        inbox: Cow::Owned(inbox.parse()?),
                        sign_as: Some(ActorLocalRef::Person(user)),
                        object: (&body).into(),
                    })
                    .await?;
                }

                if community_local == Some(true) {
                    let community_local_id = CommunityLocalID(row.get(2));
                    crate::apub_util::enqueue_forward_to_community_followers(
                        community_local_id,
                        body,
                        ctx,
                    )
                    .await?;
                }
            }

            Ok(())
        });
    }

    Ok(crate::empty_response())
}

async fn route_unstable_comments_likes_list(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use chrono::offset::TimeZone;
    use std::convert::TryInto;

    let (comment_id,) = params;

    #[derive(Deserialize)]
    struct LikesListQuery<'a> {
        pub page: Option<Cow<'a, str>>,
    }

    let query: LikesListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;
    let page: Option<(chrono::DateTime<chrono::offset::FixedOffset>, i64)> = query
        .page
        .map(|src| {
            let mut spl = src.split(',');

            let ts = spl.next().ok_or(())?;
            let u = spl.next().ok_or(())?;
            if spl.next().is_some() {
                Err(())
            } else {
                let ts: i64 = ts.parse().map_err(|_| ())?;
                let u: i64 = u.parse().map_err(|_| ())?;

                let ts = chrono::offset::Utc.timestamp_nanos(ts);

                Ok((ts.into(), u))
            }
        })
        .transpose()
        .map_err(|_| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Invalid page",
            ))
        })?;

    let limit: i64 = 30;
    let real_limit = limit + 1;

    let db = ctx.db_pool.get().await?;

    let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&comment_id, &real_limit];
    let page_conditions = match &page {
        Some((ts, u)) => {
            values.push(ts);
            values.push(u);

            " AND (reply_like.created_local < $3 OR (reply_like.created_local = $3 AND reply_like.person <= $4))"
        }
        None => "",
    };

    let sql: &str = &format!("SELECT person.id, person.username, person.local, person.ap_id, reply_like.created_local, person.avatar, person.is_bot FROM reply_like, person WHERE person.id = reply_like.person AND reply_like.reply = $1{} ORDER BY reply_like.created_local DESC, reply_like.person DESC LIMIT $2", page_conditions);

    let mut rows = db.query(sql, &values).await?;

    let next_page = if rows.len() > limit.try_into().unwrap() {
        let row = rows.pop().unwrap();

        let ts: chrono::DateTime<chrono::offset::FixedOffset> = row.get(4);
        let ts = ts.timestamp_nanos();

        let u: i64 = row.get(0);

        Some(format!("{},{}", ts, u))
    } else {
        None
    };

    let likes = rows
        .iter()
        .map(|row| {
            let id = UserLocalID(row.get(0));
            let username: &str = row.get(1);
            let local: bool = row.get(2);
            let ap_id: Option<&str> = row.get(3);
            let avatar: Option<&str> = row.get(5);

            let remote_url = if local {
                Some(Cow::Owned(String::from(
                    crate::apub_util::LocalObjectRef::User(id).to_local_uri(&ctx.host_url_apub),
                )))
            } else {
                ap_id.map(Cow::Borrowed)
            };

            JustUser {
                user: RespMinimalAuthorInfo {
                    id,
                    username: Cow::Borrowed(username),
                    local,
                    host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                    remote_url,
                    is_bot: row.get(6),
                    avatar: avatar.map(|url| RespAvatarInfo {
                        url: ctx.process_avatar_href(url, id),
                    }),
                },
            }
        })
        .collect::<Vec<_>>();

    let body = RespList {
        items: (&likes).into(),
        next_page: next_page.as_deref().map(Cow::Borrowed),
    };

    crate::json_response(&body)
}

async fn route_unstable_comments_unlike(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;

    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let new_undo = {
        let trans = db.transaction().await?;

        let row_count = trans
            .execute(
                "DELETE FROM reply_like WHERE reply=$1 AND person=$2",
                &[&comment_id, &user],
            )
            .await?;

        let new_undo = if row_count > 0 {
            let id = uuid::Uuid::new_v4();
            trans
                .execute(
                    "INSERT INTO local_reply_like_undo (id, reply, person) VALUES ($1, $2, $3)",
                    &[&id, &comment_id, &user],
                )
                .await?;

            Some(id)
        } else {
            None
        };

        trans.commit().await?;

        new_undo
    };

    if let Some(new_undo) = new_undo {
        crate::spawn_task(async move {
            let row = db.query_opt(
                "SELECT reply.local, reply.ap_id, community.id, community.local, community.ap_id, COALESCE(community.ap_shared_inbox, community.ap_inbox), COALESCE(comment_author.ap_shared_inbox, comment_author.ap_inbox), comment_author.id, comment_author.ap_id FROM reply LEFT OUTER JOIN post ON (reply.post = post.id) LEFT OUTER JOIN community ON (post.community = community.id) LEFT OUTER JOIN person AS comment_author ON (comment_author.id = reply.author) WHERE reply.id = $1",
                &[&comment_id],
            ).await?;
            if let Some(row) = row {
                let comment_local: bool = row.get(0);

                let mut inboxes = HashSet::new();

                if !comment_local {
                    let author_inbox: Option<&str> = row.get(6);
                    if let Some(inbox) = author_inbox {
                        inboxes.insert(inbox);
                    }
                }

                let community_local: Option<bool> = row.get(3);

                if community_local == Some(false) {
                    if let Some(inbox) = row.get(5) {
                        inboxes.insert(inbox);
                    }
                }

                let author_ap_id = if comment_local {
                    Some(
                        crate::apub_util::LocalObjectRef::User(UserLocalID(row.get(7)))
                            .to_local_uri(&ctx.host_url_apub)
                            .into(),
                    )
                } else {
                    row.get::<_, Option<&str>>(8)
                        .map(|x| x.parse())
                        .transpose()?
                };

                let undo = crate::apub_util::local_comment_like_undo_to_ap(
                    new_undo,
                    comment_id,
                    author_ap_id,
                    user,
                    &ctx.host_url_apub,
                )?;

                let body = serde_json::to_string(&undo)?;

                for inbox in inboxes {
                    ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                        inbox: Cow::Owned(inbox.parse()?),
                        sign_as: Some(ActorLocalRef::Person(user)),
                        object: (&body).into(),
                    })
                    .await?;
                }

                if community_local == Some(true) {
                    let community_local_id = CommunityLocalID(row.get(2));
                    crate::apub_util::enqueue_forward_to_community_followers(
                        community_local_id,
                        body,
                        ctx,
                    )
                    .await?;
                }
            }

            Ok(())
        });
    }

    Ok(crate::empty_response())
}

async fn route_unstable_comments_replies_list(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;

    #[derive(Deserialize)]
    struct RepliesListQuery<'a> {
        #[serde(default)]
        include_your: bool,
        #[serde(default = "super::default_replies_depth")]
        depth: u8,
        #[serde(default = "super::default_replies_limit")]
        limit: u8,
        #[serde(default = "super::default_comment_sort")]
        sort: super::SortType,
        page: Option<Cow<'a, str>>,
    }

    let query: RepliesListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let db = ctx.db_pool.get().await?;

    let include_your_for = if query.include_your {
        let user = crate::require_login(&req, &db).await?;
        Some(user)
    } else {
        None
    };

    let body: RespList<RespPostCommentInfo> = super::get_comments_replies(
        &[comment_id],
        include_your_for,
        query.depth,
        query.limit,
        query.sort,
        query.page.as_deref(),
        &db,
        &ctx,
    )
    .await?
    .remove(&comment_id)
    .unwrap_or_default()
    .into();

    crate::json_response(&body)
}

async fn route_unstable_comments_replies_create(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (parent_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommentRepliesCreateBody<'a> {
        content_text: Option<Cow<'a, str>>,
        content_markdown: Option<String>,
        attachment: Option<Cow<'a, str>>,
        sensitive: Option<bool>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommentRepliesCreateBody<'_> = serde_json::from_slice(&body)?;

    if let Some(attachment) = &body.attachment {
        if !attachment.starts_with("local-media://") {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Comment attachment must be local media",
            )));
        }
    }

    let (content_text, content_markdown, content_html, mentions) =
        super::process_comment_content(&lang, body.content_text, body.content_markdown, &ctx)
            .await?;

    let post: PostLocalID = match db
        .query_opt("SELECT post FROM reply WHERE id=$1", &[&parent_id])
        .await?
    {
        None => Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr(&lang::no_such_comment()).into_owned(),
        ))),
        Some(row) => Ok(PostLocalID(row.get(0))),
    }?;

    let sensitive = body.sensitive.unwrap_or(false);

    let (reply_id, created) = {
        let trans = db.transaction().await?;

        let row = trans.query_one(
            "INSERT INTO reply (post, parent, author, created, local, content_text, content_markdown, content_html, attachment_href, sensitive) VALUES ($1, $2, $3, current_timestamp, TRUE, $4, $5, $6, $7, $8) RETURNING id, created",
            &[&post, &parent_id, &user, &content_text, &content_markdown, &content_html, &body.attachment, &sensitive],
        ).await?;

        let reply_id = CommentLocalID(row.get(0));
        let created = row.get(1);

        let (nest_person, nest_text): (Vec<_>, Vec<_>) = mentions
            .into_iter()
            .map(|info| (info.person, info.text))
            .unzip();

        trans.execute(
            "INSERT INTO reply_mention (reply, person, text) SELECT $1, * FROM UNNEST($2::BIGINT[], $3::TEXT[])",
            &[&reply_id, &nest_person, &nest_text],
        ).await?;

        trans.commit().await?;

        (reply_id, created)
    };

    let info = crate::CommentInfo {
        id: reply_id,
        author: Some(user),
        post,
        parent: Some(parent_id),
        content_text: content_text.map(|x| Cow::Owned(x.into_owned())),
        content_markdown: content_markdown.map(Cow::Owned),
        content_html: content_html.map(Cow::Owned),
        created,
        ap_id: crate::APIDOrLocal::Local,
        attachment_href: body.attachment,
        sensitive,
    };

    crate::on_post_add_comment(info, ctx);

    crate::json_response(&serde_json::json!({ "id": reply_id, "post": {"id": post} }))
}

pub fn route_comments() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child_parse::<CommentLocalID, _>(
        crate::RouteNode::new()
            .with_handler_async(hyper::Method::GET, route_unstable_comments_get)
            .with_handler_async(hyper::Method::DELETE, route_unstable_comments_delete)
            .with_child(
                "replies",
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, route_unstable_comments_replies_list)
                    .with_handler_async(
                        hyper::Method::POST,
                        route_unstable_comments_replies_create,
                    ),
            )
            .with_child(
                "votes",
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::GET, route_unstable_comments_likes_list),
            )
            .with_child(
                "your_vote",
                crate::RouteNode::new()
                    .with_handler_async(hyper::Method::PUT, route_unstable_comments_like)
                    .with_handler_async(hyper::Method::DELETE, route_unstable_comments_unlike),
            ),
    )
}
