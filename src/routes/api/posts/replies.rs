use crate::types::{
    CommentLocalID, ImageHandling, JustURL, PostLocalID, RespAvatarInfo, RespList,
    RespMinimalAuthorInfo, RespMinimalCommentInfo, RespPostCommentInfo, UserLocalID,
};
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::sync::Arc;

async fn route_unstable_posts_replies_list(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    #[derive(Deserialize)]
    struct RepliesListQuery<'a> {
        #[serde(default)]
        include_your: bool,
        #[serde(default = "crate::routes::api::default_replies_limit")]
        limit: u8,
        #[serde(default = "crate::routes::api::default_comment_sort")]
        sort: crate::routes::api::SortType,
        page: Option<Cow<'a, str>>,

        #[serde(default = "crate::routes::api::default_image_handling")]
        image_handling: ImageHandling,
    }

    let query: RepliesListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let db = ctx.db_pool.get().await?;

    let include_your_for = if query.include_your {
        let user = crate::require_login(&req, &db).await?;
        Some(user)
    } else {
        None
    };

    let (replies, next_page) = get_post_comments(
        post_id,
        include_your_for,
        query.sort,
        query.limit,
        query.page.as_deref(),
        query.image_handling,
        &db,
        &ctx,
    )
    .await?;

    let body = RespList {
        items: (&replies).into(),
        next_page: next_page.as_deref().map(Cow::Borrowed),
    };

    crate::json_response(&body)
}

async fn route_unstable_posts_replies_create(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct RepliesCreateBody<'a> {
        content_text: Option<Cow<'a, str>>,
        content_markdown: Option<String>,
        attachment: Option<Cow<'a, str>>,
        sensitive: Option<bool>,
    }

    let body: RepliesCreateBody<'_> = serde_json::from_slice(&body)?;

    if let Some(attachment) = &body.attachment {
        if !attachment.starts_with("local-media://") {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Comment attachment must be local media",
            )));
        }
    }

    let (content_text, content_markdown, content_html, mentions) =
        crate::routes::api::process_comment_content(
            &lang,
            body.content_text,
            body.content_markdown,
            &ctx,
        )
        .await?;

    let sensitive = body.sensitive.unwrap_or(false);

    let (reply_id, created) = {
        let trans = db.transaction().await?;
        let row = trans.query_one(
            "INSERT INTO reply (post, author, created, local, content_text, content_markdown, content_html, attachment_href, sensitive) VALUES ($1, $2, current_timestamp, TRUE, $3, $4, $5, $6, $7) RETURNING id, created",
            &[&post_id, &user, &content_text, &content_markdown, &content_html, &body.attachment, &sensitive],
        ).await?;

        let reply_id = CommentLocalID(row.get(0));
        let created = row.get(1);

        let (nest_person, nest_text): (Vec<_>, Vec<_>) = mentions
            .iter()
            .map(|info| (info.person, &info.text))
            .unzip();

        trans.execute(
            "INSERT INTO reply_mention (reply, person, text) SELECT $1, * FROM UNNEST($2::BIGINT[], $3::TEXT[])",
            &[&reply_id, &nest_person, &nest_text],
        ).await?;

        trans.commit().await?;

        (reply_id, created)
    };

    let comment = crate::CommentInfo {
        id: reply_id,
        author: Some(user),
        post: post_id,
        parent: None,
        content_text: content_text.map(|x| Cow::Owned(x.into_owned())),
        content_markdown: content_markdown.map(Cow::Owned),
        content_html: content_html.map(Cow::Owned),
        created,
        ap_id: crate::APIDOrLocal::Local,
        attachment_href: body.attachment,
        sensitive,
        mentions: mentions.into(),
    };

    crate::on_post_add_comment(comment, ctx);

    crate::json_response(&serde_json::json!({ "id": reply_id }))
}

async fn get_post_comments<'a>(
    post_id: PostLocalID,
    include_your_for: Option<UserLocalID>,
    sort: crate::routes::api::SortType,
    limit: u8,
    page: Option<&'a str>,
    image_handling: ImageHandling,
    db: &tokio_postgres::Client,
    ctx: &'a crate::BaseContext,
) -> Result<(Vec<RespPostCommentInfo<'a>>, Option<String>), crate::Error> {
    use futures::TryStreamExt;

    let limit_i = i64::from(limit) + 1;

    let sql1 = "SELECT reply.id, reply.author, reply.content_text, reply.created, reply.content_html, person.username, person.local, person.ap_id, reply.deleted, person.avatar, attachment_href, reply.local, (SELECT COUNT(*) FROM reply_like WHERE reply = reply.id), reply.content_markdown, person.is_bot, reply.ap_id, reply.local, reply.sensitive";
    let (sql2, mut values): (_, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) =
        if include_your_for.is_some() {
            (
                ", EXISTS(SELECT 1 FROM reply_like WHERE reply = reply.id AND person = $3)",
                vec![&post_id, &limit_i, &include_your_for],
            )
        } else {
            ("", vec![&post_id, &limit_i])
        };
    let mut sql3 = " FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE post=$1 AND parent IS NULL ".to_owned();
    let mut sql4 = format!("ORDER BY {} LIMIT $2", sort.comment_sort_sql());

    let mut con1 = None;
    let mut con2 = None;
    let (page_part1, page_part2) = sort
        .handle_page(
            page,
            "reply",
            false,
            crate::routes::api::ValueConsumer {
                targets: vec![&mut con1, &mut con2],
                start_idx: values.len(),
                used: 0,
            },
        )
        .map_err(super::InvalidPage::into_user_error)?;
    if let Some(value) = &con1 {
        values.push(value.as_ref());
        if let Some(value) = &con2 {
            values.push(value.as_ref());
        }
    }

    if let Some(part) = page_part1 {
        sql3.push_str(&part);
    }
    if let Some(part) = page_part2 {
        sql4.push_str(&part);
    }

    let sql: &str = &format!("{}{}{}{}", sql1, sql2, sql3, sql4);

    let stream = crate::query_stream(db, sql, &values[..]).await?;

    let mut comments: Vec<_> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id = CommentLocalID(row.get(0));
            let content_text: Option<String> = row.get(2);
            let content_html: Option<String> = row.get(4);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);
            let ap_id: Option<String> = row.get(15);
            let local: bool = row.get(16);
            let sensitive: bool = row.get(17);

            let remote_url = if local {
                Some(String::from(
                    crate::apub_util::LocalObjectRef::Comment(id).to_local_uri(&ctx.host_url_apub),
                ))
            } else {
                ap_id
            };

            let author_username: Option<String> = row.get(5);
            let author = author_username.map(|author_username| {
                let author_id = UserLocalID(row.get(1));
                let author_local: bool = row.get(6);
                let author_ap_id: Option<&str> = row.get(7);
                let author_avatar: Option<&str> = row.get(9);

                let author_remote_url = if author_local {
                    Some(String::from(
                        crate::apub_util::LocalObjectRef::User(author_id)
                            .to_local_uri(&ctx.host_url_apub),
                    ))
                } else {
                    author_ap_id.map(ToOwned::to_owned)
                };

                RespMinimalAuthorInfo {
                    id: author_id,
                    username: author_username.into(),
                    local: author_local,
                    host: crate::get_actor_host_or_unknown(
                        author_local,
                        author_ap_id,
                        &ctx.local_hostname,
                    ),
                    remote_url: author_remote_url.map(Cow::Owned),
                    is_bot: row.get(14),
                    avatar: author_avatar.map(|url| RespAvatarInfo {
                        url: ctx.process_avatar_href(url, author_id).into_owned().into(),
                    }),
                }
            });

            futures::future::ok((
                (),
                RespPostCommentInfo {
                    base: RespMinimalCommentInfo {
                        id,
                        remote_url: remote_url.map(Cow::Owned),
                        content_text: content_text.map(From::from),
                        content_html_safe: content_html
                            .map(|html| crate::clean_html(&html, image_handling)),
                        sensitive,
                    },

                    attachments: match ctx
                        .process_attachments_inner(row.get::<_, Option<_>>(10).map(Cow::Owned), id)
                    {
                        None => vec![],
                        Some(href) => vec![JustURL { url: href }],
                    },
                    author,
                    content_markdown: row.get::<_, Option<String>>(13).map(Cow::Owned),
                    created: created.to_rfc3339(),
                    deleted: row.get(8),
                    local: row.get(11),
                    replies: Some(RespList::empty()),
                    score: row.get(12),
                    your_vote: include_your_for.map(|_| {
                        if row.get(18) {
                            Some(crate::types::Empty {})
                        } else {
                            None
                        }
                    }),
                },
            ))
        })
        .try_collect()
        .await?;

    let next_page = if comments.len() > usize::from(limit) {
        Some(sort.get_next_comments_page(comments.pop().unwrap().1, limit, page))
    } else {
        None
    };

    crate::routes::api::apply_comments_replies(
        &mut comments,
        include_your_for,
        2,
        limit,
        sort,
        image_handling,
        db,
        ctx,
    )
    .await?;

    Ok((
        comments.into_iter().map(|(_, comment)| comment).collect(),
        next_page,
    ))
}

pub fn route_replies() -> crate::RouteNode<(PostLocalID,)> {
    crate::RouteNode::new()
        .with_handler_async(hyper::Method::GET, route_unstable_posts_replies_list)
        .with_handler_async(hyper::Method::POST, route_unstable_posts_replies_create)
}
