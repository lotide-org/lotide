use super::{
    InvalidPage, JustURL, RespAvatarInfo, RespList, RespMinimalAuthorInfo, RespMinimalCommentInfo,
    RespMinimalCommunityInfo, RespPostCommentInfo, RespPostListPost, ValueConsumer,
};
use crate::types::{
    ActorLocalRef, CommentLocalID, CommunityLocalID, JustUser, PostLocalID, RespPostInfo,
    UserLocalID,
};
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::Write;
use std::sync::Arc;

async fn get_post_comments<'a>(
    post_id: PostLocalID,
    include_your_for: Option<UserLocalID>,
    sort: super::SortType,
    limit: u8,
    page: Option<&'a str>,
    db: &tokio_postgres::Client,
    ctx: &'a crate::BaseContext,
) -> Result<(Vec<RespPostCommentInfo<'a>>, Option<String>), crate::Error> {
    use futures::TryStreamExt;

    let limit_i = i64::from(limit) + 1;

    let sql1 = "SELECT reply.id, reply.author, reply.content_text, reply.created, reply.content_html, person.username, person.local, person.ap_id, reply.deleted, person.avatar, attachment_href, reply.local, (SELECT COUNT(*) FROM reply_like WHERE reply = reply.id), reply.content_markdown";
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
            ValueConsumer {
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

            let author_username: Option<String> = row.get(5);
            let author = author_username.map(|author_username| {
                let author_id = UserLocalID(row.get(1));
                let author_local: bool = row.get(6);
                let author_ap_id: Option<&str> = row.get(7);
                let author_avatar: Option<&str> = row.get(9);

                RespMinimalAuthorInfo {
                    id: author_id,
                    username: author_username.into(),
                    local: author_local,
                    host: crate::get_actor_host_or_unknown(
                        author_local,
                        author_ap_id,
                        &ctx.local_hostname,
                    ),
                    remote_url: author_ap_id.map(|x| x.to_owned().into()),
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
                        content_text: content_text.map(From::from),
                        content_html_safe: content_html.map(|html| crate::clean_html(&html)),
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
                    your_vote: match include_your_for {
                        None => None,
                        Some(_) => Some(if row.get(14) {
                            Some(crate::types::Empty {})
                        } else {
                            None
                        }),
                    },
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

    super::apply_comments_replies(&mut comments, include_your_for, 2, limit, db, &ctx).await?;

    Ok((
        comments.into_iter().map(|(_, comment)| comment).collect(),
        next_page,
    ))
}

async fn route_unstable_posts_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum PostsListExtraSortType {
        Relevant,
    }

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum PostsListSortType {
        Normal(super::SortType),
        Extra(PostsListExtraSortType),
    }

    impl PostsListSortType {
        fn get_next_posts_page(
            &self,
            post: &RespPostListPost<'_>,
            sort_sticky: bool,
            limit: u8,
            current_page: Option<&str>,
        ) -> String {
            match self {
                Self::Normal(inner) => {
                    inner.get_next_posts_page(post, sort_sticky, limit, current_page)
                }
                Self::Extra(PostsListExtraSortType::Relevant) => super::format_number_58(
                    i64::from(limit)
                        + match current_page {
                            None => 0,
                            Some(current_page) => super::parse_number_58(current_page).unwrap(),
                        },
                ),
            }
        }

        pub fn handle_page(
            &self,
            page: Option<&str>,
            sort_sticky: bool,
            mut value_out: ValueConsumer,
        ) -> Result<(Option<String>, Option<String>), InvalidPage> {
            match self {
                Self::Extra(sort) => {
                    match page {
                        None => Ok((None, None)),
                        Some(page) => match sort {
                            PostsListExtraSortType::Relevant => {
                                // TODO maybe
                                let page: i64 =
                                    super::parse_number_58(page).map_err(|_| InvalidPage)?;
                                let idx = value_out.push(page);
                                Ok((None, Some(format!(" OFFSET ${}", idx))))
                            }
                        },
                    }
                }
                Self::Normal(sort) => sort.handle_page(page, "post", sort_sticky, value_out),
            }
        }
    }

    impl Default for PostsListSortType {
        fn default() -> Self {
            Self::Normal(super::SortType::Hot)
        }
    }

    fn default_limit() -> u8 {
        30
    }

    #[derive(Deserialize)]
    struct PostsListQuery<'a> {
        in_any_local_community: Option<bool>,
        in_your_follows: Option<bool>,
        search: Option<Cow<'a, str>>,
        #[serde(default)]
        use_aggregate_filters: bool,
        community: Option<CommunityLocalID>,

        #[serde(default = "default_limit")]
        limit: u8,

        page: Option<Cow<'a, str>>,

        #[serde(default)]
        include_your: bool,

        #[serde(default)]
        sort: PostsListSortType,

        #[serde(default)]
        sort_sticky: bool,
    }

    let query: PostsListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let include_your_for = if query.include_your {
        let user = crate::require_login(&req, &db).await?;
        Some(user)
    } else {
        None
    };

    let limit_plus_1: i64 = (query.limit + 1).into();

    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&limit_plus_1];

    let search_value_idx = if let Some(search) = &query.search {
        values.push(search);
        Some(values.len())
    } else {
        None
    };

    let include_your_idx = if let Some(user) = &include_your_for {
        values.push(user);
        Some(values.len())
    } else {
        None
    };

    let mut sql = format!(
        "SELECT {}",
        super::common_posts_list_query(include_your_idx)
    );

    let relevance_sql = if let Some(search_value_idx) = search_value_idx {
        Some(format!("ts_rank_cd(to_tsvector('english', title || ' ' || COALESCE(content_text, content_markdown, content_html, '')), plainto_tsquery('english', ${}))", search_value_idx))
    } else {
        None
    };

    if let Some(relevance_sql) = &relevance_sql {
        sql.push_str(", ");
        sql.push_str(relevance_sql);
    }

    sql.push_str( " FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND deleted=FALSE");
    if query.use_aggregate_filters {
        sql.push_str(" AND community.hide_posts_from_aggregates=FALSE");
    }
    if let Some(search_value_idx) = &search_value_idx {
        write!(sql, " AND to_tsvector('english', title || ' ' || COALESCE(content_text, content_markdown, content_html, '')) @@ plainto_tsquery('english', ${})", search_value_idx).unwrap();
    }
    if let Some(value) = query.in_any_local_community {
        write!(
            sql,
            " AND {}(community.local AND post.approved)",
            if value { "" } else { "NOT " }
        )
        .unwrap();
    }
    if let Some(value) = query.in_your_follows {
        if let Some(include_your_idx) = include_your_idx {
            write!(
                sql,
                " AND {}(community.id IN (SELECT community FROM community_follow WHERE accepted AND follower=${}) AND post.approved)",
                if value { "" } else { "NOT " },
                include_your_idx,
            ).unwrap();
        } else {
            return Err(crate::Error::InternalStrStatic(
                "in_your_follows can only be used with include_your=true",
            ));
        }
    }
    if let Some(value) = &query.community {
        values.push(value);
        write!(sql, " AND community.id=${} AND post.approved", values.len(),).unwrap();
    }

    let mut con1 = None;
    let mut con2 = None;
    let (page_part1, page_part2) = query
        .sort
        .handle_page(
            query.page.as_deref(),
            query.sort_sticky,
            ValueConsumer {
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
        sql.push_str(&part);
    }

    sql.push_str(" ORDER BY ");
    if query.sort_sticky {
        sql.push_str("sticky DESC, ");
    }
    match &query.sort {
        PostsListSortType::Normal(ty) => sql.push_str(ty.post_sort_sql()),
        PostsListSortType::Extra(PostsListExtraSortType::Relevant) => {
            if let Some(relevance_sql) = relevance_sql {
                write!(sql, "{} DESC, post.id DESC", relevance_sql).unwrap();
            } else {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    lang.tr("sort_relevant_not_search", None).into_owned(),
                )));
            }
        }
    }
    sql.push_str(" LIMIT $1");

    if let Some(part) = page_part2 {
        sql.push_str(&part);
    }

    let sql: &str = &sql;

    let stream = crate::query_stream(&db, sql, &values).await?;

    let posts = super::handle_common_posts_list(
        stream,
        &ctx,
        include_your_for.is_some(),
        search_value_idx.is_some(),
    )
    .await?;
    let output = if posts.len() > query.limit as usize {
        let last_post = &posts[posts.len() - 1];

        RespList {
            next_page: Some(Cow::Owned(query.sort.get_next_posts_page(
                last_post,
                query.sort_sticky,
                query.limit,
                query.page.as_deref(),
            ))),
            items: Cow::Borrowed(&posts[0..(posts.len() - 1)]),
        }
    } else {
        RespList {
            items: posts.into(),
            next_page: None,
        }
    };

    crate::json_response(&output)
}

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
        #[serde(default = "super::default_replies_limit")]
        limit: u8,
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

    let (replies, next_page) = get_post_comments(
        post_id,
        include_your_for,
        super::SortType::Hot,
        query.limit,
        query.page.as_deref(),
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

async fn route_unstable_posts_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct PostsCreateBody {
        community: CommunityLocalID,
        href: Option<String>,
        content_markdown: Option<String>,
        content_text: Option<String>,
        title: String,
    }

    let body: PostsCreateBody = serde_json::from_slice(&body)?;

    if body.href.is_none() && body.content_text.is_none() && body.content_markdown.is_none() {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr("post_needs_content", None).into_owned(),
        )));
    }

    if body.content_markdown.is_some() && body.content_text.is_some() {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr("post_content_conflict", None).into_owned(),
        )));
    }

    if let Some(href) = &body.href {
        if url::Url::parse(href).is_err() {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr("post_href_invalid", None).into_owned(),
            )));
        }
    }

    // TODO validate permissions to post

    let (content_text, content_markdown, content_html) = match body.content_markdown {
        Some(md) => {
            let (html, md) =
                tokio::task::spawn_blocking(move || (crate::render_markdown(&md), md)).await?;
            (None, Some(md), Some(html))
        }
        None => match body.content_text {
            Some(text) => (Some(text), None, None),
            None => (None, None, None),
        },
    };

    let community_row = db
        .query_opt(
            "SELECT local FROM community WHERE id=$1",
            &[&body.community],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr("no_such_community", None).into_owned(),
            ))
        })?;

    let community_local: bool = community_row.get(0);
    let already_approved = community_local;

    let res_row = db.query_one(
        "INSERT INTO post (author, href, title, created, community, local, content_text, content_markdown, content_html, approved) VALUES ($1, $2, $3, current_timestamp, $4, TRUE, $5, $6, $7, $8) RETURNING id, created",
        &[&user, &body.href, &body.title, &body.community, &content_text, &content_markdown, &content_html, &already_approved],
    ).await?;

    let id = PostLocalID(res_row.get(0));
    let created = res_row.get(1);

    let post = crate::PostInfoOwned {
        id,
        author: Some(user),
        content_text,
        content_markdown,
        content_html,
        href: body.href,
        title: body.title,
        created,
        community: body.community,
    };

    crate::spawn_task(async move {
        if community_local {
            crate::on_local_community_add_post(
                post.community,
                post.id,
                crate::apub_util::get_local_post_apub_id(post.id, &ctx.host_url_apub).into(),
                ctx,
            );
        } else {
            crate::apub_util::spawn_enqueue_send_local_post_to_community(post, ctx);
        }

        Ok(())
    });

    crate::json_response(&serde_json::json!({ "id": id }))
}

async fn route_unstable_posts_get(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use futures::future::TryFutureExt;

    #[derive(Deserialize)]
    struct PostsGetQuery {
        #[serde(default)]
        include_your: bool,
    }

    let query: PostsGetQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let include_your_for = if query.include_your {
        let user = crate::require_login(&req, &db).await?;
        Some(user)
    } else {
        None
    };

    let (post_id,) = params;

    let (row, your_vote) = futures::future::try_join(
        db.query_opt(
            "SELECT post.author, post.href, post.content_text, post.title, post.created, post.content_html, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id, (SELECT COUNT(*) FROM post_like WHERE post_like.post = $1), post.approved, person.avatar, post.local, post.sticky, post.content_markdown FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND post.id = $1",
            &[&post_id],
        )
        .map_err(crate::Error::from),
        async {
            if let Some(user) = include_your_for {
                let row = db.query_opt("SELECT 1 FROM post_like WHERE post=$1 AND person=$2", &[&post_id, &user]).await?;
                if row.is_some() {
                    Ok(Some(Some(crate::types::Empty {})))
                } else {
                    Ok(Some(None))
                }
            } else {
                Ok(None)
            }
        }
    ).await?;

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr("no_such_post", None).into_owned(),
        )),
        Some(row) => {
            let href: Option<&str> = row.get(1);
            let content_text: Option<&str> = row.get(2);
            let content_html: Option<&str> = row.get(5);
            let title: &str = row.get(3);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);
            let community_id = CommunityLocalID(row.get(6));
            let community_name: &str = row.get(7);
            let community_local = row.get(8);
            let community_ap_id: Option<&str> = row.get(9);

            let author = match row.get(10) {
                Some(author_username) => {
                    let author_id = UserLocalID(row.get(0));
                    let author_local = row.get(11);
                    let author_ap_id = row.get(12);
                    let author_avatar: Option<&str> = row.get(15);
                    Some(RespMinimalAuthorInfo {
                        id: author_id,
                        username: Cow::Borrowed(author_username),
                        local: author_local,
                        host: crate::get_actor_host_or_unknown(
                            author_local,
                            author_ap_id,
                            &ctx.local_hostname,
                        ),
                        remote_url: author_ap_id.map(From::from),
                        avatar: author_avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, author_id),
                        }),
                    })
                }
                None => None,
            };

            let community = RespMinimalCommunityInfo {
                id: community_id,
                name: Cow::Borrowed(community_name),
                local: community_local,
                host: crate::get_actor_host_or_unknown(
                    community_local,
                    community_ap_id,
                    &ctx.local_hostname,
                ),
                remote_url: community_ap_id.map(Cow::Borrowed),
            };

            let post = RespPostListPost {
                id: post_id,
                title: Cow::Borrowed(title),
                href: ctx.process_href_opt(href.map(Cow::Borrowed), post_id),
                content_text: content_text.map(Cow::Borrowed),
                content_html_safe: content_html.map(|html| crate::clean_html(&html)),
                author: author.map(Cow::Owned),
                created: Cow::Owned(created.to_rfc3339()),
                community: Cow::Owned(community),
                relevance: None,
                replies_count_total: None,
                score: row.get(13),
                sticky: row.get(17),
                your_vote,
            };

            let output = RespPostInfo {
                post: &post,
                content_markdown: row.get(18),
                local: row.get(16),
                approved: row.get(14),
            };

            crate::json_response(&output)
        }
    }
}

async fn route_unstable_posts_delete(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let login_user = crate::require_login(&req, &db).await?;

    let row = db
        .query_opt(
            "SELECT author, community, local FROM post WHERE id=$1 AND deleted=FALSE",
            &[&post_id],
        )
        .await?;
    match row {
        None => Ok(crate::empty_response()), // already gone
        Some(row) => {
            let author = row.get::<_, Option<_>>(0).map(UserLocalID);
            if author != Some(login_user) {
                if row.get(2) && crate::is_site_admin(&db, login_user).await? {
                    // still ok
                } else {
                    return Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        lang.tr("post_not_yours", None).into_owned(),
                    )));
                }
            }

            let actor = author.unwrap_or(login_user);

            db.execute("UPDATE post SET had_href=(href IS NOT NULL), href=NULL, title='[deleted]', content_text='[deleted]', content_markdown=NULL, content_html=NULL, deleted=TRUE WHERE id=$1", &[&post_id]).await?;

            crate::spawn_task(async move {
                let community = row.get::<_, Option<_>>(1).map(CommunityLocalID);
                if let Some(community) = community {
                    let delete_ap = crate::apub_util::local_post_delete_to_ap(
                        post_id,
                        actor,
                        &ctx.host_url_apub,
                    )?;
                    let row = db.query_one("SELECT local, ap_id, COALESCE(ap_shared_inbox, ap_inbox) FROM community WHERE id=$1", &[&community]).await?;

                    let local = row.get(0);
                    if local {
                        crate::spawn_task(
                            crate::apub_util::enqueue_forward_to_community_followers(
                                community,
                                serde_json::to_string(&delete_ap)?,
                                ctx,
                            ),
                        );
                    } else {
                        let community_inbox: Option<String> = row.get(2);

                        if let Some(community_inbox) = community_inbox {
                            crate::spawn_task(async move {
                                ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                                    inbox: Cow::Owned(community_inbox.parse()?),
                                    sign_as: Some(ActorLocalRef::Person(actor)),
                                    object: serde_json::to_string(&delete_ap)?,
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

async fn route_unstable_posts_like(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let row_count = db.execute(
        "INSERT INTO post_like (post, person, local) VALUES ($1, $2, TRUE) ON CONFLICT (post, person) DO NOTHING",
        &[&post_id, &user],
    ).await?;

    if row_count > 0 {
        crate::spawn_task(async move {
            let row = db.query_opt(
                "SELECT post.local, post.ap_id, community.id, community.local, community.ap_id, COALESCE(community.ap_shared_inbox, community.ap_inbox), COALESCE(post_author.ap_shared_inbox, post_author.ap_inbox) FROM post LEFT OUTER JOIN community ON (post.community = community.id) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) WHERE post.id = $1",
                &[&post_id],
            ).await?;
            if let Some(row) = row {
                let post_local = row.get(0);
                let post_ap_id = if post_local {
                    crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub)
                } else {
                    row.get::<_, &str>(1).parse()?
                };

                let mut inboxes = HashSet::new();

                if !post_local {
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

                let like = crate::apub_util::local_post_like_to_ap(
                    post_id,
                    post_ap_id,
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

async fn route_unstable_posts_likes_list(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use chrono::offset::TimeZone;
    use std::convert::TryInto;

    let (post_id,) = params;

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

    let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&post_id, &real_limit];
    let page_conditions = match &page {
        Some((ts, u)) => {
            values.push(ts);
            values.push(u);

            " AND (post_like.created_local < $3 OR (post_like.created_local = $3 AND post_like.person <= $4))"
        }
        None => "",
    };

    let sql: &str = &format!("SELECT person.id, person.username, person.local, person.ap_id, post_like.created_local, person.avatar FROM post_like, person WHERE person.id = post_like.person AND post_like.post = $1{} ORDER BY post_like.created_local DESC, post_like.person DESC LIMIT $2", page_conditions);

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

            JustUser {
                user: RespMinimalAuthorInfo {
                    id,
                    username: Cow::Borrowed(username),
                    local,
                    host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                    remote_url: ap_id.map(From::from),
                    avatar: avatar.map(|url| RespAvatarInfo {
                        url: ctx.process_avatar_href(url, id),
                    }),
                },
            }
        })
        .collect::<Vec<_>>();

    let body = serde_json::json!({
        "items": likes,
        "next_page": next_page,
    });
    crate::json_response(&body)
}

async fn route_unstable_posts_unlike(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let new_undo = {
        let trans = db.transaction().await?;

        let row_count = trans
            .execute(
                "DELETE FROM post_like WHERE post=$1 AND person=$2",
                &[&post_id, &user],
            )
            .await?;

        let new_undo = if row_count > 0 {
            let id = uuid::Uuid::new_v4();
            trans
                .execute(
                    "INSERT INTO local_post_like_undo (id, post, person) VALUES ($1, $2, $3)",
                    &[&id, &post_id, &user],
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
                "SELECT post.local, community.id, community.local, community.ap_id, COALESCE(community.ap_shared_inbox, community.ap_inbox), COALESCE(post_author.ap_shared_inbox, post_author.ap_inbox) FROM post LEFT OUTER JOIN community ON (post.community = community.id) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) WHERE post.id = $1",
                &[&post_id],
            ).await?;
            if let Some(row) = row {
                let post_local: bool = row.get(0);

                let mut inboxes = HashSet::new();

                if !post_local {
                    let author_inbox: Option<&str> = row.get(5);
                    if let Some(inbox) = author_inbox {
                        inboxes.insert(inbox);
                    }
                }

                let community_local: Option<bool> = row.get(2);

                if community_local == Some(false) {
                    if let Some(inbox) = row.get(4) {
                        inboxes.insert(inbox);
                    }
                }

                let undo = crate::apub_util::local_post_like_undo_to_ap(
                    new_undo,
                    post_id,
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
                    let community_local_id = CommunityLocalID(row.get(1));
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

async fn route_unstable_posts_replies_create(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct RepliesCreateBody<'a> {
        content_text: Option<Cow<'a, str>>,
        content_markdown: Option<String>,
        attachment: Option<Cow<'a, str>>,
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

    let (content_text, content_markdown, content_html) =
        super::process_comment_content(&lang, body.content_text, body.content_markdown).await?;

    let row = db.query_one(
        "INSERT INTO reply (post, author, created, local, content_text, content_markdown, content_html, attachment_href) VALUES ($1, $2, current_timestamp, TRUE, $3, $4, $5, $6) RETURNING id, created",
        &[&post_id, &user, &content_text, &content_markdown, &content_html, &body.attachment],
    ).await?;

    let reply_id = CommentLocalID(row.get(0));
    let created = row.get(1);

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
    };

    crate::on_post_add_comment(comment, ctx);

    crate::json_response(&serde_json::json!({ "id": reply_id }))
}

pub fn route_posts() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_handler_async("GET", route_unstable_posts_list)
        .with_handler_async("POST", route_unstable_posts_create)
        .with_child_parse::<PostLocalID, _>(
            crate::RouteNode::new()
                .with_handler_async("GET", route_unstable_posts_get)
                .with_handler_async("DELETE", route_unstable_posts_delete)
                .with_child(
                    "replies",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_posts_replies_list)
                        .with_handler_async("POST", route_unstable_posts_replies_create),
                )
                .with_child(
                    "votes",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_posts_likes_list),
                )
                .with_child(
                    "your_vote",
                    crate::RouteNode::new()
                        .with_handler_async("PUT", route_unstable_posts_like)
                        .with_handler_async("DELETE", route_unstable_posts_unlike),
                ),
        )
}
