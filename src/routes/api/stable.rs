use crate::{CommentLocalID, CommunityLocalID, PostLocalID, UserLocalID};
use futures::TryStreamExt;
use std::borrow::Cow;
use std::sync::Arc;

async fn route_stable_comments_attachments_0_href_get(
    params: (CommentLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt(
            "SELECT attachment_href FROM reply WHERE id=$1",
            &[&comment_id],
        )
        .await?;
    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr("no_such_comment", None).into_owned(),
        )),
        Some(row) => {
            let href: Option<String> = row.get(0);
            match href {
                None => Ok(crate::simple_response(
                    hyper::StatusCode::NOT_FOUND,
                    lang.tr("no_such_attachment", None).into_owned(),
                )),
                Some(href) => {
                    if href.starts_with("local-media://") {
                        // local media, serve file content

                        let media_id: crate::Pineapple = (&href[14..]).parse()?;

                        let media_row = db
                            .query_opt(
                                "SELECT path, mime FROM media WHERE id=$1",
                                &[&media_id.as_int()],
                            )
                            .await?;
                        match media_row {
                            None => Ok(crate::simple_response(
                                hyper::StatusCode::NOT_FOUND,
                                lang.tr("media_upload_missing", None).into_owned(),
                            )),
                            Some(media_row) => {
                                let path: &str = media_row.get(0);
                                let mime: &str = media_row.get(1);

                                if let Some(media_location) = &ctx.media_location {
                                    let path = media_location.join(path);

                                    let file = tokio::fs::File::open(path).await?;
                                    let body = hyper::Body::wrap_stream(
                                        tokio_util::codec::FramedRead::new(
                                            file,
                                            tokio_util::codec::BytesCodec::new(),
                                        ),
                                    );

                                    Ok(crate::common_response_builder()
                                        .header(hyper::header::CONTENT_TYPE, mime)
                                        .body(body)?)
                                } else {
                                    Ok(crate::simple_response(
                                        hyper::StatusCode::NOT_FOUND,
                                        lang.tr("media_upload_missing", None).into_owned(),
                                    ))
                                }
                            }
                        }
                    } else {
                        Ok(crate::common_response_builder()
                            .status(hyper::StatusCode::FOUND)
                            .header(hyper::header::LOCATION, &href)
                            .body(href.into())?)
                    }
                }
            }
        }
    }
}

async fn route_stable_communities_feed_get(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let community_row = db
        .query_opt(
            "SELECT name, local FROM community WHERE id=$1",
            &[&community_id],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr("no_such_community", None).into_owned(),
            ))
        })?;

    let community_local: bool = community_row.get(1);
    if !community_local {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr("community_not_local", None).into_owned(),
        )));
    }

    let community_name: String = community_row.get(0);

    let mut builder = atom_syndication::FeedBuilder::default();
    builder.title(community_name);
    builder.id(format!(
        "{}/stable/communities/{}/feed",
        ctx.host_url_api, community_id
    ));
    builder.generator(atom_syndication::Generator {
        value: "lotide".to_owned(),
        uri: Some("https://sr.ht/~vpzom/lotide".to_owned()),
        version: Some(env!("CARGO_PKG_VERSION").to_owned()),
    });

    let limit: i64 = 30;

    let values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&community_id, &limit];
    let sql: &str = &format!(
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, post.content_html, post.ap_id, post.local, person.username, person.local, person.ap_id FROM post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = $1 AND post.approved=TRUE AND post.deleted=FALSE ORDER BY {} LIMIT $2",
        super::SortType::New.post_sort_sql(),
    );

    let stream = crate::query_stream(&db, sql, &values).await?;
    let mut first = true;

    stream
        .map_err(crate::Error::from)
        .try_for_each(|row| {
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(5);
            if first {
                first = false;
                builder.updated(created.clone());
            }

            let post_id = PostLocalID(row.get(0));
            let title: String = row.get(4);

            let href_raw: Option<&str> = row.get(2);
            let href = ctx.process_href_opt(href_raw.map(Cow::Borrowed), post_id);

            let post_ap_id = if row.get(8) {
                crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub).to_string()
            } else {
                row.get(7)
            };

            let author_username = row.get(9);
            let author_ap_id = if row.get(10) {
                Some(
                    crate::apub_util::get_local_person_apub_id(
                        UserLocalID(row.get(1)),
                        &ctx.host_url_apub,
                    )
                    .to_string(),
                )
            } else {
                row.get(11)
            };

            let mut entry_builder = atom_syndication::EntryBuilder::default();
            entry_builder.title(title);
            entry_builder.id(post_ap_id.clone());
            entry_builder.updated(created.clone());
            entry_builder.author(atom_syndication::Person {
                name: author_username,
                email: None,
                uri: author_ap_id,
            });
            entry_builder.published(created);
            entry_builder.link(
                atom_syndication::LinkBuilder::default()
                    .href(post_ap_id)
                    .rel("self")
                    .build(),
            );
            entry_builder.content(atom_syndication::Content {
                content_type: Some("html".to_owned()),
                value: {
                    let content_html = row.get::<_, Option<&str>>(6).map(Cow::Borrowed);
                    let content = content_html.or_else(|| {
                        let content_text: Option<&str> = row.get(3);
                        content_text
                            .map(|content_text| Cow::Owned(ammonia::clean_text(&content_text)))
                    });

                    let link_content = match href {
                        None => None,
                        Some(href) => Some(format!(
                            r#"<p><a href="{0}">{0}</a></p>"#,
                            ammonia::clean_text(&href)
                        )),
                    };

                    match (content, link_content) {
                        (None, None) => None,
                        (Some(content), None) => Some(content.into_owned()),
                        (None, Some(link_content)) => Some(link_content),
                        (Some(content), Some(link_content)) => {
                            Some(format!("{}{}", content, link_content))
                        }
                    }
                },
                ..Default::default()
            });

            let entry = entry_builder.build();
            builder.entry(entry);

            futures::future::ok(())
        })
        .await?;

    let feed = builder.build();

    let mut output = Vec::new();
    feed.write_to(&mut output)?;

    Ok(crate::common_response_builder()
        .header(hyper::header::CONTENT_TYPE, "application/atom+xml")
        .body(hyper::Body::from(output))?)
}

async fn route_stable_posts_href_get(
    params: (PostLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt("SELECT href FROM post WHERE id=$1", &[&post_id])
        .await?;
    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr("no_such_post", None).into_owned(),
        )),
        Some(row) => {
            let href: Option<String> = row.get(0);
            match href {
                None => Ok(crate::simple_response(
                    hyper::StatusCode::NOT_FOUND,
                    lang.tr("post_not_link", None).into_owned(),
                )),
                Some(href) => {
                    if href.starts_with("local-media://") {
                        // local media, serve file content

                        let media_id: crate::Pineapple = (&href[14..]).parse()?;

                        let media_row = db
                            .query_opt(
                                "SELECT path, mime FROM media WHERE id=$1",
                                &[&media_id.as_int()],
                            )
                            .await?;
                        match media_row {
                            None => Ok(crate::simple_response(
                                hyper::StatusCode::NOT_FOUND,
                                lang.tr("media_upload_missing", None).into_owned(),
                            )),
                            Some(media_row) => {
                                let path: &str = media_row.get(0);
                                let mime: &str = media_row.get(1);

                                if let Some(media_location) = &ctx.media_location {
                                    let path = media_location.join(path);

                                    let file = tokio::fs::File::open(path).await?;
                                    let body = hyper::Body::wrap_stream(
                                        tokio_util::codec::FramedRead::new(
                                            file,
                                            tokio_util::codec::BytesCodec::new(),
                                        ),
                                    );

                                    Ok(crate::common_response_builder()
                                        .header(hyper::header::CONTENT_TYPE, mime)
                                        .body(body)?)
                                } else {
                                    Ok(crate::simple_response(
                                        hyper::StatusCode::NOT_FOUND,
                                        lang.tr("media_upload_missing", None).into_owned(),
                                    ))
                                }
                            }
                        }
                    } else {
                        Ok(crate::common_response_builder()
                            .status(hyper::StatusCode::FOUND)
                            .header(hyper::header::LOCATION, &href)
                            .body(href.into())?)
                    }
                }
            }
        }
    }
}

async fn route_unstable_users_avatar_href_get(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt("SELECT avatar FROM person WHERE id=$1", &[&user_id])
        .await?;
    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr("no_such_user", None).into_owned(),
        )),
        Some(row) => {
            let href: Option<String> = row.get(0);
            match href {
                None => Ok(crate::simple_response(
                    hyper::StatusCode::NOT_FOUND,
                    lang.tr("user_no_avatar", None).into_owned(),
                )),
                Some(href) => {
                    if href.starts_with("local-media://") {
                        // local media, serve file content

                        let media_id: crate::Pineapple = (&href[14..]).parse()?;

                        let media_row = db
                            .query_opt(
                                "SELECT path, mime FROM media WHERE id=$1",
                                &[&media_id.as_int()],
                            )
                            .await?;
                        match media_row {
                            None => Ok(crate::simple_response(
                                hyper::StatusCode::NOT_FOUND,
                                lang.tr("media_upload_missing", None).into_owned(),
                            )),
                            Some(media_row) => {
                                let path: &str = media_row.get(0);
                                let mime: &str = media_row.get(1);

                                if let Some(media_location) = &ctx.media_location {
                                    let path = media_location.join(path);

                                    let file = tokio::fs::File::open(path).await?;
                                    let body = hyper::Body::wrap_stream(
                                        tokio_util::codec::FramedRead::new(
                                            file,
                                            tokio_util::codec::BytesCodec::new(),
                                        ),
                                    );

                                    Ok(crate::common_response_builder()
                                        .header(hyper::header::CONTENT_TYPE, mime)
                                        .body(body)?)
                                } else {
                                    Ok(crate::simple_response(
                                        hyper::StatusCode::NOT_FOUND,
                                        lang.tr("media_upload_missing", None).into_owned(),
                                    ))
                                }
                            }
                        }
                    } else {
                        Ok(crate::common_response_builder()
                            .status(hyper::StatusCode::FOUND)
                            .header(hyper::header::LOCATION, &href)
                            .body(href.into())?)
                    }
                }
            }
        }
    }
}

pub fn route_stable() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child(
            "comments",
            crate::RouteNode::new().with_child_parse::<CommentLocalID, _>(
                crate::RouteNode::new().with_child(
                    "attachments/0/href",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_stable_comments_attachments_0_href_get),
                ),
            ),
        )
        .with_child(
            "communities",
            crate::RouteNode::new().with_child_parse::<CommunityLocalID, _>(
                crate::RouteNode::new().with_child(
                    "feed",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_stable_communities_feed_get),
                ),
            ),
        )
        .with_child(
            "posts",
            crate::RouteNode::new().with_child_parse::<PostLocalID, _>(
                crate::RouteNode::new().with_child(
                    "href",
                    crate::RouteNode::new().with_handler_async("GET", route_stable_posts_href_get),
                ),
            ),
        )
        .with_child(
            "users",
            crate::RouteNode::new().with_child_parse::<UserLocalID, _>(
                crate::RouteNode::new().with_child(
                    "avatar/href",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_users_avatar_href_get),
                ),
            ),
        )
}
