use crate::{CommentLocalID, PostLocalID, UserLocalID};
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
