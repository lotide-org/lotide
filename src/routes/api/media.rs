use crate::lang;
use futures::TryStreamExt;
use std::sync::Arc;

async fn route_unstable_media_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);

    let content_type = req
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr(&lang::missing_content_type()).into_owned(),
            ))
        })?;
    let content_type = std::str::from_utf8(content_type.as_ref())?;
    let content_type: mime::Mime = content_type.parse()?;

    if content_type.type_() != mime::IMAGE {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr(&lang::media_upload_not_image()).into_owned(),
        )));
    }

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    if let Some(media_storage) = &ctx.media_storage {
        let path = media_storage
            .save(
                req.into_body()
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)),
                content_type.as_ref(),
            )
            .await?;

        let id = crate::Pineapple::generate();

        db.execute(
            "INSERT INTO media (id, path, person, mime, created) VALUES ($1, $2, $3, $4, current_timestamp)",
            &[&id.as_int(), &path, &user, &content_type.as_ref()],
        )
        .await?;

        crate::json_response(&serde_json::json!({"id": id.to_string()}))
    } else {
        Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::INTERNAL_SERVER_ERROR,
            lang.tr(&lang::media_upload_not_configured()).into_owned(),
        )))
    }
}

pub fn route_media() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_handler_async(hyper::Method::POST, route_unstable_media_create)
}
