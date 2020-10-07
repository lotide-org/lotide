use crate::UserLocalID;
use lettre::Tokio02Transport;
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::sync::Arc;

type ForgotPasswordKey = crate::Pineapple;

async fn route_unstable_forgot_password_keys_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);

    if ctx.mailer.is_none() {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::INTERNAL_SERVER_ERROR,
            lang.tr("email_not_configured", None).into_owned(),
        )));
    }

    #[derive(Deserialize)]
    struct ForgotPasswordBody<'a> {
        email_address: Cow<'a, str>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: ForgotPasswordBody = serde_json::from_slice(&body)?;

    let db = ctx.db_pool.get().await?;

    let user_row = db.query_opt("SELECT id, username, email_address FROM person WHERE local AND LOWER(email_address) = LOWER($1)", &[&body.email_address]).await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr("no_such_local_user_by_email", None).into_owned(),
            ))
        })?;

    let user_id = UserLocalID(user_row.get(0));
    let username: &str = user_row.get(1);
    let user_email: &str = user_row.get(2);

    let user_email = lettre::Mailbox::new(None, user_email.parse()?);

    let key = ForgotPasswordKey::generate();
    db.execute(
        "INSERT INTO forgot_password_key (key, person, created) VALUES ($1, $2, current_timestamp)",
        &[&key.as_int(), &user_id],
    )
    .await?;

    let msg_body = lang
        .tr(
            "email_content_forgot_password",
            Some(&fluent::fluent_args!["key" => key.to_string(), "username" => username]),
        )
        .into_owned();

    let msg = lettre::Message::builder()
        .date_now()
        .subject("Forgot Password Request")
        .from(ctx.mail_from.as_ref().unwrap().clone())
        .to(user_email)
        .singlepart(
            lettre::message::SinglePart::binary()
                .header(lettre::message::header::ContentType::text_utf8())
                .body(msg_body),
        )?;

    crate::spawn_task(async move {
        ctx.mailer.as_ref().unwrap().send(msg).await?;

        Ok(())
    });

    Ok(crate::common_response_builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body("{}".into())?)
}

async fn route_unstable_forgot_password_keys_get(
    params: (ForgotPasswordKey,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (key,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let row = db.query_opt("SELECT created < (current_timestamp - INTERVAL '1 HOUR') FROM forgot_password_key WHERE key=$1", &[&key.as_int()]).await?;

    let found = match row {
        None => false,
        Some(row) => !row.get::<_, bool>(0),
    };

    if found {
        Ok(crate::common_response_builder()
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body("{}".into())?)
    } else {
        Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr("no_such_forgot_password_key", None).into_owned(),
        )))
    }
}

async fn route_unstable_forgot_password_keys_reset(
    params: (ForgotPasswordKey,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (key,) = params;

    #[derive(Deserialize)]
    struct PasswordResetBody {
        new_password: String,
    }

    let lang = crate::get_lang_for_req(&req);

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: PasswordResetBody = serde_json::from_slice(&body)?;

    let mut db = ctx.db_pool.get().await?;

    let row = db.query_opt("SELECT created < (current_timestamp - INTERVAL '1 HOUR'), person FROM forgot_password_key WHERE key=$1", &[&key.as_int()]).await?;

    let user_id = match row {
        None => None,
        Some(row) => {
            if row.get(0) {
                None
            } else {
                Some(UserLocalID(row.get(1)))
            }
        }
    };

    match user_id {
        Some(user_id) => {
            let passhash = tokio::task::spawn_blocking(move || {
                bcrypt::hash(body.new_password, bcrypt::DEFAULT_COST)
            })
            .await??;

            {
                let trans = db.transaction().await?;
                trans
                    .execute(
                        "UPDATE person SET passhash=$1 WHERE id=$2",
                        &[&passhash, &user_id],
                    )
                    .await?;
                trans
                    .execute(
                        "DELETE FROM forgot_password_key WHERE key=$1",
                        &[&key.as_int()],
                    )
                    .await?;

                trans.commit().await?;
            }

            Ok(crate::common_response_builder()
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body("{}".into())?)
        }
        None => Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr("no_such_forgot_password_key", None).into_owned(),
        ))),
    }
}

pub fn route_forgot_password() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child(
        "keys",
        crate::RouteNode::new()
            .with_handler_async("POST", route_unstable_forgot_password_keys_create)
            .with_child_parse::<ForgotPasswordKey, _>(
                crate::RouteNode::new()
                    .with_handler_async("GET", route_unstable_forgot_password_keys_get)
                    .with_child(
                        "reset",
                        crate::RouteNode::new()
                            .with_handler_async("POST", route_unstable_forgot_password_keys_reset),
                    ),
            ),
    )
}
