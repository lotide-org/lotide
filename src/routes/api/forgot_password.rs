use crate::UserLocalID;
use lettre::Tokio02Transport;
use rand::Rng;
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::sync::Arc;

struct ForgotPasswordKey {
    value: i32,
}

impl ForgotPasswordKey {
    pub fn generate() -> Self {
        Self {
            value: rand::thread_rng().gen(),
        }
    }

    pub fn as_int(&self) -> i32 {
        self.value
    }

    pub fn to_str(&self) -> String {
        bs58::encode(&self.value.to_be_bytes()).into_string()
    }
}

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
            Some(&fluent::fluent_args!["key" => key.to_str(), "username" => username]),
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

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body("{}".into())?)
}

pub fn route_forgot_password() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child(
        "keys",
        crate::RouteNode::new()
            .with_handler_async("POST", route_unstable_forgot_password_keys_create),
    )
}
