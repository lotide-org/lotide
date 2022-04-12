use crate::lang;
use crate::types::{
    InvitationsListQuery, RespAvatarInfo, RespInvitationInfo, RespList, RespMinimalAuthorInfo,
    UserLocalID,
};
use std::borrow::Cow;
use std::sync::Arc;

async fn route_unstable_invitations_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let query: InvitationsListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    if let Some(key_str) = query.key {
        match key_str.parse::<crate::Pineapple>() {
            Ok(key) => {
                let row = db.query_opt("SELECT invitation.id, invitation.key, invitation.created_at, invitation.used_by, person.id, person.username, person.local, person.ap_id, person.is_bot, person.avatar FROM invitation INNER JOIN person ON (person.id = invitation.created_by) WHERE invitation.key=$1", &[&key.as_int()]).await?;

                if let Some(row) = row {
                    let created_at: chrono::DateTime<chrono::FixedOffset> = row.get(2);
                    let user_id = UserLocalID(row.get(4));
                    let user_local = row.get(6);
                    let user_ap_id: Option<&str> = row.get(7);
                    let user_avatar: Option<&str> = row.get(9);

                    crate::json_response(&RespList {
                        items: Cow::Owned(vec![RespInvitationInfo {
                            id: row.get(0),
                            key: key_str,
                            created_at: created_at.to_rfc3339(),
                            used: row.get::<_, Option<i64>>(3).is_some(),
                            created_by: RespMinimalAuthorInfo {
                                host: crate::get_actor_host_or_unknown(
                                    user_local,
                                    user_ap_id,
                                    &ctx.local_hostname,
                                ),
                                username: Cow::Borrowed(row.get(5)),
                                is_bot: row.get(8),
                                avatar: user_avatar.map(|url| RespAvatarInfo {
                                    url: ctx.process_avatar_href(url, user_id),
                                }),
                                id: user_id,
                                local: user_local,
                                remote_url: if user_local {
                                    Some(Cow::Owned(String::from(
                                        crate::apub_util::LocalObjectRef::User(user_id)
                                            .to_local_uri(&ctx.host_url_apub),
                                    )))
                                } else {
                                    user_ap_id.map(Cow::Borrowed)
                                },
                            },
                        }]),
                        next_page: None,
                    })
                } else {
                    crate::json_response(&RespList::<()> {
                        items: Cow::Borrowed(&[]),
                        next_page: None,
                    })
                }
            }
            Err(_) => crate::json_response(&RespList::<()> {
                items: Cow::Borrowed(&[]),
                next_page: None,
            }),
        }
    } else {
        Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "A query is required",
        )))
    }
}

async fn route_unstable_invitations_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    {
        let row = db
            .query_one(
                "SELECT users_create_invitations, allow_invitations FROM site WHERE local",
                &[],
            )
            .await?;

        if row.get(1) {
            if row.get(0) {
                Ok(())
            } else {
                if crate::is_site_admin(&db, user).await? {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        lang.tr(&lang::invitations_not_allowed()).into_owned(),
                    )))
                }
            }
        } else {
            Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::invitations_disabled()).into_owned(),
            )))
        }
    }?;

    let key = crate::Pineapple::generate();

    let row = db.query_one("INSERT INTO invitation (key, created_by, created_at) VALUES ($1, $2, current_timestamp) RETURNING id", &[&key.as_int(), &user]).await?;

    crate::json_response(&serde_json::json!({"key": key.to_string(), "id": row.get::<_, i32>(0)}))
}

pub fn route_invitations() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_handler_async(hyper::Method::GET, route_unstable_invitations_list)
        .with_handler_async(hyper::Method::POST, route_unstable_invitations_create)
}
