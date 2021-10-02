use super::InvalidPage;
use crate::types::{
    CommentLocalID, CommunityLocalID, JustContentText, MaybeIncludeYour, PostLocalID,
    RespAvatarInfo, RespList, RespLoginUserInfo, RespMinimalAuthorInfo, RespMinimalCommentInfo,
    RespMinimalCommunityInfo, RespMinimalPostInfo, RespNotification, RespNotificationInfo,
    RespPostListPost, RespThingInfo, RespUserInfo, UserLocalID,
};
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::sync::Arc;

struct MeOrLocalAndAdminResult {
    pub login_user: UserLocalID,
    pub target_user: UserLocalID,
    is_admin: Option<bool>,
}

impl MeOrLocalAndAdminResult {
    pub async fn require_admin(
        &self,
        db: &tokio_postgres::Client,
        lang: &crate::Translator,
    ) -> Result<(), crate::Error> {
        let is_admin = match self.is_admin {
            Some(value) => value,
            None => crate::is_site_admin(db, self.login_user).await?,
        };

        if is_admin {
            Ok(())
        } else {
            Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr("not_admin", None).into_owned(),
            )))
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum UserIDOrMe {
    User(UserLocalID),
    Me,
}

impl UserIDOrMe {
    pub fn resolve(self, me: UserLocalID) -> UserLocalID {
        match self {
            UserIDOrMe::User(id) => id,
            UserIDOrMe::Me => me,
        }
    }

    pub async fn try_resolve(
        self,
        req: &hyper::Request<hyper::Body>,
        db: &tokio_postgres::Client,
    ) -> Result<UserLocalID, crate::Error> {
        match self {
            UserIDOrMe::User(id) => Ok(id),
            UserIDOrMe::Me => crate::require_login(req, db).await,
        }
    }

    pub async fn require_me(
        self,
        req: &hyper::Request<hyper::Body>,
        db: &tokio_postgres::Client,
    ) -> Result<UserLocalID, crate::Error> {
        let login_user = crate::require_login(req, db).await?;
        match self {
            UserIDOrMe::Me => Ok(login_user),
            UserIDOrMe::User(id) => {
                if id == login_user {
                    Ok(login_user)
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        "This endpoint is only available for the current user",
                    )))
                }
            }
        }
    }

    pub async fn require_me_or_local_and_admin(
        self,
        req: &hyper::Request<hyper::Body>,
        db: &tokio_postgres::Client,
    ) -> Result<MeOrLocalAndAdminResult, crate::Error> {
        let login_user = crate::require_login(req, db).await?;
        match self {
            UserIDOrMe::Me => Ok(MeOrLocalAndAdminResult {
                login_user,
                target_user: login_user,
                is_admin: None,
            }),
            UserIDOrMe::User(target_user) => {
                if target_user == login_user {
                    Ok(MeOrLocalAndAdminResult {
                        login_user,
                        target_user,
                        is_admin: None,
                    })
                } else if crate::is_site_admin(db, login_user).await?
                    && crate::is_local_user(db, target_user).await?
                {
                    Ok(MeOrLocalAndAdminResult {
                        login_user,
                        target_user,
                        is_admin: Some(true),
                    })
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        "You do not have permission to do that",
                    )))
                }
            }
        }
    }
}

impl std::str::FromStr for UserIDOrMe {
    type Err = std::num::ParseIntError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        if src == "~me" {
            Ok(UserIDOrMe::Me)
        } else {
            src.parse().map(UserIDOrMe::User)
        }
    }
}

async fn route_unstable_users_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    #[derive(Deserialize)]
    struct UsersListQuery<'a> {
        local: Option<bool>,
        username: Option<Cow<'a, str>>,
    }

    let query: UsersListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let username = match (query.local, query.username) {
        (Some(true), Some(username)) => username,
        _ => {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                "User listing is only allowed when filtering by local=true and a username",
            )))
        }
    };

    let db = ctx.db_pool.get().await?;

    let rows = db.query(
        "SELECT id, description, description_html, avatar, suspended, is_bot FROM person WHERE local AND username=$1",
        &[&username]
    )
        .await?;

    let output = RespList {
        next_page: None,
        items: Cow::Owned(
            rows.iter()
                .map(|row| {
                    let avatar: Option<&str> = row.get(3);

                    let user_id = UserLocalID(row.get(0));

                    let info = RespMinimalAuthorInfo {
                        id: user_id,
                        local: true,
                        username: Cow::Borrowed(&username),
                        host: Cow::Borrowed(&ctx.local_hostname),
                        remote_url: Some(
                            String::from(crate::apub_util::get_local_person_apub_id(
                                user_id,
                                &ctx.host_url_apub,
                            ))
                            .into(),
                        ),
                        is_bot: row.get(5),
                        avatar: avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, user_id),
                        }),
                    };

                    let (description, description_text, description_html) = match row.get(2) {
                        Some(description_html) => (
                            description_html,
                            None,
                            Some(crate::clean_html(description_html)),
                        ),
                        None => {
                            let description_text: &str = row.get(1);
                            (description_text, Some(description_text), None)
                        }
                    };

                    RespUserInfo {
                        base: info,
                        description,
                        description_html,
                        description_text,
                        suspended: Some(row.get(4)),
                        your_note: None,
                    }
                })
                .collect::<Vec<_>>(),
        ),
    };

    crate::json_response(&output)
}

async fn route_unstable_users_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);
    let mut db = ctx.db_pool.get().await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct UsersCreateBody<'a> {
        username: Cow<'a, str>,
        password: String,
        email_address: Option<Cow<'a, str>>,

        #[serde(default)]
        login: bool,
    }

    let body: UsersCreateBody<'_> = serde_json::from_slice(&body)?;

    for ch in body.username.chars() {
        if !super::USERNAME_ALLOWED_CHARS.contains(&ch) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr("user_name_disallowed_chars", None).into_owned(),
            )));
        }
    }

    if let Some(email) = &body.email_address {
        if !fast_chemail::is_valid_email(email) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr("user_email_invalid", None).into_owned(),
            )));
        }
    }

    let req_password = body.password;
    let passhash =
        tokio::task::spawn_blocking(move || bcrypt::hash(req_password, bcrypt::DEFAULT_COST))
            .await??;

    let user_id = {
        let trans = db.transaction().await?;
        trans
            .execute(
                "INSERT INTO local_actor_name (name) VALUES ($1)",
                &[&body.username],
            )
            .await
            .map_err(|err| {
                if err.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                    crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr("name_in_use", None).into_owned(),
                    ))
                } else {
                    err.into()
                }
            })?;
        let row = trans.query_one(
            "INSERT INTO person (username, local, created_local, passhash, email_address) VALUES ($1, TRUE, current_timestamp, $2, $3) RETURNING id",
            &[&body.username, &passhash, &body.email_address],
        ).await?;

        trans.commit().await?;

        UserLocalID(row.get(0))
    };

    let info = RespLoginUserInfo {
        id: user_id,
        username: &body.username,
        is_site_admin: false,
        has_unread_notifications: false,
    };

    let output = if body.login {
        let token = super::insert_token(user_id, &db).await?;
        serde_json::json!({"user": info, "token": token.to_string()})
    } else {
        serde_json::json!({ "user": info })
    };

    crate::json_response(&output)
}

async fn route_unstable_users_patch(
    params: (UserIDOrMe,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);
    let mut db = ctx.db_pool.get().await?;

    let me_or_admin = params.0.require_me_or_local_and_admin(&req, &db).await?;
    let user_id = me_or_admin.target_user;

    #[derive(Deserialize)]
    struct UsersEditBody<'a> {
        #[serde(alias = "description")]
        description_text: Option<Cow<'a, str>>,
        email_address: Option<Cow<'a, str>>,
        password: Option<String>,
        avatar: Option<Cow<'a, str>>,
        suspended: Option<bool>,
        is_bot: Option<bool>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: UsersEditBody = serde_json::from_slice(&body)?;

    let arena = bumpalo::Bump::new();

    let mut changes = Vec::<(&str, &(dyn tokio_postgres::types::ToSql + Sync))>::new();

    if let Some(description) = body.description_text.as_ref() {
        changes.push(("description", description));
    }
    if let Some(email_address) = body.email_address.as_ref() {
        if !fast_chemail::is_valid_email(email_address) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr("user_email_invalid", None).into_owned(),
            )));
        }

        changes.push(("email_address", email_address));
    }
    if let Some(password) = body.password {
        let passhash =
            tokio::task::spawn_blocking(move || bcrypt::hash(password, bcrypt::DEFAULT_COST))
                .await??;

        changes.push(("passhash", arena.alloc(passhash)));
    }
    if let Some(avatar) = &body.avatar {
        if !avatar.starts_with("local-media://") {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Avatar must be local media",
            )));
        }

        changes.push(("avatar", avatar));
    }
    if let Some(suspended) = &body.suspended {
        me_or_admin.require_admin(&db, &lang).await?;

        changes.push(("suspended", suspended));
    }
    if let Some(is_bot) = &body.is_bot {
        changes.push(("is_bot", is_bot));
    }

    if !changes.is_empty() {
        use std::fmt::Write;

        let mut sql = "UPDATE person SET ".to_owned();
        let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = changes
            .iter()
            .enumerate()
            .map(|(idx, (key, value))| {
                write!(
                    sql,
                    "{}{}=${}",
                    if idx == 0 { "" } else { "," },
                    key,
                    idx + 1
                )
                .unwrap();

                *value
            })
            .collect();
        values.push(&user_id);
        write!(sql, " WHERE id=${}", values.len()).unwrap();

        let sql: &str = &sql;

        let trans = db.transaction().await?;
        trans.execute(sql, &values).await?;
        if body.suspended == Some(true) {
            // just suspended, need to clear out current logins

            trans
                .execute("DELETE FROM login WHERE person=$1", &[&user_id])
                .await?;
        }

        trans.commit().await?;
    }

    Ok(crate::empty_response())
}

async fn route_unstable_users_notifications_list(
    params: (UserIDOrMe,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user,) = params;

    let mut db = ctx.db_pool.get().await?;

    let user = user.require_me(&req, &db).await?;

    let limit: i64 = 30;

    let rows = {
        let trans = db.transaction().await?;

        let rows = trans.query(
            "SELECT notification.kind, (notification.created_at > (SELECT last_checked_notifications FROM person WHERE id=$1)), reply.id, reply.content_text, reply.content_html, parent_reply.id, parent_reply_post.id, parent_reply_post.title, parent_post.id, parent_post.title, parent_post.ap_id, parent_post.local, parent_reply_post.ap_id, parent_reply_post.local, reply.ap_id, reply.local FROM notification LEFT OUTER JOIN reply ON (reply.id = notification.reply) LEFT OUTER JOIN reply AS parent_reply ON (parent_reply.id = notification.parent_reply) LEFT OUTER JOIN post AS parent_reply_post ON (parent_reply_post.id = parent_reply.post) LEFT OUTER JOIN post AS parent_post ON (parent_post.id = notification.parent_post) WHERE notification.to_user = $1 AND NOT COALESCE(reply.deleted OR parent_reply.deleted OR parent_reply_post.deleted OR parent_post.deleted, FALSE) ORDER BY created_at DESC LIMIT $2",
            &[&user, &limit],
        ).await?;
        trans
            .execute(
                "UPDATE person SET last_checked_notifications=current_timestamp WHERE id=$1",
                &[&user],
            )
            .await?;

        trans.commit().await?;

        rows
    };

    let notifications: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            let kind: &str = row.get(0);
            let unseen: bool = row.get(1);
            let info = match kind {
                "post_reply" => {
                    if let Some(reply_id) = row.get(2) {
                        if let Some(post_id) = row.get(8) {
                            let reply_id = CommentLocalID(reply_id);

                            let reply_ap_id: Option<&str> = row.get(14);
                            let reply_local: bool = row.get(15);

                            let reply_remote_url = if reply_local {
                                Some(Cow::Owned(String::from(
                                    crate::apub_util::get_local_comment_apub_id(
                                        reply_id,
                                        &ctx.host_url_apub,
                                    ),
                                )))
                            } else {
                                reply_ap_id.map(Cow::Borrowed)
                            };

                            let post_id = PostLocalID(post_id);

                            let comment = RespMinimalCommentInfo {
                                id: reply_id,
                                remote_url: reply_remote_url,
                                content_text: row.get::<_, Option<_>>(3).map(Cow::Borrowed),
                                content_html_safe: row
                                    .get::<_, Option<&str>>(4)
                                    .map(|html| crate::clean_html(html)),
                            };

                            let post_ap_id: Option<&str> = row.get(10);
                            let post_local: bool = row.get(11);

                            let post_remote_url = if post_local {
                                Some(Cow::Owned(String::from(
                                    crate::apub_util::get_local_post_apub_id(
                                        post_id,
                                        &ctx.host_url_apub,
                                    ),
                                )))
                            } else {
                                post_ap_id.map(Cow::Borrowed)
                            };

                            let post = RespMinimalPostInfo {
                                id: post_id,
                                title: row.get(9),
                                remote_url: post_remote_url,
                            };

                            Some(RespNotificationInfo::PostReply {
                                reply: comment,
                                post,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                "reply_reply" => {
                    if let Some(reply_id) = row.get(2) {
                        if let Some(parent_id) = row.get(5) {
                            let reply_id = CommentLocalID(reply_id);

                            let reply_ap_id: Option<&str> = row.get(14);
                            let reply_local: bool = row.get(15);

                            let reply_remote_url = if reply_local {
                                Some(Cow::Owned(String::from(
                                    crate::apub_util::get_local_comment_apub_id(
                                        reply_id,
                                        &ctx.host_url_apub,
                                    ),
                                )))
                            } else {
                                reply_ap_id.map(Cow::Borrowed)
                            };

                            let reply = RespMinimalCommentInfo {
                                id: reply_id,
                                remote_url: reply_remote_url,
                                content_text: row.get::<_, Option<_>>(3).map(Cow::Borrowed),
                                content_html_safe: row
                                    .get::<_, Option<&str>>(4)
                                    .map(|html| crate::clean_html(html)),
                            };
                            let parent_id = CommentLocalID(parent_id);

                            let post = row.get::<_, Option<_>>(6).map(|post_id| {
                                let post_id = PostLocalID(post_id);
                                let post_ap_id: Option<&str> = row.get(12);
                                let post_local: bool = row.get(13);

                                let post_remote_url = if post_local {
                                    Some(Cow::Owned(String::from(
                                        crate::apub_util::get_local_post_apub_id(
                                            post_id,
                                            &ctx.host_url_apub,
                                        ),
                                    )))
                                } else {
                                    post_ap_id.map(Cow::Borrowed)
                                };

                                RespMinimalPostInfo {
                                    id: post_id,
                                    title: row.get(7),
                                    remote_url: post_remote_url,
                                }
                            });

                            Some(RespNotificationInfo::CommentReply {
                                reply,
                                comment: parent_id,
                                post,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            };

            info.map(|info| RespNotification { info, unseen })
        })
        .collect();

    crate::json_response(&notifications)
}

async fn route_unstable_users_get(
    params: (UserIDOrMe,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;

    let query: MaybeIncludeYour = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let your_note_row;

    let (user_id, your_note) = if query.include_your {
        let user = crate::require_login(&req, &db).await?;

        let user_id = user_id.resolve(user);

        (
            user_id,
            Some({
                your_note_row = db
                    .query_opt(
                        "SELECT content_text FROM person_note WHERE author=$1 AND target=$2",
                        &[&user, &user_id],
                    )
                    .await?;

                your_note_row.as_ref().map(|row| JustContentText {
                    content_text: Cow::Borrowed(row.get(0)),
                })
            }),
        )
    } else {
        let user_id = user_id.try_resolve(&req, &db).await?;
        (user_id, None)
    };

    let row = db
        .query_opt(
            "SELECT username, local, ap_id, description, description_html, avatar, suspended, is_bot FROM person WHERE id=$1",
            &[&user_id],
        )
        .await?;

    let row = row.ok_or_else(|| {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr("no_such_user", None).into_owned(),
        ))
    })?;

    let local = row.get(1);
    let ap_id: Option<_> = row.get(2);

    let remote_url = if local {
        Some(Cow::Owned(String::from(
            crate::apub_util::get_local_person_apub_id(user_id, &ctx.host_url_apub),
        )))
    } else {
        ap_id.map(Cow::Borrowed)
    };
    let avatar: Option<&str> = row.get(5);

    let info = RespMinimalAuthorInfo {
        id: user_id,
        local,
        username: Cow::Borrowed(row.get(0)),
        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
        remote_url,
        is_bot: row.get(7),
        avatar: avatar.map(|url| RespAvatarInfo {
            url: ctx.process_avatar_href(url, user_id),
        }),
    };

    let (description, description_text, description_html) = match row.get(4) {
        Some(description_html) => (
            description_html,
            None,
            Some(crate::clean_html(description_html)),
        ),
        None => {
            let description_text: &str = row.get(3);
            (description_text, Some(description_text), None)
        }
    };

    let info = RespUserInfo {
        base: info,
        description,
        description_html,
        description_text,
        suspended: if local { Some(row.get(6)) } else { None },
        your_note,
    };

    crate::json_response(&info)
}

async fn route_unstable_users_your_note_put(
    params: (UserIDOrMe,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (target_user,) = params;

    let db = ctx.db_pool.get().await?;
    let login_user = crate::require_login(&req, &db).await?;

    let target_user = target_user.resolve(login_user);

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: JustContentText = serde_json::from_slice(&body)?;

    db.execute(
        "INSERT INTO person_note (author, target, content_text) VALUES ($1, $2, $3) ON CONFLICT (author, target) DO UPDATE SET content_text=$3",
        &[&login_user, &target_user, &body.content_text],
    ).await?;

    Ok(crate::empty_response())
}

async fn route_unstable_users_things_list(
    params: (UserIDOrMe,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;

    let db = ctx.db_pool.get().await?;

    let user_id = user_id.try_resolve(&req, &db).await?;

    fn default_limit() -> u8 {
        30
    }

    #[derive(Deserialize)]
    struct UserThingsListQuery<'a> {
        #[serde(default = "default_limit")]
        limit: u8,

        page: Option<Cow<'a, str>>,
    }
    let query: UserThingsListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let limit_plus_1: i64 = (query.limit + 1).into();

    let page: Option<(chrono::DateTime<chrono::offset::FixedOffset>, bool, i64)> = query
        .page
        .map(|src| {
            let mut spl = src.split(',');

            let ts = spl.next().ok_or(InvalidPage)?;
            let is_post = spl.next().ok_or(InvalidPage)?;
            let id = spl.next().ok_or(InvalidPage)?;
            if spl.next().is_some() {
                Err(InvalidPage)
            } else {
                use chrono::TimeZone;

                let ts: i64 = ts.parse().map_err(|_| InvalidPage)?;
                let is_post: bool = is_post.parse().map_err(|_| InvalidPage)?;
                let id: i64 = id.parse().map_err(|_| InvalidPage)?;

                let ts = chrono::offset::Utc.timestamp_nanos(ts);

                Ok((ts.into(), is_post, id))
            }
        })
        .transpose()
        .map_err(|err| err.into_user_error())?;

    let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&user_id, &limit_plus_1];

    let page_conditions = match &page {
        Some((ts, is_post, id)) => {
            values.push(ts);
            values.push(id);

            Cow::Owned(format!(
                " AND (created < $3 OR (created = $3 AND {}))",
                if *is_post {
                    "is_post AND id > $4"
                } else {
                    "is_post OR id > $4"
                }
            ))
        }
        None => Cow::Borrowed(""),
    };

    let sql: &str = &format!(
        "(SELECT TRUE AS is_post, post.id AS thing_id, post.href, post.title, post.created, community.id, community.name, community.local, community.ap_id, (SELECT COUNT(*) FROM post_like WHERE post_like.post = post.id), (SELECT COUNT(*) FROM reply WHERE reply.post = post.id), post.sticky, post.ap_id, post.local, post.content_html, post.content_text, post.content_markdown FROM post, community WHERE post.community = community.id AND post.author = $1 AND NOT post.deleted) UNION ALL (SELECT FALSE AS is_post, reply.id AS thing_id, reply.content_text, reply.content_html, reply.created, post.id, post.title, NULL, reply.ap_id, NULL, NULL, reply.local, post.ap_id, post.local, NULL, NULL, NULL FROM reply, post WHERE post.id = reply.post AND reply.author = $1 AND NOT reply.deleted){} ORDER BY created DESC, is_post ASC, thing_id DESC LIMIT $2",
        page_conditions,
    );

    let mut rows = db.query(sql, &values).await?;

    let next_page = if rows.len() > query.limit as usize {
        let row = rows.pop().unwrap();

        let ts: chrono::DateTime<chrono::offset::FixedOffset> = row.get(4);
        let ts = ts.timestamp_nanos();

        let is_post: bool = row.get(0);
        let id: i64 = row.get(1);

        Some(format!("{},{},{}", ts, is_post, id))
    } else {
        None
    };

    let rows = rows;

    let things: Vec<RespThingInfo> = rows
        .iter()
        .map(|row| {
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);
            let created = created.to_rfc3339();

            let post_ap_id: Option<&str> = row.get(12);
            let post_local: bool = row.get(13);

            if row.get(0) {
                let community_id = CommunityLocalID(row.get(5));
                let community_local = row.get(7);
                let community_ap_id: Option<_> = row.get(8);

                let community_remote_url = if community_local {
                    Some(Cow::Owned(String::from(
                        crate::apub_util::get_local_community_apub_id(
                            community_id,
                            &ctx.host_url_apub,
                        ),
                    )))
                } else {
                    community_ap_id.map(Cow::Borrowed)
                };

                let post_id = PostLocalID(row.get(1));

                let post_remote_url = if post_local {
                    Some(Cow::Owned(String::from(
                        crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub),
                    )))
                } else {
                    post_ap_id.map(Cow::Borrowed)
                };

                RespThingInfo::Post(RespPostListPost {
                    id: post_id,
                    href: ctx.process_href_opt(
                        row.get::<_, Option<&str>>(2).map(Cow::Borrowed),
                        post_id,
                    ),
                    title: Cow::Borrowed(row.get(3)),
                    created: Cow::Owned(created),
                    community: Cow::Owned(RespMinimalCommunityInfo {
                        id: community_id,
                        name: Cow::Borrowed(row.get(6)),
                        local: community_local,
                        host: crate::get_actor_host_or_unknown(
                            community_local,
                            community_ap_id,
                            &ctx.local_hostname,
                        ),
                        remote_url: community_remote_url,
                    }),
                    relevance: None,
                    remote_url: post_remote_url,
                    replies_count_total: row.get(10),
                    sticky: row.get(11),
                    score: row.get(9),
                    content_html_safe: row
                        .get::<_, Option<&str>>(14)
                        .map(|html| crate::clean_html(&html)),
                    content_text: row.get::<_, Option<&str>>(15).map(Cow::Borrowed),
                    content_markdown: row.get::<_, Option<&str>>(16).map(Cow::Borrowed),
                    author: None,
                    your_vote: None,
                })
            } else {
                let post_id = PostLocalID(row.get(5));

                let post_remote_url = if post_local {
                    Some(Cow::Owned(String::from(
                        crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub),
                    )))
                } else {
                    post_ap_id.map(Cow::Borrowed)
                };

                let comment_id = CommentLocalID(row.get(1));
                let comment_ap_id: Option<&str> = row.get(8);
                let comment_local: bool = row.get(11);

                let comment_remote_url = if comment_local {
                    Some(Cow::Owned(String::from(
                        crate::apub_util::get_local_comment_apub_id(comment_id, &ctx.host_url_apub),
                    )))
                } else {
                    comment_ap_id.map(Cow::Borrowed)
                };

                RespThingInfo::Comment {
                    base: RespMinimalCommentInfo {
                        id: comment_id,
                        remote_url: comment_remote_url,
                        content_text: row.get::<_, Option<_>>(2).map(Cow::Borrowed),
                        content_html_safe: row
                            .get::<_, Option<&str>>(3)
                            .map(|html| crate::clean_html(html)),
                    },
                    created,
                    post: RespMinimalPostInfo {
                        id: post_id,
                        title: row.get(6),
                        remote_url: post_remote_url,
                    },
                }
            }
        })
        .collect();

    crate::json_response(&RespList {
        next_page: next_page.map(Cow::Owned),
        items: Cow::Owned(things),
    })
}

pub fn route_users() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_handler_async("GET", route_unstable_users_list)
        .with_handler_async("POST", route_unstable_users_create)
        .with_child_parse::<UserIDOrMe, _>(
            crate::RouteNode::new()
                .with_handler_async("GET", route_unstable_users_get)
                .with_handler_async("PATCH", route_unstable_users_patch)
                .with_child(
                    "notifications",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_users_notifications_list),
                )
                .with_child(
                    "things",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_users_things_list),
                )
                .with_child(
                    "your_note",
                    crate::RouteNode::new()
                        .with_handler_async("PUT", route_unstable_users_your_note_put),
                ),
        )
}
