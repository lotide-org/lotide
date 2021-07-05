use super::{
    MaybeIncludeYour, RespAvatarInfo, RespLoginUserInfo, RespMinimalAuthorInfo,
    RespMinimalCommentInfo, RespMinimalCommunityInfo, RespMinimalPostInfo, RespThingInfo,
};
use crate::{CommentLocalID, CommunityLocalID, PostLocalID, UserLocalID};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::ops::Deref;
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

#[derive(Deserialize, Serialize)]
struct JustContentText<'a> {
    content_text: Cow<'a, str>,
}

#[derive(Serialize)]
struct RespUserInfo<'a> {
    #[serde(flatten)]
    base: RespMinimalAuthorInfo<'a>,

    description: &'a str,
    description_html: Option<&'a str>,
    description_text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suspended: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    your_note: Option<Option<JustContentText<'a>>>,
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
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: UsersEditBody = serde_json::from_slice(&body)?;

    let arena = bumpalo::Bump::new();

    let mut changes = Vec::<(&str, &(dyn tokio_postgres::types::ToSql + Sync))>::new();

    if let Some(description) = body.description_text.as_ref() {
        changes.push(("description", description));
    }
    if let Some(email_address) = body.email_address.as_ref() {
        if !fast_chemail::is_valid_email(&email_address) {
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

async fn route_unstable_users_following_posts_list(
    params: (UserIDOrMe,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let user = params.0.require_me(&req, &db).await?;

    let limit: i64 = 30; // TODO make configurable

    let values: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&user, &limit];

    let stream = db.query_raw(
        format!("SELECT {} FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND post.approved AND post.deleted=FALSE AND community.id IN (SELECT community FROM community_follow WHERE follower=$1 AND accepted) ORDER BY hot_rank((SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author), post.created) DESC LIMIT $2", super::common_posts_list_query(Some(1))).deref(),
        values.iter().map(|s| *s as _)
    ).await?;

    let posts = super::handle_common_posts_list(stream, &ctx, true).await?;

    crate::json_response(&posts)
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
            "SELECT notification.kind, (notification.created_at > (SELECT last_checked_notifications FROM person WHERE id=$1)), reply.id, reply.content_text, reply.content_html, parent_reply.id, parent_reply_post.id, parent_reply_post.title, parent_post.id, parent_post.title FROM notification LEFT OUTER JOIN reply ON (reply.id = notification.reply) LEFT OUTER JOIN reply AS parent_reply ON (parent_reply.id = notification.parent_reply) LEFT OUTER JOIN post AS parent_reply_post ON (parent_reply_post.id = parent_reply.post) LEFT OUTER JOIN post AS parent_post ON (parent_post.id = notification.parent_post) WHERE notification.to_user = $1 AND NOT COALESCE(reply.deleted OR parent_reply.deleted OR parent_reply_post.deleted OR parent_post.deleted, FALSE) ORDER BY created_at DESC LIMIT $2",
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

    #[derive(Serialize)]
    #[serde(tag = "type")]
    #[serde(rename_all = "snake_case")]
    enum RespNotificationInfo<'a> {
        PostReply {
            reply: RespMinimalCommentInfo<'a>,
            post: RespMinimalPostInfo<'a>,
        },
        CommentReply {
            reply: RespMinimalCommentInfo<'a>,
            comment: CommentLocalID,
            post: Option<RespMinimalPostInfo<'a>>,
        },
    }

    #[derive(Serialize)]
    struct RespNotification<'a> {
        #[serde(flatten)]
        info: RespNotificationInfo<'a>,

        unseen: bool,
    }

    let notifications: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            let kind: &str = row.get(0);
            let unseen: bool = row.get(1);
            let info = match kind {
                "post_reply" => {
                    if let Some(reply_id) = row.get(2) {
                        if let Some(post_id) = row.get(8) {
                            let comment = RespMinimalCommentInfo {
                                id: CommentLocalID(reply_id),
                                content_text: row.get::<_, Option<_>>(3).map(Cow::Borrowed),
                                content_html_safe: row
                                    .get::<_, Option<&str>>(4)
                                    .map(|html| crate::clean_html(&html)),
                            };
                            let post = RespMinimalPostInfo {
                                id: PostLocalID(post_id),
                                title: row.get(9),
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
                            let reply = RespMinimalCommentInfo {
                                id: CommentLocalID(reply_id),
                                content_text: row.get::<_, Option<_>>(3).map(Cow::Borrowed),
                                content_html_safe: row
                                    .get::<_, Option<&str>>(4)
                                    .map(|html| crate::clean_html(&html)),
                            };
                            let parent_id = CommentLocalID(parent_id);
                            let post =
                                row.get::<_, Option<_>>(6)
                                    .map(|post_id| RespMinimalPostInfo {
                                        id: PostLocalID(post_id),
                                        title: row.get(7),
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
            "SELECT username, local, ap_id, description, description_html, avatar, suspended FROM person WHERE id=$1",
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
    let ap_id = row.get(2);
    let avatar: Option<&str> = row.get(5);

    let info = RespMinimalAuthorInfo {
        id: user_id,
        local,
        username: Cow::Borrowed(row.get(0)),
        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
        remote_url: ap_id.map(From::from),
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
        description_html: description_html.as_deref(),
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

    let limit: i64 = 30;

    let rows = db.query(
        "(SELECT TRUE, post.id, post.href, post.title, post.created, community.id, community.name, community.local, community.ap_id, (SELECT COUNT(*) FROM post_like WHERE post_like.post = post.id), (SELECT COUNT(*) FROM reply WHERE reply.post = post.id) FROM post, community WHERE post.community = community.id AND post.author = $1 AND NOT post.deleted) UNION ALL (SELECT FALSE, reply.id, reply.content_text, reply.content_html, reply.created, post.id, post.title, NULL, NULL, NULL, NULL FROM reply, post WHERE post.id = reply.post AND reply.author = $1 AND NOT reply.deleted) ORDER BY created DESC LIMIT $2",
        &[&user_id, &limit],
    )
        .await?;

    let things: Vec<RespThingInfo> = rows
        .iter()
        .map(|row| {
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);
            let created = created.to_rfc3339();

            if row.get(0) {
                let community_local = row.get(7);
                let community_ap_id = row.get(8);

                let post_id = PostLocalID(row.get(1));

                RespThingInfo::Post {
                    id: post_id,
                    href: ctx.process_href_opt(row.get(2), post_id),
                    title: row.get(3),
                    created,
                    community: RespMinimalCommunityInfo {
                        id: CommunityLocalID(row.get(5)),
                        name: row.get(6),
                        local: community_local,
                        host: crate::get_actor_host_or_unknown(
                            community_local,
                            community_ap_id,
                            &ctx.local_hostname,
                        ),
                        remote_url: community_ap_id,
                    },
                    replies_count_total: row.get(10),
                    score: row.get(9),
                }
            } else {
                RespThingInfo::Comment {
                    base: RespMinimalCommentInfo {
                        id: CommentLocalID(row.get(1)),
                        content_text: row.get::<_, Option<_>>(2).map(Cow::Borrowed),
                        content_html_safe: row
                            .get::<_, Option<&str>>(3)
                            .map(|html| crate::clean_html(&html)),
                    },
                    created,
                    post: RespMinimalPostInfo {
                        id: PostLocalID(row.get(5)),
                        title: row.get(6),
                    },
                }
            }
        })
        .collect();

    crate::json_response(&things)
}

pub fn route_users() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_handler_async("POST", route_unstable_users_create)
        .with_child_parse::<UserIDOrMe, _>(
            crate::RouteNode::new()
                .with_handler_async("GET", route_unstable_users_get)
                .with_handler_async("PATCH", route_unstable_users_patch)
                .with_child(
                    "following:posts",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_users_following_posts_list),
                )
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
