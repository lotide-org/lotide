use super::{
    handle_common_posts_list, MaybeIncludeYour, RespAvatarInfo, RespMinimalAuthorInfo,
    RespMinimalCommentInfo, RespMinimalCommunityInfo, RespMinimalPostInfo, RespThingInfo,
};
use crate::{CommentLocalID, CommunityLocalID, PostLocalID, UserLocalID};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

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
}

impl std::str::FromStr for UserIDOrMe {
    type Err = std::num::ParseIntError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        if src == "~me" || src == "me"
        /* temporary backward compat */
        {
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

    let output = if body.login {
        let token = super::insert_token(user_id, &db).await?;
        serde_json::json!({"user": {"id": user_id}, "token": token.to_string()})
    } else {
        serde_json::json!({"user": {"id": user_id}})
    };

    crate::json_response(&output)
}

async fn route_unstable_users_patch(
    params: (UserIDOrMe,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user_id = params.0.require_me(&req, &db).await?;

    #[derive(Deserialize)]
    struct UsersEditBody<'a> {
        description: Option<Cow<'a, str>>,
        email_address: Option<Cow<'a, str>>,
        password: Option<String>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: UsersEditBody = serde_json::from_slice(&body)?;

    let arena = bumpalo::Bump::new();

    let mut changes = Vec::<(&str, &(dyn tokio_postgres::types::ToSql + Sync))>::new();

    if let Some(description) = body.description.as_ref() {
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

        db.execute(sql, &values).await?;
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
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, post.content_html, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id, person.avatar FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND post.approved AND post.deleted=FALSE AND community.id IN (SELECT community FROM community_follow WHERE follower=$1 AND accepted) ORDER BY hot_rank((SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author), post.created) DESC LIMIT $2",
        values.iter().map(|s| *s as _)
    ).await?;

    let posts = handle_common_posts_list(stream, &ctx.local_hostname).await?;

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
                                content_html: row.get::<_, Option<_>>(4).map(Cow::Borrowed),
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
                                content_html: row.get::<_, Option<_>>(4).map(Cow::Borrowed),
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
            "SELECT username, local, ap_id, description, avatar FROM person WHERE id=$1",
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
    let avatar: Option<&str> = row.get(4);

    let info = RespMinimalAuthorInfo {
        id: user_id,
        local,
        username: Cow::Borrowed(row.get(0)),
        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
        remote_url: ap_id.map(From::from),
        avatar: avatar.map(|url| RespAvatarInfo { url: url.into() }),
    };

    let info = RespUserInfo {
        base: info,
        description: row.get(3),
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
        "(SELECT TRUE, post.id, post.href, post.title, post.created, community.id, community.name, community.local, community.ap_id FROM post, community WHERE post.community = community.id AND post.author = $1 AND NOT post.deleted) UNION ALL (SELECT FALSE, reply.id, reply.content_text, reply.content_html, reply.created, post.id, post.title, NULL, NULL FROM reply, post WHERE post.id = reply.post AND reply.author = $1 AND NOT reply.deleted) ORDER BY created DESC LIMIT $2",
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

                RespThingInfo::Post {
                    id: PostLocalID(row.get(1)),
                    href: row.get(2),
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
                }
            } else {
                RespThingInfo::Comment {
                    base: RespMinimalCommentInfo {
                        id: CommentLocalID(row.get(1)),
                        content_text: row.get::<_, Option<_>>(2).map(Cow::Borrowed),
                        content_html: row.get::<_, Option<_>>(3).map(Cow::Borrowed),
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
