use crate::routes::well_known::{FingerRequestQuery, FingerResponse};
use crate::{CommentLocalID, CommunityLocalID, PostLocalID, UserLocalID};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

mod comments;
mod communities;
mod posts;

lazy_static::lazy_static! {
    static ref USERNAME_ALLOWED_CHARS: HashSet<char> = {
        use unic_char_range::chars;
        chars!('a'..='z').into_iter().chain(chars!('A'..='Z')).chain(chars!('0'..='9')).chain(std::iter::once('_'))
            .collect()
    };
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum SortType {
    Hot,
    New,
}

impl SortType {
    pub fn post_sort_sql(&self) -> &'static str {
        match self {
            SortType::Hot => "hot_rank((SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author), post.created) DESC",
            SortType::New => "post.created DESC",
        }
    }
}

#[derive(Serialize)]
struct Empty {}

#[derive(Serialize)]
struct JustID<T: serde::Serialize> {
    pub id: T,
}

#[derive(Deserialize)]
struct MaybeIncludeYour {
    #[serde(default)]
    pub include_your: bool,
}

#[derive(Serialize)]
struct RespMinimalAuthorInfo<'a> {
    id: UserLocalID,
    username: Cow<'a, str>,
    local: bool,
    host: Cow<'a, str>,
    remote_url: Option<Cow<'a, str>>,
}

#[derive(Serialize)]
struct JustUser<'a> {
    user: RespMinimalAuthorInfo<'a>,
}

#[derive(Serialize)]
struct RespMinimalCommunityInfo<'a> {
    id: CommunityLocalID,
    name: &'a str,
    local: bool,
    host: Cow<'a, str>,
    remote_url: Option<&'a str>,
}

#[derive(Serialize)]
struct RespMinimalPostInfo<'a> {
    id: PostLocalID,
    title: &'a str,
}

#[derive(Serialize)]
struct RespUserInfo<'a> {
    #[serde(flatten)]
    base: RespMinimalAuthorInfo<'a>,

    description: &'a str,
}

#[derive(Serialize)]
struct RespPostListPost<'a> {
    id: PostLocalID,
    title: &'a str,
    href: Option<&'a str>,
    content_text: Option<&'a str>,
    content_html: Option<&'a str>,
    author: Option<&'a RespMinimalAuthorInfo<'a>>,
    created: &'a str,
    community: &'a RespMinimalCommunityInfo<'a>,
}

#[derive(Serialize)]
struct RespMinimalCommentInfo<'a> {
    id: CommentLocalID,
    content_text: Option<Cow<'a, str>>,
    content_html: Option<Cow<'a, str>>,
}

#[derive(Serialize)]
struct RespPostCommentInfo<'a> {
    #[serde(flatten)]
    base: RespMinimalCommentInfo<'a>,

    author: Option<RespMinimalAuthorInfo<'a>>,
    created: String,
    deleted: bool,
    replies: Option<Vec<RespPostCommentInfo<'a>>>,
    has_replies: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    your_vote: Option<Option<Empty>>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum RespThingInfo<'a> {
    #[serde(rename = "post")]
    Post {
        id: PostLocalID,
        href: Option<&'a str>,
        title: &'a str,
        created: String,
        community: RespMinimalCommunityInfo<'a>,
    },
    #[serde(rename = "comment")]
    Comment {
        #[serde(flatten)]
        base: RespMinimalCommentInfo<'a>,
        created: String,
        post: RespMinimalPostInfo<'a>,
    },
}

pub fn route_api() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child(
        "unstable",
        crate::RouteNode::new()
            .with_child(
                "actors:lookup",
                crate::RouteNode::new().with_child_str(
                    crate::RouteNode::new().with_handler_async("GET", route_unstable_actors_lookup),
                ),
            )
            .with_child(
                "logins",
                crate::RouteNode::new()
                    .with_handler_async("POST", route_unstable_logins_create)
                    .with_child(
                        "~current",
                        crate::RouteNode::new()
                            .with_handler_async("GET", route_unstable_logins_current_get),
                    ),
            )
            .with_child(
                "nodeinfo/2.0",
                crate::RouteNode::new().with_handler_async("GET", route_unstable_nodeinfo_20_get),
            )
            .with_child("communities", communities::route_communities())
            .with_child(
                "instance",
                crate::RouteNode::new().with_handler_async("GET", route_unstable_instance_get),
            )
            .with_child("posts", posts::route_posts())
            .with_child("comments", comments::route_comments())
            .with_child(
                "users",
                crate::RouteNode::new()
                    .with_handler_async("POST", route_unstable_users_create)
                    .with_child(
                        "me",
                        crate::RouteNode::new()
                            .with_handler_async("PATCH", route_unstable_users_me_patch)
                            .with_child(
                                "following:posts",
                                crate::RouteNode::new().with_handler_async(
                                    "GET",
                                    route_unstable_users_me_following_posts_list,
                                ),
                            )
                            .with_child(
                                "notifications",
                                crate::RouteNode::new().with_handler_async(
                                    "GET",
                                    route_unstable_users_me_notifications_list,
                                ),
                            ),
                    )
                    .with_child_parse::<UserLocalID, _>(
                        crate::RouteNode::new()
                            .with_handler_async("GET", route_unstable_users_get)
                            .with_child(
                                "things",
                                crate::RouteNode::new()
                                    .with_handler_async("GET", route_unstable_users_things_list),
                            ),
                    ),
            ),
    )
}

async fn insert_token(
    user_id: UserLocalID,
    db: &tokio_postgres::Client,
) -> Result<uuid::Uuid, tokio_postgres::Error> {
    let token = uuid::Uuid::new_v4();
    db.execute(
        "INSERT INTO login (token, person, created) VALUES ($1, $2, current_timestamp)",
        &[&token, &user_id],
    )
    .await?;

    Ok(token)
}

enum Lookup<'a> {
    URL(url::Url),
    WebFinger { user: &'a str, host: &'a str },
}

fn parse_lookup(src: &str) -> Result<Lookup, crate::Error> {
    if src.starts_with("http") {
        return Ok(Lookup::URL(src.parse()?));
    }
    if let Some(at_idx) = src.rfind('@') {
        let mut user = &src[..at_idx];
        let host = &src[(at_idx + 1)..];
        if user.starts_with("acct:") {
            user = &user[5..];
        } else if user.starts_with('@') {
            user = &user[1..];
        }
        return Ok(Lookup::WebFinger { user, host });
    }

    Err(crate::Error::InternalStrStatic(
        "Unrecognized lookup format",
    ))
}

async fn route_unstable_actors_lookup(
    params: (String,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (query,) = params;
    println!("lookup {}", query);

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let lookup = parse_lookup(&query)?;

    let uri = match lookup {
        Lookup::URL(uri) => Some(uri),
        Lookup::WebFinger { user, host } => {
            let uri = format!(
                "https://{}/.well-known/webfinger?{}",
                host,
                serde_urlencoded::to_string(FingerRequestQuery {
                    resource: format!("acct:{}@{}", user, host).into(),
                    rel: Some("self".into()),
                })?
            );
            println!("{}", uri);
            let res = ctx
                .http_client
                .request(hyper::Request::get(uri).body(Default::default())?)
                .await?;

            if res.status() == hyper::StatusCode::NOT_FOUND {
                println!("not found");
                None
            } else {
                let res = crate::res_to_error(res).await?;

                let res = hyper::body::to_bytes(res.into_body()).await?;
                let res: FingerResponse = serde_json::from_slice(&res)?;

                let mut found_uri = None;
                for entry in res.links {
                    if entry.rel == "self"
                        && entry.type_.as_deref() == Some(crate::apub_util::ACTIVITY_TYPE)
                    {
                        if let Some(href) = entry.href {
                            found_uri = Some(href.parse()?);
                            break;
                        }
                    }
                }

                found_uri
            }
        }
    };

    let uri = match uri {
        Some(uri) => uri,
        None => {
            return Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(&serde_json::json!([]))?.into())?);
        }
    };

    let actor = crate::apub_util::fetch_actor(&uri, &db, &ctx.http_client).await?;

    if let crate::apub_util::ActorLocalInfo::Community { id, .. } = actor {
        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&serde_json::json!([{ "id": id }]))?.into())?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr("not_group", None).into_owned(),
        ))
    }
}

async fn route_unstable_logins_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct LoginsCreateBody<'a> {
        username: Cow<'a, str>,
        password: Cow<'a, str>,
    }

    let body: LoginsCreateBody<'_> = serde_json::from_slice(&body)?;

    let row = db
        .query_opt(
            "SELECT id, passhash FROM person WHERE LOWER(username)=LOWER($1) AND local",
            &[&body.username],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr("no_such_local_user_by_name", None).into_owned(),
            ))
        })?;

    let id = UserLocalID(row.get(0));
    let passhash: Option<String> = row.get(1);

    let passhash = passhash.ok_or_else(|| {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr("no_password", None).into_owned(),
        ))
    })?;

    let req_password = body.password.to_owned();

    let correct =
        tokio::task::spawn_blocking(move || bcrypt::verify(req_password.as_ref(), &passhash))
            .await??;

    if correct {
        let token = insert_token(id, &db).await?;

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&serde_json::json!({"token": token.to_string()}))?.into())?)
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr("password_incorrect", None).into_owned(),
        ))
    }
}

async fn route_unstable_logins_current_get(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let has_notifications: bool = {
        let row = db.query_one("SELECT EXISTS(SELECT 1 FROM notification WHERE to_user = $1 AND created_at > (SELECT last_checked_notifications FROM person WHERE id=$1))", &[&user]).await?;
        row.get(0)
    };

    let body = serde_json::to_vec(
        &serde_json::json!({"user": {"id": user, "has_unread_notifications": has_notifications}}),
    )?
    .into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body)?)
}

async fn route_unstable_nodeinfo_20_get(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let local_posts = {
        let row = db
            .query_one("SELECT COUNT(*) FROM post WHERE local", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let local_comments = {
        let row = db
            .query_one("SELECT COUNT(*) FROM reply WHERE local", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let local_users = {
        let row = db
            .query_one("SELECT COUNT(*) FROM person WHERE local", &[])
            .await?;
        row.get::<_, i64>(0)
    };

    let body = serde_json::json!({
        "version": "2.0",
        "software": {
            "name": "lotide",
            "version": env!("CARGO_PKG_VERSION")
        },
        "protocols": ["activitypub"],
        "services": {
            "inbound": [],
            "outbound": []
        },
        "openRegistrations": true,
        "usage": {
            "users": {
                "total": local_users,
            },
            "localPosts": local_posts,
            "localComments": local_comments
        },
        "metadata": {}
    });

    let body = serde_json::to_vec(&body)?.into();

    Ok(hyper::Response::builder()
        .header(
            hyper::header::CONTENT_TYPE,
            "application/json; profile=http://nodeinfo.diaspora.software/ns/schema/2.0#",
        )
        .body(body)?)
}

async fn route_unstable_instance_get(
    _: (),
    _ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let body = serde_json::json!({
        "software": {
            "name": "lotide",
            "version": env!("CARGO_PKG_VERSION"),
        }
    });

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&body)?.into())?)
}

async fn apply_comments_replies<'a, T>(
    comments: &mut Vec<(T, RespPostCommentInfo<'a>)>,
    include_your_for: Option<UserLocalID>,
    depth: u8,
    db: &tokio_postgres::Client,
    local_hostname: &'a str,
) -> Result<(), crate::Error> {
    let ids = comments
        .iter()
        .map(|(_, comment)| comment.base.id)
        .collect::<Vec<_>>();
    if depth > 0 {
        let mut replies =
            get_comments_replies_box(&ids, include_your_for, depth - 1, db, local_hostname).await?;

        for (_, comment) in comments {
            let current = replies.remove(&comment.base.id).unwrap_or_else(Vec::new);
            comment.has_replies = !current.is_empty();
            comment.replies = Some(current);
        }
    } else {
        use futures::stream::TryStreamExt;

        let stream = crate::query_stream(
            &db,
            "SELECT DISTINCT parent FROM reply WHERE parent = ANY($1)",
            &[&ids],
        )
        .await?;

        let with_replies: HashSet<CommentLocalID> = stream
            .map_err(crate::Error::from)
            .map_ok(|row| CommentLocalID(row.get(0)))
            .try_collect()
            .await?;

        for (_, comment) in comments {
            comment.has_replies = with_replies.contains(&comment.base.id);
        }
    }

    Ok(())
}

fn get_comments_replies_box<'a: 'b, 'b>(
    parents: &'b [CommentLocalID],
    include_your_for: Option<UserLocalID>,
    depth: u8,
    db: &'b tokio_postgres::Client,
    local_hostname: &'a str,
) -> std::pin::Pin<
    Box<
        dyn Future<
                Output = Result<
                    HashMap<CommentLocalID, Vec<RespPostCommentInfo<'a>>>,
                    crate::Error,
                >,
            > + Send
            + 'b,
    >,
> {
    Box::pin(get_comments_replies(
        parents,
        include_your_for,
        depth,
        db,
        local_hostname,
    ))
}

async fn get_comments_replies<'a>(
    parents: &[CommentLocalID],
    include_your_for: Option<UserLocalID>,
    depth: u8,
    db: &tokio_postgres::Client,
    local_hostname: &'a str,
) -> Result<HashMap<CommentLocalID, Vec<RespPostCommentInfo<'a>>>, crate::Error> {
    use futures::TryStreamExt;

    let sql1 = "SELECT reply.id, reply.author, reply.content_text, reply.created, reply.parent, reply.content_html, person.username, person.local, person.ap_id, reply.deleted";
    let (sql2, values): (_, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) =
        if include_your_for.is_some() {
            (
                ", EXISTS(SELECT 1 FROM reply_like WHERE reply = reply.id AND person = $2)",
                vec![&parents, &include_your_for],
            )
        } else {
            ("", vec![&parents])
        };
    let sql3 = " FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE parent = ANY($1::BIGINT[]) ORDER BY hot_rank((SELECT COUNT(*) FROM reply_like WHERE reply = reply.id AND person != reply.author), reply.created) DESC";

    let sql: &str = &format!("{}{}{}", sql1, sql2, sql3);

    let stream = crate::query_stream(db, sql, &values).await?;

    let mut comments: Vec<_> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id = CommentLocalID(row.get(0));
            let content_text: Option<String> = row.get(2);
            let content_html: Option<String> = row.get(5);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);
            let parent = CommentLocalID(row.get(4));

            let author_username: Option<String> = row.get(6);
            let author = author_username.map(|author_username| {
                let author_id = UserLocalID(row.get(1));
                let author_local: bool = row.get(7);
                let author_ap_id: Option<&str> = row.get(8);

                RespMinimalAuthorInfo {
                    id: author_id,
                    username: author_username.into(),
                    local: author_local,
                    host: crate::get_actor_host_or_unknown(
                        author_local,
                        author_ap_id,
                        &local_hostname,
                    ),
                    remote_url: author_ap_id.map(|x| x.to_owned().into()),
                }
            });

            futures::future::ok((
                parent,
                RespPostCommentInfo {
                    base: RespMinimalCommentInfo {
                        id,
                        content_text: content_text.map(From::from),
                        content_html: content_html.map(From::from),
                    },

                    author,
                    created: created.to_rfc3339(),
                    deleted: row.get(9),
                    replies: None,
                    has_replies: false,
                    your_vote: match include_your_for {
                        None => None,
                        Some(_) => Some(if row.get(10) { Some(Empty {}) } else { None }),
                    },
                },
            ))
        })
        .try_collect()
        .await?;

    apply_comments_replies(&mut comments, include_your_for, depth, db, local_hostname).await?;

    let mut result = HashMap::new();
    for (parent, comment) in comments {
        result.entry(parent).or_insert_with(Vec::new).push(comment);
    }

    Ok(result)
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
        #[serde(default)]
        login: bool,
    }

    let body: UsersCreateBody<'_> = serde_json::from_slice(&body)?;

    for ch in body.username.chars() {
        if !USERNAME_ALLOWED_CHARS.contains(&ch) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr("user_name_disallowed_chars", None).into_owned(),
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
            "INSERT INTO person (username, local, created_local, passhash) VALUES ($1, TRUE, current_timestamp, $2) RETURNING id",
            &[&body.username, &passhash],
        ).await?;

        trans.commit().await?;

        UserLocalID(row.get(0))
    };

    let output = if body.login {
        let token = insert_token(user_id, &db).await?;
        serde_json::json!({"user": {"id": user_id}, "token": token.to_string()})
    } else {
        serde_json::json!({"user": {"id": user_id}})
    };

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&output)?.into())?)
}

async fn route_unstable_users_me_patch(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct UsersEditBody<'a> {
        description: Option<Cow<'a, str>>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: UsersEditBody = serde_json::from_slice(&body)?;

    if let Some(description) = body.description {
        db.execute(
            "UPDATE person SET description=$1 WHERE id=$2",
            &[&description, &user],
        )
        .await?;

        // TODO maybe send this somewhere?
    }

    Ok(crate::empty_response())
}

async fn route_unstable_users_me_following_posts_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let limit: i64 = 30; // TODO make configurable

    let values: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&user, &limit];

    let stream = db.query_raw(
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, post.content_html, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND post.approved AND post.deleted=FALSE AND community.id IN (SELECT community FROM community_follow WHERE follower=$1 AND accepted) ORDER BY hot_rank((SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author), post.created) DESC LIMIT $2",
        values.iter().map(|s| *s as _)
    ).await?;

    let posts = handle_common_posts_list(stream, &ctx.local_hostname).await?;

    let body = serde_json::to_vec(&posts)?;

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

async fn route_unstable_users_me_notifications_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

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

    let body = serde_json::to_vec(&notifications)?;

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

async fn route_unstable_users_get(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt(
            "SELECT username, local, ap_id, description FROM person WHERE id=$1",
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

    let info = RespMinimalAuthorInfo {
        id: user_id,
        local,
        username: Cow::Borrowed(row.get(0)),
        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
        remote_url: ap_id.map(From::from),
    };

    let info = RespUserInfo {
        base: info,
        description: row.get(3),
    };

    let body = serde_json::to_vec(&info)?;

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

async fn route_unstable_users_things_list(
    params: (UserLocalID,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;

    let db = ctx.db_pool.get().await?;

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

    let body = serde_json::to_vec(&things)?;

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

async fn handle_common_posts_list(
    stream: impl futures::stream::TryStream<Ok = tokio_postgres::Row, Error = tokio_postgres::Error>
        + Send,
    local_hostname: &str,
) -> Result<Vec<serde_json::Value>, crate::Error> {
    use futures::stream::TryStreamExt;

    let posts: Vec<serde_json::Value> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id = PostLocalID(row.get(0));
            let author_id = row.get::<_, Option<_>>(1).map(UserLocalID);
            let href: Option<&str> = row.get(2);
            let content_text: Option<&str> = row.get(3);
            let content_html: Option<&str> = row.get(6);
            let title: &str = row.get(4);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(5);
            let community_id = CommunityLocalID(row.get(7));
            let community_name: &str = row.get(8);
            let community_local: bool = row.get(9);
            let community_ap_id: Option<&str> = row.get(10);

            let author = author_id.map(|id| {
                let author_name: &str = row.get(11);
                let author_local: bool = row.get(12);
                let author_ap_id: Option<&str> = row.get(13);
                RespMinimalAuthorInfo {
                    id,
                    username: author_name.into(),
                    local: author_local,
                    host: crate::get_actor_host_or_unknown(
                        author_local,
                        author_ap_id,
                        &local_hostname,
                    ),
                    remote_url: author_ap_id.map(|x| x.to_owned().into()),
                }
            });

            let community = RespMinimalCommunityInfo {
                id: community_id,
                name: community_name,
                local: community_local,
                host: crate::get_actor_host_or_unknown(
                    community_local,
                    community_ap_id,
                    &local_hostname,
                ),
                remote_url: community_ap_id,
            };

            let post = RespPostListPost {
                id,
                title,
                href,
                content_text,
                content_html,
                author: author.as_ref(),
                created: &created.to_rfc3339(),
                community: &community,
            };

            futures::future::ready(serde_json::to_value(&post).map_err(Into::into))
        })
        .try_collect()
        .await?;

    Ok(posts)
}
