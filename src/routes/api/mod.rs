use crate::routes::well_known::{FingerRequestQuery, FingerResponse};
use crate::{CommentLocalID, CommunityLocalID, PostLocalID, UserLocalID};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

mod comments;
mod communities;
mod forgot_password;
mod media;
mod posts;
mod users;

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
struct JustID<T: serde::Serialize> {
    pub id: T,
}

#[derive(Deserialize)]
struct MaybeIncludeYour {
    #[serde(default)]
    pub include_your: bool,
}

#[derive(Serialize)]
struct RespAvatarInfo<'a> {
    url: Cow<'a, str>,
}

#[derive(Serialize)]
struct RespMinimalAuthorInfo<'a> {
    id: UserLocalID,
    username: Cow<'a, str>,
    local: bool,
    host: Cow<'a, str>,
    remote_url: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avatar: Option<RespAvatarInfo<'a>>,
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
struct RespPostListPost<'a> {
    id: PostLocalID,
    title: &'a str,
    href: Option<Cow<'a, str>>,
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
    your_vote: Option<Option<crate::Empty>>,
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
                            .with_handler_async("GET", route_unstable_logins_current_get)
                            .with_handler_async("DELETE", route_unstable_logins_current_delete),
                    ),
            )
            .with_child("media", media::route_media())
            .with_child(
                "nodeinfo/2.0",
                crate::RouteNode::new().with_handler_async("GET", route_unstable_nodeinfo_20_get),
            )
            .with_child("communities", communities::route_communities())
            .with_child(
                "instance",
                crate::RouteNode::new()
                    .with_handler_async("GET", route_unstable_instance_get)
                    .with_handler_async("PATCH", route_unstable_instance_patch),
            )
            .with_child(
                "misc",
                crate::RouteNode::new().with_child(
                    "render_markdown",
                    crate::RouteNode::new()
                        .with_handler_async("POST", route_unstable_misc_render_markdown),
                ),
            )
            .with_child("posts", posts::route_posts())
            .with_child("comments", comments::route_comments())
            .with_child("users", users::route_users())
            .with_child("forgot_password", forgot_password::route_forgot_password()),
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
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (query,) = params;
    println!("lookup {}", query);

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
            return Ok(crate::common_response_builder()
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body("[]".into())?);
        }
    };

    let actor = crate::apub_util::fetch_actor(&uri, &db, &ctx.http_client).await?;

    let info = match actor {
        crate::apub_util::ActorLocalInfo::Community { id, .. } => {
            serde_json::json!({"id": id, "type": "community"})
        }
        crate::apub_util::ActorLocalInfo::User { id, .. } => {
            serde_json::json!({"id": id, "type": "user"})
        }
    };

    crate::json_response(&[info])
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

        crate::json_response(&serde_json::json!({"token": token.to_string()}))
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

    let row = db.query_one("SELECT username, is_site_admin, EXISTS(SELECT 1 FROM notification WHERE to_user = person.id AND created_at > person.last_checked_notifications) FROM person WHERE id=$1", &[&user]).await?;
    let username: &str = row.get(0);
    let is_site_admin: bool = row.get(1);
    let has_notifications: bool = row.get(2);

    crate::json_response(&serde_json::json!({
        "user": {"id": user, "name": username, "is_site_admin": is_site_admin, "has_unread_notifications": has_notifications}
    }))
}

async fn route_unstable_logins_current_delete(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    if let Some(token) = crate::get_auth_token(&req) {
        let db = ctx.db_pool.get().await?;
        db.execute("DELETE FROM login WHERE token=$1", &[&token])
            .await?;
    }

    Ok(crate::empty_response())
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

    Ok(crate::common_response_builder()
        .header(
            hyper::header::CONTENT_TYPE,
            "application/json; profile=http://nodeinfo.diaspora.software/ns/schema/2.0#",
        )
        .body(body)?)
}

async fn route_unstable_instance_get(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_one("SELECT description FROM site WHERE local = TRUE", &[])
        .await?;
    let description: &str = row.get(0);

    let body = serde_json::json!({
        "description": description,
        "software": {
            "name": "lotide",
            "version": env!("CARGO_PKG_VERSION"),
        }
    });

    crate::json_response(&body)
}

async fn route_unstable_instance_patch(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    #[derive(Deserialize)]
    struct InstanceEditBody<'a> {
        description: Option<Cow<'a, str>>,
    }

    let lang = crate::get_lang_for_req(&req);

    let (req_parts, body) = req.into_parts();

    let body = hyper::body::to_bytes(body).await?;
    let body: InstanceEditBody = serde_json::from_slice(&body)?;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req_parts, &db).await?;

    let is_site_admin: bool = {
        let row = db
            .query_one("SELECT is_site_admin FROM person WHERE id=$1", &[&user])
            .await?;
        row.get(0)
    };

    if is_site_admin {
        if let Some(description) = body.description {
            db.execute("UPDATE site SET description=$1", &[&description])
                .await?;
        }

        Ok(crate::empty_response())
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr("not_admin", None).into_owned(),
        ))
    }
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

        for (_, comment) in comments.iter_mut() {
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

        for (_, comment) in comments.iter_mut() {
            comment.has_replies = with_replies.contains(&comment.base.id);
        }
    }

    comments.retain(|(_, comment)| !comment.deleted || comment.has_replies);

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

    let sql1 = "SELECT reply.id, reply.author, reply.content_text, reply.created, reply.parent, reply.content_html, person.username, person.local, person.ap_id, reply.deleted, person.avatar";
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
                let author_avatar: Option<&str> = row.get(10);

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
                    avatar: author_avatar.map(|url| RespAvatarInfo {
                        url: url.to_owned().into(),
                    }),
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
                        Some(_) => Some(if row.get(11) {
                            Some(crate::Empty {})
                        } else {
                            None
                        }),
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

async fn route_unstable_misc_render_markdown(
    _: (),
    _ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct RenderMarkdownBody<'a> {
        content_markdown: Cow<'a, str>,
    }

    let body: RenderMarkdownBody = serde_json::from_slice(&body)?;

    let html =
        tokio::task::spawn_blocking(move || crate::render_markdown(&body.content_markdown)).await?;

    crate::json_response(&serde_json::json!({ "content_html": html }))
}

async fn handle_common_posts_list(
    stream: impl futures::stream::TryStream<Ok = tokio_postgres::Row, Error = tokio_postgres::Error>
        + Send,
    ctx: &crate::RouteContext,
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
                let author_avatar: Option<&str> = row.get(14);
                RespMinimalAuthorInfo {
                    id,
                    username: author_name.into(),
                    local: author_local,
                    host: crate::get_actor_host_or_unknown(
                        author_local,
                        author_ap_id,
                        &ctx.local_hostname,
                    ),
                    remote_url: author_ap_id.map(|x| x.to_owned().into()),
                    avatar: author_avatar.map(|url| RespAvatarInfo {
                        url: url.to_owned().into(),
                    }),
                }
            });

            let community = RespMinimalCommunityInfo {
                id: community_id,
                name: community_name,
                local: community_local,
                host: crate::get_actor_host_or_unknown(
                    community_local,
                    community_ap_id,
                    &ctx.local_hostname,
                ),
                remote_url: community_ap_id,
            };

            let post = RespPostListPost {
                id,
                title,
                href: ctx.process_href_opt(href, id),
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

pub async fn process_comment_content<'a>(
    lang: &crate::Translator,
    content_text: Option<Cow<'a, str>>,
    content_markdown: Option<String>,
) -> Result<(Option<Cow<'a, str>>, Option<String>, Option<String>), crate::Error> {
    if !(content_markdown.is_some() ^ content_text.is_some()) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr("comment_content_conflict", None).into_owned(),
        )));
    }

    Ok(match content_markdown {
        Some(md) => {
            if md.trim().is_empty() {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    lang.tr("comment_empty", None).into_owned(),
                )));
            }

            let (html, md) =
                tokio::task::spawn_blocking(move || (crate::render_markdown(&md), md)).await?;
            (None, Some(md), Some(html))
        }
        None => match content_text {
            Some(text) => {
                if text.trim().is_empty() {
                    return Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr("comment_empty", None).into_owned(),
                    )));
                }

                (Some(text), None, None)
            }
            None => (None, None, None),
        },
    })
}
