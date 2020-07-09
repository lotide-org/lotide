use crate::routes::well_known::{FingerRequestQuery, FingerResponse};
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

mod communities;

lazy_static::lazy_static! {
    static ref USERNAME_ALLOWED_CHARS: HashSet<char> = {
        use unic_char_range::chars;
        chars!('a'..'z').into_iter().chain(chars!('A'..'Z')).chain(chars!('0'..'9')).chain(std::iter::once('_'))
            .collect()
    };
}

#[derive(Serialize)]
struct Empty {}

#[derive(Deserialize)]
struct MaybeIncludeYour {
    #[serde(default)]
    pub include_your: bool,
}

#[derive(Serialize)]
struct RespMinimalAuthorInfo<'a> {
    id: i64,
    username: Cow<'a, str>,
    local: bool,
    host: Cow<'a, str>,
}

#[derive(Serialize)]
struct RespMinimalCommunityInfo<'a> {
    id: i64,
    name: &'a str,
    local: bool,
    host: Cow<'a, str>,
}

#[derive(Serialize)]
struct RespMinimalPostInfo<'a> {
    id: i64,
    title: &'a str,
}

#[derive(Serialize)]
struct RespPostListPost<'a> {
    id: i64,
    title: &'a str,
    href: Option<&'a str>,
    content_text: Option<&'a str>,
    content_html: Option<&'a str>,
    author: Option<&'a RespMinimalAuthorInfo<'a>>,
    created: &'a str,
    community: &'a RespMinimalCommunityInfo<'a>,
}

#[derive(Serialize)]
struct RespPostCommentInfo<'a> {
    id: i64,
    author: Option<RespMinimalAuthorInfo<'a>>,
    created: Cow<'a, str>,
    content_text: Option<Cow<'a, str>>,
    content_html: Option<Cow<'a, str>>,
    deleted: bool,
    replies: Option<Vec<RespPostCommentInfo<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    your_vote: Option<Option<Empty>>,
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
                "posts",
                crate::RouteNode::new()
                    .with_handler_async("GET", route_unstable_posts_list)
                    .with_handler_async("POST", route_unstable_posts_create)
                    .with_child_parse::<i64, _>(
                        crate::RouteNode::new()
                            .with_handler_async("GET", route_unstable_posts_get)
                            .with_handler_async("DELETE", route_unstable_posts_delete)
                            .with_child(
                                "like",
                                crate::RouteNode::new()
                                    .with_handler_async("POST", route_unstable_posts_like),
                            )
                            .with_child(
                                "unlike",
                                crate::RouteNode::new()
                                    .with_handler_async("POST", route_unstable_posts_unlike),
                            )
                            .with_child(
                                "replies",
                                crate::RouteNode::new().with_handler_async(
                                    "POST",
                                    route_unstable_posts_replies_create,
                                ),
                            ),
                    ),
            )
            .with_child(
                "comments",
                crate::RouteNode::new().with_child_parse::<i64, _>(
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_comments_get)
                        .with_handler_async("DELETE", route_unstable_comments_delete)
                        .with_child(
                            "like",
                            crate::RouteNode::new()
                                .with_handler_async("POST", route_unstable_comments_like),
                        )
                        .with_child(
                            "unlike",
                            crate::RouteNode::new()
                                .with_handler_async("POST", route_unstable_comments_unlike),
                        )
                        .with_child(
                            "replies",
                            crate::RouteNode::new()
                                .with_handler_async("POST", route_unstable_comments_replies_create),
                        ),
                ),
            )
            .with_child(
                "users",
                crate::RouteNode::new()
                    .with_handler_async("POST", route_unstable_users_create)
                    .with_child(
                        "me",
                        crate::RouteNode::new().with_child(
                            "following:posts",
                            crate::RouteNode::new().with_handler_async(
                                "GET",
                                route_unstable_users_me_following_posts_list,
                            ),
                        ),
                    )
                    .with_child_parse::<i64, _>(
                        crate::RouteNode::new().with_handler_async("GET", route_unstable_users_get),
                    ),
            ),
    )
}

async fn insert_token(
    user_id: i64,
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
    URI(hyper::Uri),
    WebFinger { user: &'a str, host: &'a str },
}

fn parse_lookup<'a>(src: &'a str) -> Result<Lookup<'a>, crate::Error> {
    if src.starts_with("http") {
        return Ok(Lookup::URI(src.parse()?));
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

    return Err(crate::Error::InternalStrStatic(
        "Unrecognized lookup format",
    ));
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
        Lookup::URI(uri) => Some(uri),
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

    let uri_str = uri.to_string();

    let body = crate::apub_util::fetch_ap_object(&uri_str, &ctx.http_client).await?;

    let group: activitystreams::ext::Ext<
        activitystreams::actor::Group,
        activitystreams::actor::properties::ApActorProperties,
    > = serde_json::from_value(body)?;

    let name = group
        .extension
        .get_preferred_username()
        .map(|x| x.as_str())
        .or_else(|| group.as_ref().get_name_xsd_string().map(|x| x.as_str()))
        .unwrap_or("");
    let ap_inbox = group.extension.get_inbox().as_str();
    let ap_shared_inbox = group
        .extension
        .get_endpoints()
        .and_then(|endpoints| endpoints.get_shared_inbox())
        .map(|x| x.as_str());

    let row = db.query_one(
        "INSERT INTO community (name, local, ap_id, ap_inbox, ap_shared_inbox) VALUES ($1, FALSE, $2, $3, $4) ON CONFLICT (ap_id) DO UPDATE SET name=$1, ap_inbox=$3, ap_shared_inbox=$4 RETURNING id",
        &[&name, &group.as_ref().id.as_ref().unwrap().as_str(), &ap_inbox, &ap_shared_inbox],
    )
    .await?;

    let new_id: i64 = row.get(0);

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&serde_json::json!([{ "id": new_id }]))?.into())?)
}

async fn route_unstable_logins_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
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
                "No local user found by that name",
            ))
        })?;

    let id: i64 = row.get(0);
    let passhash: Option<String> = row.get(1);

    let passhash = passhash.ok_or_else(|| {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "No password set for this user",
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
            "Incorrect password",
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

    let body = serde_json::to_vec(&serde_json::json!({"user": {"id": user}}))?.into();

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

async fn route_unstable_posts_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let limit: i64 = 10;

    let stream = db.query_raw(
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, post.content_html, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND deleted=FALSE ORDER BY hot_rank((SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author), post.created) DESC LIMIT $1",
        ([limit]).iter().map(|x| x as _),
    ).await?;

    let posts = handle_common_posts_list(stream, &ctx.host_url_apub).await?;

    let body = serde_json::to_vec(&posts)?;

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

async fn route_unstable_posts_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct PostsCreateBody {
        community: i64,
        href: Option<String>,
        content_text: Option<String>,
        title: String,
    }

    let body: PostsCreateBody = serde_json::from_slice(&body)?;

    if body.href.is_none() && body.content_text.is_none() {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Post must contain either href or content_text",
        )));
    }

    // TODO validate permissions to post

    let res_row = db.query_one(
        "INSERT INTO post (author, href, content_text, title, created, community, local) VALUES ($1, $2, $3, $4, current_timestamp, $5, TRUE) RETURNING id, created, (SELECT local FROM community WHERE id=post.community)",
        &[&user, &body.href, &body.content_text, &body.title, &body.community],
    ).await?;

    crate::spawn_task(async move {
        let id = res_row.get(0);
        let created = res_row.get(1);
        let community_local: Option<bool> = res_row.get(2);

        let post = crate::PostInfoOwned {
            id,
            author: Some(user),
            content_text: body.content_text,
            href: body.href,
            title: body.title,
            created,
            community: body.community,
        };

        if let Some(community_local) = community_local {
            if community_local {
                crate::on_community_add_post(
                    post.community,
                    post.id,
                    &crate::apub_util::get_local_post_apub_id(post.id, &ctx.host_url_apub),
                    ctx,
                );
            } else {
                crate::apub_util::spawn_enqueue_send_local_post_to_community(post, ctx);
            }
        }

        Ok(())
    });

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn apply_comments_replies<'a, T>(
    comments: &mut Vec<(T, RespPostCommentInfo<'a>)>,
    include_your_for: Option<i64>,
    depth: u8,
    db: &tokio_postgres::Client,
    local_hostname: &'a str,
) -> Result<(), crate::Error> {
    if depth > 0 {
        let ids = comments
            .iter()
            .map(|(_, comment)| comment.id)
            .collect::<Vec<_>>();
        let mut replies =
            get_comments_replies_box(&ids, include_your_for, depth - 1, db, local_hostname).await?;

        for (_, comment) in comments {
            comment.replies = Some(replies.remove(&comment.id).unwrap_or_else(Vec::new));
        }
    }

    Ok(())
}

fn get_comments_replies_box<'a: 'b, 'b>(
    parents: &'b [i64],
    include_your_for: Option<i64>,
    depth: u8,
    db: &'b tokio_postgres::Client,
    local_hostname: &'a str,
) -> std::pin::Pin<
    Box<
        dyn Future<Output = Result<HashMap<i64, Vec<RespPostCommentInfo<'a>>>, crate::Error>>
            + Send
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
    parents: &[i64],
    include_your_for: Option<i64>,
    depth: u8,
    db: &tokio_postgres::Client,
    local_hostname: &'a str,
) -> Result<HashMap<i64, Vec<RespPostCommentInfo<'a>>>, crate::Error> {
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
            let id: i64 = row.get(0);
            let content_text: Option<String> = row.get(2);
            let content_html: Option<String> = row.get(5);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);
            let parent: i64 = row.get(4);

            let author_username: Option<String> = row.get(6);
            let author = author_username.map(|author_username| {
                let author_id: i64 = row.get(1);
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
                }
            });

            futures::future::ok((
                parent,
                RespPostCommentInfo {
                    id,
                    author,
                    content_text: content_text.map(From::from),
                    content_html: content_html.map(From::from),
                    created: created.to_rfc3339().into(),
                    deleted: row.get(9),
                    replies: None,
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
        result.entry(parent).or_insert(Vec::new()).push(comment);
    }

    Ok(result)
}

async fn get_post_comments<'a>(
    post_id: i64,
    include_your_for: Option<i64>,
    db: &tokio_postgres::Client,
    local_hostname: &'a str,
) -> Result<Vec<RespPostCommentInfo<'a>>, crate::Error> {
    use futures::TryStreamExt;

    let sql1 = "SELECT reply.id, reply.author, reply.content_text, reply.created, reply.content_html, person.username, person.local, person.ap_id, reply.deleted";
    let (sql2, values): (_, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) =
        if include_your_for.is_some() {
            (
                ", EXISTS(SELECT 1 FROM reply_like WHERE reply = reply.id AND person = $2)",
                vec![&post_id, &include_your_for],
            )
        } else {
            ("", vec![&post_id])
        };
    let sql3 = " FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE post=$1 AND parent IS NULL ORDER BY hot_rank((SELECT COUNT(*) FROM reply_like WHERE reply = reply.id AND person != reply.author), reply.created) DESC";

    let sql: &str = &format!("{}{}{}", sql1, sql2, sql3);

    let stream = crate::query_stream(db, sql, &values[..]).await?;

    let mut comments: Vec<_> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id: i64 = row.get(0);
            let content_text: Option<String> = row.get(2);
            let content_html: Option<String> = row.get(4);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);

            let author_username: Option<String> = row.get(5);
            let author = author_username.map(|author_username| {
                let author_id: i64 = row.get(1);
                let author_local: bool = row.get(6);
                let author_ap_id: Option<&str> = row.get(7);

                RespMinimalAuthorInfo {
                    id: author_id,
                    username: author_username.into(),
                    local: author_local,
                    host: crate::get_actor_host_or_unknown(
                        author_local,
                        author_ap_id,
                        &local_hostname,
                    ),
                }
            });

            futures::future::ok((
                (),
                RespPostCommentInfo {
                    id,
                    author,
                    content_text: content_text.map(From::from),
                    content_html: content_html.map(From::from),
                    created: created.to_rfc3339().into(),
                    deleted: row.get(8),
                    replies: None,
                    your_vote: match include_your_for {
                        None => None,
                        Some(_) => Some(if row.get(9) { Some(Empty {}) } else { None }),
                    },
                },
            ))
        })
        .try_collect()
        .await?;

    apply_comments_replies(&mut comments, include_your_for, 2, db, local_hostname).await?;

    Ok(comments.into_iter().map(|(_, comment)| comment).collect())
}

async fn route_unstable_posts_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use futures::future::TryFutureExt;

    let query: MaybeIncludeYour = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let db = ctx.db_pool.get().await?;

    let include_your_for = if query.include_your {
        let user = crate::require_login(&req, &db).await?;
        Some(user)
    } else {
        None
    };

    #[derive(Serialize)]
    struct RespPostInfo<'a> {
        #[serde(flatten)]
        post: &'a RespPostListPost<'a>,
        score: i64,
        comments: Vec<RespPostCommentInfo<'a>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        your_vote: Option<Option<Empty>>,
    }

    let (post_id,) = params;

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    let (row, comments, your_vote) = futures::future::try_join3(
        db.query_opt(
            "SELECT post.author, post.href, post.content_text, post.title, post.created, post.content_html, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id, (SELECT COUNT(*) FROM post_like WHERE post_like.post = $1) FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND post.id = $1",
            &[&post_id],
        )
        .map_err(crate::Error::from),
        get_post_comments(post_id, include_your_for, &db, &local_hostname),
        async {
            if let Some(user) = include_your_for {
                let row = db.query_opt("SELECT 1 FROM post_like WHERE post=$1 AND person=$2", &[&post_id, &user]).await?;
                if row.is_some() {
                    Ok(Some(Some(Empty {})))
                } else {
                    Ok(Some(None))
                }
            } else {
                Ok(None)
            }
        }
    ).await?;

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such post",
        )),
        Some(row) => {
            let href = row.get(1);
            let content_text = row.get(2);
            let content_html = row.get(5);
            let title = row.get(3);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);
            let community_id = row.get(6);
            let community_name = row.get(7);
            let community_local = row.get(8);
            let community_ap_id = row.get(9);

            let author = match row.get(10) {
                Some(author_username) => {
                    let author_local = row.get(11);
                    Some(RespMinimalAuthorInfo {
                        id: row.get(0),
                        username: Cow::Borrowed(author_username),
                        local: author_local,
                        host: crate::get_actor_host_or_unknown(
                            author_local,
                            row.get(12),
                            &local_hostname,
                        ),
                    })
                }
                None => None,
            };

            let community = RespMinimalCommunityInfo {
                id: community_id,
                name: community_name,
                local: community_local,
                host: crate::get_actor_host_or_unknown(
                    community_local,
                    community_ap_id,
                    &local_hostname,
                ),
            };

            let post = RespPostListPost {
                id: post_id,
                title,
                href,
                content_text,
                content_html,
                author: author.as_ref(),
                created: &created.to_rfc3339(),
                community: &community,
            };

            let output = RespPostInfo {
                post: &post,
                comments,
                score: row.get(13),
                your_vote,
            };

            let output = serde_json::to_vec(&output)?;

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(output.into())?)
        }
    }
}

async fn route_unstable_posts_delete(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let row = db
        .query_opt(
            "SELECT author, community FROM post WHERE id=$1 AND deleted=FALSE",
            &[&post_id],
        )
        .await?;
    match row {
        None => return Ok(crate::empty_response()), // already gone
        Some(row) => {
            let author: Option<i64> = row.get(0);
            if author != Some(user) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::FORBIDDEN,
                    "That's not your post",
                )));
            }

            db.execute("UPDATE post SET had_href=(href IS NOT NULL), href=NULL, title='[deleted]', content_text='[deleted]', deleted=TRUE WHERE id=$1", &[&post_id]).await?;

            crate::spawn_task(async move {
                let community: Option<i64> = row.get(1);
                if let Some(community) = community {
                    let delete_ap = crate::apub_util::local_post_delete_to_ap(
                        post_id,
                        user,
                        &ctx.host_url_apub,
                    )?;
                    let row = db.query_one("SELECT local, ap_id, COALESCE(ap_shared_inbox, ap_inbox) FROM community WHERE id=$1", &[&community]).await?;

                    let local = row.get(0);
                    if local {
                        crate::spawn_task(
                            crate::apub_util::enqueue_forward_to_community_followers(
                                community,
                                serde_json::to_string(&delete_ap)?,
                                ctx,
                            ),
                        );
                    } else {
                        let community_inbox: Option<String> = row.get(2);

                        if let Some(community_inbox) = community_inbox {
                            crate::spawn_task(async move {
                                ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                                    inbox: community_inbox.into(),
                                    sign_as: Some(crate::ActorLocalRef::Person(user)),
                                    object: serde_json::to_string(&delete_ap)?,
                                })
                                .await
                            });
                        }
                    }
                }

                Ok(())
            });

            Ok(crate::empty_response())
        }
    }
}

async fn route_unstable_posts_like(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let row_count = db.execute(
        "INSERT INTO post_like (post, person, local) VALUES ($1, $2, TRUE) ON CONFLICT (post, person) DO NOTHING",
        &[&post_id, &user],
    ).await?;

    if row_count > 0 {
        crate::spawn_task(async move {
            let row = db.query_opt(
                "SELECT post.local, post.ap_id, community.id, community.local, community.ap_id, COALESCE(community.ap_shared_inbox, community.ap_inbox), COALESCE(post_author.ap_shared_inbox, post_author.ap_inbox) FROM post LEFT OUTER JOIN community ON (post.community = community.id) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) WHERE post.id = $1",
                &[&post_id],
            ).await?;
            if let Some(row) = row {
                let post_local = row.get(0);
                let post_ap_id: &str = &if post_local {
                    Cow::Owned(crate::apub_util::get_local_post_apub_id(
                        post_id,
                        &ctx.host_url_apub,
                    ))
                } else {
                    Cow::Borrowed(row.get(1))
                };

                let mut inboxes = HashSet::new();

                if !post_local {
                    let author_inbox: Option<&str> = row.get(6);
                    if let Some(inbox) = author_inbox {
                        inboxes.insert(inbox);
                    }
                }

                let community_local: Option<bool> = row.get(3);

                if community_local == Some(false) {
                    if let Some(inbox) = row.get(5) {
                        inboxes.insert(inbox);
                    }
                }

                let like = crate::apub_util::local_post_like_to_ap(
                    post_id,
                    post_ap_id,
                    user,
                    &ctx.host_url_apub,
                )?;

                let body = serde_json::to_string(&like)?;

                for inbox in inboxes {
                    ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                        inbox: inbox.into(),
                        sign_as: Some(crate::ActorLocalRef::Person(user)),
                        object: (&body).into(),
                    })
                    .await?;
                }

                if community_local == Some(true) {
                    let community_local_id = row.get(2);
                    crate::apub_util::enqueue_forward_to_community_followers(
                        community_local_id,
                        body,
                        ctx,
                    )
                    .await?;
                }
            }

            Ok(())
        });
    }

    Ok(crate::empty_response())
}

async fn route_unstable_posts_unlike(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let new_undo = {
        let trans = db.transaction().await?;

        let row_count = trans
            .execute(
                "DELETE FROM post_like WHERE post=$1 AND person=$2",
                &[&post_id, &user],
            )
            .await?;

        let new_undo = if row_count > 0 {
            let id = uuid::Uuid::new_v4();
            trans
                .execute(
                    "INSERT INTO local_post_like_undo (id, post, person) VALUES ($1, $2, $3)",
                    &[&id, &post_id, &user],
                )
                .await?;

            Some(id)
        } else {
            None
        };

        trans.commit().await?;

        new_undo
    };

    if let Some(new_undo) = new_undo {
        crate::spawn_task(async move {
            let row = db.query_opt(
                "SELECT post.local, community.id, community.local, community.ap_id, COALESCE(community.ap_shared_inbox, community.ap_inbox), COALESCE(post_author.ap_shared_inbox, post_author.ap_inbox) FROM post LEFT OUTER JOIN community ON (post.community = community.id) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) WHERE post.id = $1",
                &[&post_id],
            ).await?;
            if let Some(row) = row {
                let post_local: bool = row.get(0);

                let mut inboxes = HashSet::new();

                if !post_local {
                    let author_inbox: Option<&str> = row.get(5);
                    if let Some(inbox) = author_inbox {
                        inboxes.insert(inbox);
                    }
                }

                let community_local: Option<bool> = row.get(2);

                if community_local == Some(false) {
                    if let Some(inbox) = row.get(4) {
                        inboxes.insert(inbox);
                    }
                }

                let undo = crate::apub_util::local_post_like_undo_to_ap(
                    new_undo,
                    post_id,
                    user,
                    &ctx.host_url_apub,
                )?;

                let body = serde_json::to_string(&undo)?;

                for inbox in inboxes {
                    ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                        inbox: inbox.into(),
                        sign_as: Some(crate::ActorLocalRef::Person(user)),
                        object: (&body).into(),
                    })
                    .await?;
                }

                if community_local == Some(true) {
                    let community_local_id = row.get(1);
                    crate::apub_util::enqueue_forward_to_community_followers(
                        community_local_id,
                        body,
                        ctx,
                    )
                    .await?;
                }
            }

            Ok(())
        });
    }

    Ok(crate::empty_response())
}

async fn route_unstable_posts_replies_create(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (post_id,) = params;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct RepliesCreateBody<'a> {
        content_text: Cow<'a, str>,
    }

    let body: RepliesCreateBody<'_> = serde_json::from_slice(&body)?;

    let row = db.query_one(
        "INSERT INTO reply (post, author, content_text, created, local) VALUES ($1, $2, $3, current_timestamp, TRUE) RETURNING id, created",
        &[&post_id, &user, &body.content_text],
    ).await?;

    let reply_id: i64 = row.get(0);
    let created = row.get(1);

    let comment = crate::CommentInfo {
        id: reply_id,
        author: Some(user),
        post: post_id,
        parent: None,
        content_text: Some(body.content_text.into_owned()),
        created,
        ap_id: crate::APIDOrLocal::Local,
    };

    crate::on_post_add_comment(comment, ctx);

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&serde_json::json!({ "id": reply_id }))?.into())?)
}

async fn route_unstable_comments_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use futures::future::TryFutureExt;

    let query: MaybeIncludeYour = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    #[derive(Serialize)]
    struct RespCommentInfo<'a> {
        #[serde(flatten)]
        base: RespPostCommentInfo<'a>,
        post: Option<RespMinimalPostInfo<'a>>,
    }

    let (comment_id,) = params;

    let db = ctx.db_pool.get().await?;

    let include_your_for = if query.include_your {
        let user = crate::require_login(&req, &db).await?;
        Some(user)
    } else {
        None
    };

    let (row, your_vote) = futures::future::try_join(
        db.query_opt(
            "SELECT reply.author, reply.post, reply.content_text, reply.created, reply.local, reply.content_html, person.username, person.local, person.ap_id, post.title, reply.deleted FROM reply INNER JOIN post ON (reply.post = post.id) LEFT OUTER JOIN person ON (reply.author = person.id) WHERE reply.id = $1",
            &[&comment_id],
        )
        .map_err(crate::Error::from),
        async {
            Ok(if let Some(user) = include_your_for {
                let row = db.query_opt(
                    "SELECT 1 FROM reply_like WHERE reply=$1 AND person=$2",
                    &[&comment_id, &user],
                ).await?;

                Some(row.map(|_| Empty {}))
            } else {
                None
            })
        },
    ).await?;

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        )),
        Some(row) => {
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);
            let author = match row.get(6) {
                Some(author_username) => {
                    let author_local = row.get(7);
                    Some(RespMinimalAuthorInfo {
                        id: row.get(0),
                        username: Cow::Borrowed(author_username),
                        local: author_local,
                        host: crate::get_actor_host_or_unknown(
                            author_local,
                            row.get(8),
                            &local_hostname,
                        ),
                    })
                }
                None => None,
            };

            let post = match row.get(9) {
                Some(post_title) => Some(RespMinimalPostInfo {
                    id: row.get(1),
                    title: post_title,
                }),
                None => None,
            };

            let replies =
                get_comments_replies(&[comment_id], include_your_for, 3, &db, &local_hostname)
                    .await?
                    .remove(&comment_id)
                    .unwrap_or_else(|| Vec::new());

            let output = RespCommentInfo {
                base: RespPostCommentInfo {
                    author,
                    content_text: row.get::<_, Option<&str>>(2).map(Cow::Borrowed),
                    content_html: row.get::<_, Option<&str>>(5).map(Cow::Borrowed),
                    created: created.to_rfc3339().into(),
                    deleted: row.get(10),
                    id: comment_id,
                    replies: Some(replies),
                    your_vote,
                },
                post,
            };

            let output = serde_json::to_vec(&output)?;

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(output.into())?)
        }
    }
}

async fn route_unstable_comments_delete(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let row = db
        .query_opt(
            "SELECT author, (SELECT community FROM post WHERE id=reply.post) FROM reply WHERE id=$1 AND deleted=FALSE",
            &[&comment_id],
        )
        .await?;
    match row {
        None => return Ok(crate::empty_response()), // already gone
        Some(row) => {
            let author: Option<i64> = row.get(0);
            if author != Some(user) {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::FORBIDDEN,
                    "That's not your post",
                )));
            }

            db.execute(
                "UPDATE reply SET content_text='[deleted]', deleted=TRUE WHERE id=$1",
                &[&comment_id],
            )
            .await?;

            crate::spawn_task(async move {
                let community: Option<i64> = row.get(1);
                if let Some(community) = community {
                    let delete_ap = crate::apub_util::local_comment_delete_to_ap(
                        comment_id,
                        user,
                        &ctx.host_url_apub,
                    )?;
                    let row = db.query_one("SELECT local, ap_id, COALESCE(ap_shared_inbox, ap_inbox) FROM community WHERE id=$1", &[&community]).await?;

                    let body = serde_json::to_string(&delete_ap)?;

                    let local = row.get(0);
                    if local {
                        crate::spawn_task(
                            crate::apub_util::enqueue_forward_to_community_followers(
                                community, body, ctx,
                            ),
                        );
                    } else {
                        let community_inbox: Option<String> = row.get(2);

                        if let Some(community_inbox) = community_inbox {
                            crate::spawn_task(async move {
                                ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                                    inbox: community_inbox.into(),
                                    sign_as: Some(crate::ActorLocalRef::Person(user)),
                                    object: body,
                                })
                                .await
                            });
                        }
                    }
                }

                Ok(())
            });

            Ok(crate::empty_response())
        }
    }
}

async fn route_unstable_comments_like(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let row_count = db.execute(
        "INSERT INTO reply_like (reply, person, local) VALUES ($1, $2, TRUE) ON CONFLICT (reply, person) DO NOTHING",
        &[&comment_id, &user],
    ).await?;

    if row_count > 0 {
        crate::spawn_task(async move {
            let row = db.query_opt(
                "SELECT reply.local, reply.ap_id, community.id, community.local, community.ap_id, COALESCE(community.ap_shared_inbox, community.ap_inbox), COALESCE(comment_author.ap_shared_inbox, comment_author.ap_inbox) FROM reply LEFT OUTER JOIN post ON (reply.post = post.id) LEFT OUTER JOIN community ON (post.community = community.id) LEFT OUTER JOIN person AS comment_author ON (comment_author.id = reply.author) WHERE reply.id = $1",
                &[&comment_id],
            ).await?;
            if let Some(row) = row {
                let comment_local = row.get(0);
                let comment_ap_id: &str = &if comment_local {
                    Cow::Owned(crate::apub_util::get_local_comment_apub_id(
                        comment_id,
                        &ctx.host_url_apub,
                    ))
                } else {
                    Cow::Borrowed(row.get(1))
                };

                let mut inboxes = HashSet::new();

                if !comment_local {
                    let author_inbox: Option<&str> = row.get(6);
                    if let Some(inbox) = author_inbox {
                        inboxes.insert(inbox);
                    }
                }

                let community_local: Option<bool> = row.get(3);

                if community_local == Some(false) {
                    if let Some(inbox) = row.get(5) {
                        inboxes.insert(inbox);
                    }
                }

                let like = crate::apub_util::local_comment_like_to_ap(
                    comment_id,
                    comment_ap_id,
                    user,
                    &ctx.host_url_apub,
                )?;

                let body = serde_json::to_string(&like)?;

                for inbox in inboxes {
                    ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                        inbox: inbox.into(),
                        sign_as: Some(crate::ActorLocalRef::Person(user)),
                        object: (&body).into(),
                    })
                    .await?;
                }

                if community_local == Some(true) {
                    let community_local_id = row.get(2);
                    crate::apub_util::enqueue_forward_to_community_followers(
                        community_local_id,
                        body,
                        ctx,
                    )
                    .await?;
                }
            }

            Ok(())
        });
    }

    Ok(crate::empty_response())
}

async fn route_unstable_comments_unlike(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (comment_id,) = params;

    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let new_undo = {
        let trans = db.transaction().await?;

        let row_count = trans
            .execute(
                "DELETE FROM reply_like WHERE reply=$1 AND person=$2",
                &[&comment_id, &user],
            )
            .await?;

        let new_undo = if row_count > 0 {
            let id = uuid::Uuid::new_v4();
            trans
                .execute(
                    "INSERT INTO local_reply_like_undo (id, reply, person) VALUES ($1, $2, $3)",
                    &[&id, &comment_id, &user],
                )
                .await?;

            Some(id)
        } else {
            None
        };

        trans.commit().await?;

        new_undo
    };

    if let Some(new_undo) = new_undo {
        crate::spawn_task(async move {
            let row = db.query_opt(
                "SELECT reply.local, reply.ap_id, community.id, community.local, community.ap_id, COALESCE(community.ap_shared_inbox, community.ap_inbox), COALESCE(comment_author.ap_shared_inbox, comment_author.ap_inbox) FROM reply LEFT OUTER JOIN post ON (reply.post = post.id) LEFT OUTER JOIN community ON (post.community = community.id) LEFT OUTER JOIN person AS comment_author ON (comment_author.id = reply.author) WHERE reply.id = $1",
                &[&comment_id],
            ).await?;
            if let Some(row) = row {
                let comment_local: bool = row.get(0);

                let mut inboxes = HashSet::new();

                if !comment_local {
                    let author_inbox: Option<&str> = row.get(6);
                    if let Some(inbox) = author_inbox {
                        inboxes.insert(inbox);
                    }
                }

                let community_local: Option<bool> = row.get(3);

                if community_local == Some(false) {
                    if let Some(inbox) = row.get(5) {
                        inboxes.insert(inbox);
                    }
                }

                let undo = crate::apub_util::local_comment_like_undo_to_ap(
                    new_undo,
                    comment_id,
                    user,
                    &ctx.host_url_apub,
                )?;

                let body = serde_json::to_string(&undo)?;

                for inbox in inboxes {
                    ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                        inbox: inbox.into(),
                        sign_as: Some(crate::ActorLocalRef::Person(user)),
                        object: (&body).into(),
                    })
                    .await?;
                }

                if community_local == Some(true) {
                    let community_local_id = row.get(2);
                    crate::apub_util::enqueue_forward_to_community_followers(
                        community_local_id,
                        body,
                        ctx,
                    )
                    .await?;
                }
            }

            Ok(())
        });
    }

    Ok(crate::empty_response())
}

async fn route_unstable_comments_replies_create(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (parent_id,) = params;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommentRepliesCreateBody<'a> {
        content_text: Cow<'a, str>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommentRepliesCreateBody<'_> = serde_json::from_slice(&body)?;

    let post: i64 = match db
        .query_opt("SELECT post FROM reply WHERE id=$1", &[&parent_id])
        .await?
    {
        None => Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        ))),
        Some(row) => Ok(row.get(0)),
    }?;

    let row = db.query_one(
        "INSERT INTO reply (post, parent, author, content_text, created, local) VALUES ($1, $2, $3, $4, current_timestamp, TRUE) RETURNING id, created",
        &[&post, &parent_id, &user, &body.content_text],
    ).await?;

    let reply_id: i64 = row.get(0);
    let created = row.get(1);

    let info = crate::CommentInfo {
        id: reply_id,
        author: Some(user),
        post,
        parent: Some(parent_id),
        content_text: Some(body.content_text.into_owned()),
        created,
        ap_id: crate::APIDOrLocal::Local,
    };

    crate::on_post_add_comment(info, ctx);

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::to_vec(&serde_json::json!({ "id": reply_id, "post": {"id": post} }))?
                .into(),
        )?)
}

async fn route_unstable_users_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

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
                "Username contains disallowed characters",
            )));
        }
    }

    let req_password = body.password;
    let passhash =
        tokio::task::spawn_blocking(move || bcrypt::hash(req_password, bcrypt::DEFAULT_COST))
            .await??;

    let row = db.query_one(
        "INSERT INTO person (username, local, created_local, passhash) VALUES ($1, TRUE, current_timestamp, $2) RETURNING id",
        &[&body.username, &passhash],
    ).await?;

    let user_id: i64 = row.get(0);

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

async fn route_unstable_users_me_following_posts_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let limit: i64 = 10; // TODO make configurable

    let values: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&user, &limit];

    let stream = db.query_raw(
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, post.content_html, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND deleted=FALSE AND community.id IN (SELECT community FROM community_follow WHERE follower=$1 AND accepted) ORDER BY hot_rank((SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author), post.created) DESC LIMIT $2",
        values.iter().map(|s| *s as _)
    ).await?;

    let posts = handle_common_posts_list(stream, &ctx.host_url_apub).await?;

    let body = serde_json::to_vec(&posts)?;

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

async fn route_unstable_users_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (user_id,) = params;

    let db = ctx.db_pool.get().await?;

    let row = db
        .query_opt(
            "SELECT username, local, ap_id FROM person WHERE id=$1",
            &[&user_id],
        )
        .await?;

    let row = row.ok_or_else(|| {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such user",
        ))
    })?;

    let local = row.get(1);

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    let info = RespMinimalAuthorInfo {
        id: user_id,
        local,
        username: Cow::Borrowed(row.get(0)),
        host: crate::get_actor_host_or_unknown(local, row.get(2), &local_hostname),
    };

    let body = serde_json::to_vec(&info)?;

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

async fn handle_common_posts_list(
    stream: impl futures::stream::TryStream<Ok = tokio_postgres::Row, Error = tokio_postgres::Error>
        + Send,
    host_url_apub: &str,
) -> Result<Vec<serde_json::Value>, crate::Error> {
    use futures::stream::TryStreamExt;

    let local_hostname = crate::get_url_host(host_url_apub).unwrap();

    let posts: Vec<serde_json::Value> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id: i64 = row.get(0);
            let author_id: Option<i64> = row.get(1);
            let href: Option<&str> = row.get(2);
            let content_text: Option<&str> = row.get(3);
            let content_html: Option<&str> = row.get(6);
            let title: &str = row.get(4);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(5);
            let community_id: i64 = row.get(7);
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
