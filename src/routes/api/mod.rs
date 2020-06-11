use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

mod communities;

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
    author: Option<&'a RespMinimalAuthorInfo<'a>>,
    created: &'a str,
    community: &'a RespMinimalCommunityInfo<'a>,
}

#[derive(Serialize)]
struct RespPostCommentInfo<'a> {
    id: i64,
    author: Option<RespMinimalAuthorInfo<'a>>,
    created: Cow<'a, str>,
    content_text: Cow<'a, str>,
    replies: Option<Vec<RespPostCommentInfo<'a>>>,
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
            .with_child("communities", communities::route_communities())
            .with_child(
                "posts",
                crate::RouteNode::new()
                    .with_handler_async("GET", route_unstable_posts_list)
                    .with_handler_async("POST", route_unstable_posts_create)
                    .with_child_parse::<i64, _>(
                        crate::RouteNode::new()
                            .with_handler_async("GET", route_unstable_posts_get)
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

async fn route_unstable_actors_lookup(
    params: (String,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (query,) = params;
    println!("lookup {}", query);

    let db = ctx.db_pool.get().await?;

    let uri = query.parse::<hyper::Uri>()?;

    let res = ctx
        .http_client
        .request(
            hyper::Request::get(uri)
                .header(hyper::header::ACCEPT, crate::apub_util::ACTIVITY_TYPE)
                .body(Default::default())?,
        )
        .await?;

    let body = hyper::body::to_bytes(res.into_body()).await?;

    let group: activitystreams::ext::Ext<
        activitystreams::actor::Group,
        activitystreams::actor::properties::ApActorProperties,
    > = serde_json::from_slice(&body)?;

    let name = group.as_ref().get_name_xsd_string();
    let ap_inbox = group.extension.get_inbox();

    if let Some(name) = name {
        db.execute(
            "INSERT INTO community (name, local, ap_id, ap_inbox) VALUES ($1, FALSE, $2, $3)",
            &[&name.as_str(), &query, &ap_inbox.as_str()],
        )
        .await?;
    }

    Ok(crate::simple_response(
        hyper::StatusCode::ACCEPTED,
        "accepted",
    ))
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
        username: &'a str,
        password: &'a str,
    }

    let body: LoginsCreateBody<'_> = serde_json::from_slice(&body)?;

    let row = db
        .query_opt(
            "SELECT id, passhash FROM person WHERE username=$1 AND local",
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
        tokio::task::spawn_blocking(move || bcrypt::verify(req_password, &passhash)).await??;

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

async fn route_unstable_posts_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let limit: i64 = 10;

    let stream = db.query_raw(
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id ORDER BY created DESC LIMIT $1",
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

        let post = crate::PostInfo {
            id,
            author: Some(user),
            content_text: body.content_text.as_deref(),
            href: body.href.as_deref(),
            title: &body.title,
            created: &created,
            community: body.community,
        };

        if let Some(community_local) = community_local {
            if community_local {
                crate::on_community_add_post(&post, ctx);
            } else {
                crate::apub_util::send_post_to_community(post, ctx).await?;
            }
        }

        Ok(())
    });

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn apply_comments_replies<'a, T>(
    comments: &mut Vec<(T, RespPostCommentInfo<'a>)>,
    depth: u8,
    db: &tokio_postgres::Client,
    local_hostname: &'a str,
) -> Result<(), crate::Error> {
    if depth > 0 {
        let ids = comments
            .iter()
            .map(|(_, comment)| comment.id)
            .collect::<Vec<_>>();
        let mut replies = get_comments_replies_box(&ids, depth - 1, db, local_hostname).await?;

        for (_, comment) in comments {
            comment.replies = Some(replies.remove(&comment.id).unwrap_or_else(Vec::new));
        }
    }

    Ok(())
}

fn get_comments_replies_box<'a: 'b, 'b>(
    parents: &'b [i64],
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
    Box::pin(get_comments_replies(parents, depth, db, local_hostname))
}

async fn get_comments_replies<'a>(
    parents: &[i64],
    depth: u8,
    db: &tokio_postgres::Client,
    local_hostname: &'a str,
) -> Result<HashMap<i64, Vec<RespPostCommentInfo<'a>>>, crate::Error> {
    use futures::TryStreamExt;

    let stream = crate::query_stream(
        db,
        "SELECT reply.id, reply.author, reply.content_text, reply.created, reply.parent, person.username, person.local, person.ap_id FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE parent = ANY($1::BIGINT[])",
        &[&parents],
    ).await?;

    let mut comments: Vec<_> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id: i64 = row.get(0);
            let content_text: String = row.get(2);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);
            let parent: i64 = row.get(4);

            let author_username: Option<String> = row.get(5);
            let author = author_username.map(move |author_username| {
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
                parent,
                RespPostCommentInfo {
                    id,
                    author,
                    content_text: content_text.into(),
                    created: created.to_rfc3339().into(),
                    replies: None,
                },
            ))
        })
        .try_collect()
        .await?;

    apply_comments_replies(&mut comments, depth, db, local_hostname).await?;

    let mut result = HashMap::new();
    for (parent, comment) in comments {
        result.entry(parent).or_insert(Vec::new()).push(comment);
    }

    Ok(result)
}

async fn get_post_comments<'a>(
    post_id: i64,
    db: &tokio_postgres::Client,
    local_hostname: &'a str,
) -> Result<Vec<RespPostCommentInfo<'a>>, crate::Error> {
    use futures::TryStreamExt;

    let stream = crate::query_stream(
        db,
        "SELECT reply.id, reply.author, reply.content_text, reply.created, person.username, person.local, person.ap_id FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE post=$1 AND parent IS NULL",
        &[&post_id],
    ).await?;

    let mut comments: Vec<_> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id: i64 = row.get(0);
            let content_text: String = row.get(2);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);

            let author_username: Option<String> = row.get(4);
            let author = author_username.map(move |author_username| {
                let author_id: i64 = row.get(1);
                let author_local: bool = row.get(5);
                let author_ap_id: Option<&str> = row.get(6);

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
                    content_text: content_text.into(),
                    created: created.to_rfc3339().into(),
                    replies: None,
                },
            ))
        })
        .try_collect()
        .await?;

    apply_comments_replies(&mut comments, 2, db, local_hostname).await?;

    Ok(comments.into_iter().map(|(_, comment)| comment).collect())
}

async fn route_unstable_posts_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use futures::future::TryFutureExt;

    #[derive(Serialize)]
    struct RespPostInfo<'a> {
        #[serde(flatten)]
        post: &'a RespPostListPost<'a>,
        comments: Vec<RespPostCommentInfo<'a>>,
    }
    let (post_id,) = params;

    let db = ctx.db_pool.get().await?;

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    let (row, comments) = futures::future::try_join(
        db.query_opt(
            "SELECT post.author, post.href, post.content_text, post.title, post.created, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND post.id = $1",
            &[&post_id],
        )
        .map_err(crate::Error::from),
        get_post_comments(post_id, &db, &local_hostname),
    ).await?;

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such post",
        )),
        Some(row) => {
            let href = row.get(1);
            let content_text = row.get(2);
            let title = row.get(3);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(4);
            let community_id = row.get(5);
            let community_name = row.get(6);
            let community_local = row.get(7);
            let community_ap_id = row.get(8);

            let author = match row.get(9) {
                Some(author_username) => {
                    let author_local = row.get(10);
                    Some(RespMinimalAuthorInfo {
                        id: row.get(0),
                        username: Cow::Borrowed(author_username),
                        local: author_local,
                        host: crate::get_actor_host_or_unknown(
                            author_local,
                            row.get(11),
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
                author: author.as_ref(),
                created: &created.to_rfc3339(),
                community: &community,
            };

            let output = RespPostInfo {
                post: &post,
                comments,
            };

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(&output)?.into())?)
        }
    }
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
        content_text: &'a str,
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
        content_text: body.content_text.to_owned(),
        created,
    };

    crate::on_post_add_comment(comment, ctx);

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&serde_json::json!({ "id": reply_id }))?.into())?)
}

async fn route_unstable_comments_get(
    params: (i64,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    #[derive(Serialize)]
    struct RespCommentInfo<'a> {
        #[serde(flatten)]
        base: RespPostCommentInfo<'a>,
        post: Option<RespMinimalPostInfo<'a>>,
    }

    let (comment_id,) = params;

    let db = ctx.db_pool.get().await?;

    let row = db.query_opt(
        "SELECT reply.author, reply.post, reply.content_text, reply.created, reply.local, person.username, person.local, person.ap_id, post.title FROM reply INNER JOIN post ON (reply.post = post.id) LEFT OUTER JOIN person ON (reply.author = person.id) WHERE reply.id = $1",
        &[&comment_id],
    ).await?;

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    match row {
        None => Ok(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            "No such comment",
        )),
        Some(row) => {
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);
            let author = match row.get(5) {
                Some(author_username) => {
                    let author_local = row.get(6);
                    Some(RespMinimalAuthorInfo {
                        id: row.get(0),
                        username: Cow::Borrowed(author_username),
                        local: author_local,
                        host: crate::get_actor_host_or_unknown(
                            author_local,
                            row.get(7),
                            &local_hostname,
                        ),
                    })
                }
                None => None,
            };

            let post = match row.get(8) {
                Some(post_title) => Some(RespMinimalPostInfo {
                    id: row.get(1),
                    title: post_title,
                }),
                None => None,
            };

            let comment = RespCommentInfo {
                base: RespPostCommentInfo {
                    author,
                    content_text: Cow::Borrowed(row.get(2)),
                    created: created.to_rfc3339().into(),
                    id: comment_id,
                    replies: None, // TODO fetch replies
                },
                post,
            };

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(&comment)?.into())?)
        }
    }
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
        content_text: &'a str,
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
        content_text: body.content_text.to_owned(),
        created,
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
        username: &'a str,
        password: String,
        #[serde(default)]
        login: bool,
    }

    let body: UsersCreateBody<'_> = serde_json::from_slice(&body)?;

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
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND community.id IN (SELECT community FROM community_follow WHERE follower=$1) ORDER BY created DESC LIMIT $2",
        values.iter().map(|s| *s as _)
    ).await?;

    let posts = handle_common_posts_list(stream, &ctx.host_url_apub).await?;

    let body = serde_json::to_vec(&posts)?;

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
            let title: &str = row.get(4);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(5);
            let community_id: i64 = row.get(6);
            let community_name: &str = row.get(7);
            let community_local: bool = row.get(8);
            let community_ap_id: Option<&str> = row.get(9);

            let author = author_id.map(|id| {
                let author_name: &str = row.get(10);
                let author_local: bool = row.get(11);
                let author_ap_id: Option<&str> = row.get(12);
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
