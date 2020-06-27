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
                            .with_handler_async("DELETE", route_unstable_posts_delete)
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
    let ap_inbox = group.extension.get_inbox().as_str();
    let ap_shared_inbox = group
        .extension
        .get_endpoints()
        .and_then(|endpoints| endpoints.get_shared_inbox())
        .map(|x| x.as_str());

    if let Some(name) = name {
        let row = db.query_one(
            "INSERT INTO community (name, local, ap_id, ap_inbox, ap_shared_inbox) VALUES ($1, FALSE, $2, $3, $4) ON CONFLICT (ap_id) DO UPDATE SET name=$1, ap_inbox=$3, ap_shared_inbox=$4 RETURNING id",
            &[&name.as_str(), &query, &ap_inbox, &ap_shared_inbox],
        )
        .await?;

        let new_id: i64 = row.get(0);

        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&serde_json::json!([{ "id": new_id }]))?.into())?)
    } else {
        Ok(hyper::Response::builder()
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&serde_json::json!([]))?.into())?)
    }
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

async fn route_unstable_posts_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let limit: i64 = 10;

    let stream = db.query_raw(
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, post.content_html, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND deleted=FALSE ORDER BY created DESC LIMIT $1",
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
                crate::on_community_add_post(
                    post.community,
                    post.id,
                    &crate::apub_util::get_local_post_apub_id(post.id, &ctx.host_url_apub),
                    ctx,
                );
            } else {
                crate::apub_util::send_local_post_to_community(post, ctx).await?;
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
        "SELECT reply.id, reply.author, reply.content_text, reply.created, reply.parent, reply.content_html, person.username, person.local, person.ap_id, reply.deleted FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE parent = ANY($1::BIGINT[]) ORDER BY created DESC",
        &[&parents],
    ).await?;

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
        "SELECT reply.id, reply.author, reply.content_text, reply.created, reply.content_html, person.username, person.local, person.ap_id, reply.deleted FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE post=$1 AND parent IS NULL ORDER BY created DESC",
        &[&post_id],
    ).await?;

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
            "SELECT post.author, post.href, post.content_text, post.title, post.created, post.content_html, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND post.id = $1",
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
            };

            Ok(hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(&output)?.into())?)
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

            db.execute("UPDATE post SET href=NULL, title='[deleted]', content_text='[deleted]', deleted=TRUE WHERE id=$1", &[&post_id]).await?;

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
                        crate::spawn_task(crate::apub_util::forward_to_community_followers(
                            community,
                            serde_json::to_vec(&delete_ap)?,
                            ctx,
                        ));
                    } else {
                        let community_inbox: Option<&str> = row.get(2);

                        if let Some(community_inbox) = community_inbox {
                            let mut req = hyper::Request::post(community_inbox)
                                .header(
                                    hyper::header::CONTENT_TYPE,
                                    crate::apub_util::ACTIVITY_TYPE,
                                )
                                .body(hyper::Body::from(serde_json::to_vec(&delete_ap)?))?;

                            if let Ok(path_and_query) =
                                crate::apub_util::get_path_and_query(&community_inbox)
                            {
                                let user_privkey =
                                    crate::apub_util::fetch_or_create_local_user_privkey(user, &db)
                                        .await?;
                                req.headers_mut()
                                    .insert(hyper::header::DATE, crate::apub_util::now_http_date());

                                let key_id = crate::apub_util::get_local_person_pubkey_apub_id(
                                    user,
                                    &ctx.host_url_apub,
                                );

                                let signature = hancock::Signature::create_legacy(
                                    &key_id,
                                    &hyper::Method::POST,
                                    &path_and_query,
                                    req.headers(),
                                    |src| crate::apub_util::do_sign(&user_privkey, &src),
                                )?;

                                req.headers_mut().insert("Signature", signature.to_header());
                            }

                            crate::res_to_error(ctx.http_client.request(req).await?).await?;
                        }
                    }
                }

                Ok(())
            });

            Ok(crate::empty_response())
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
        "SELECT reply.author, reply.post, reply.content_text, reply.created, reply.local, reply.content_html, person.username, person.local, person.ap_id, post.title, reply.deleted FROM reply INNER JOIN post ON (reply.post = post.id) LEFT OUTER JOIN person ON (reply.author = person.id) WHERE reply.id = $1",
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

            let comment = RespCommentInfo {
                base: RespPostCommentInfo {
                    author,
                    content_text: row.get::<_, Option<&str>>(2).map(Cow::Borrowed),
                    content_html: row.get::<_, Option<&str>>(5).map(Cow::Borrowed),
                    created: created.to_rfc3339().into(),
                    deleted: row.get(10),
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

                    let local = row.get(0);
                    if local {
                        crate::spawn_task(crate::apub_util::forward_to_community_followers(
                            community,
                            serde_json::to_vec(&delete_ap)?,
                            ctx,
                        ));
                    } else {
                        let community_inbox: Option<&str> = row.get(2);

                        if let Some(community_inbox) = community_inbox {
                            let mut req = hyper::Request::post(community_inbox)
                                .header(
                                    hyper::header::CONTENT_TYPE,
                                    crate::apub_util::ACTIVITY_TYPE,
                                )
                                .body(hyper::Body::from(serde_json::to_vec(&delete_ap)?))?;

                            if let Ok(path_and_query) =
                                crate::apub_util::get_path_and_query(&community_inbox)
                            {
                                let user_privkey =
                                    crate::apub_util::fetch_or_create_local_user_privkey(user, &db)
                                        .await?;
                                req.headers_mut()
                                    .insert(hyper::header::DATE, crate::apub_util::now_http_date());

                                let key_id = crate::apub_util::get_local_person_pubkey_apub_id(
                                    user,
                                    &ctx.host_url_apub,
                                );

                                let signature = hancock::Signature::create_legacy(
                                    &key_id,
                                    &hyper::Method::POST,
                                    &path_and_query,
                                    req.headers(),
                                    |src| crate::apub_util::do_sign(&user_privkey, &src),
                                )?;

                                req.headers_mut().insert("Signature", signature.to_header());
                            }

                            crate::res_to_error(ctx.http_client.request(req).await?).await?;
                        }
                    }
                }

                Ok(())
            });

            Ok(crate::empty_response())
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
        "SELECT post.id, post.author, post.href, post.content_text, post.title, post.created, post.content_html, community.id, community.name, community.local, community.ap_id, person.username, person.local, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.community = community.id AND deleted=FALSE AND community.id IN (SELECT community FROM community_follow WHERE follower=$1) ORDER BY created DESC LIMIT $2",
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
