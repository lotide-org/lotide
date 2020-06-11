use std::sync::Arc;

pub const ACTIVITY_TYPE: &'static str = "application/activity+json";

pub fn get_local_post_apub_id(post: i64, host_url_apub: &str) -> String {
    format!("{}/posts/{}", host_url_apub, post)
}

pub fn get_local_comment_apub_id(comment: i64, host_url_apub: &str) -> String {
    format!("{}/comments/{}", host_url_apub, comment)
}

pub fn get_local_person_apub_id(person: i64, host_url_apub: &str) -> String {
    format!("{}/users/{}", host_url_apub, person)
}

pub fn get_local_community_apub_id(community: i64, host_url_apub: &str) -> String {
    format!("{}/communities/{}", host_url_apub, community)
}

pub async fn get_or_fetch_user_local_id(
    ap_id: &str,
    db: &tokio_postgres::Client,
    host_url_apub: &str,
    http_client: &crate::HttpClient,
) -> Result<i64, crate::Error> {
    if ap_id.starts_with(host_url_apub) {
        if ap_id[host_url_apub.len()..].starts_with("/users/") {
            Ok(ap_id[(host_url_apub.len() + 7)..].parse()?)
        } else {
            Err(crate::Error::InternalStr(format!(
                "Unrecognized local AP ID: {:?}",
                ap_id
            )))
        }
    } else {
        match db
            .query_opt("SELECT id FROM person WHERE ap_id=$1", &[&ap_id])
            .await?
        {
            Some(row) => Ok(row.get(0)),
            None => {
                // Not known yet, time to fetch

                let res = crate::res_to_error(
                    http_client
                        .request(
                            hyper::Request::get(ap_id)
                                .header(hyper::header::ACCEPT, ACTIVITY_TYPE)
                                .body(Default::default())?,
                        )
                        .await?,
                )
                .await?;

                println!("{:?}", res);

                let body = hyper::body::to_bytes(res.into_body()).await?;

                let person: activitystreams::ext::Ext<
                    activitystreams::actor::Person,
                    activitystreams::actor::properties::ApActorProperties,
                > = serde_json::from_slice(&body)?;

                let username = person
                    .as_ref()
                    .get_name_xsd_string()
                    .map(|x| x.as_str())
                    .unwrap_or("");
                let inbox = person.extension.inbox.as_str();

                Ok(db.query_one(
                    "INSERT INTO person (username, local, created_local, ap_id, ap_inbox) VALUES ($1, FALSE, localtimestamp, $2, $3) RETURNING id",
                    &[&username, &ap_id, &inbox],
                ).await?.get(0))
            }
        }
    }
}

pub async fn send_community_follow(
    community: i64,
    local_follower: i64,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let (community_ap_id, community_inbox): (String, String) = {
        let db = ctx.db_pool.get().await?;

        let row = db
            .query_one(
                "SELECT local, ap_id, ap_inbox FROM community WHERE id=$1",
                &[&community],
            )
            .await?;
        let local = row.get(0);
        if local {
            // no need to send follows to ourself
            return Ok(());
        } else {
            let ap_id = row.get(1);
            let ap_inbox = row.get(2);

            (if let Some(ap_id) = ap_id {
                if let Some(ap_inbox) = ap_inbox {
                    Some((ap_id, ap_inbox))
                } else {
                    None
                }
            } else {
                None
            })
            .ok_or_else(|| {
                crate::Error::InternalStr(format!("Missing apub info for community {}", community))
            })?
        }
    };

    let mut follow = activitystreams::activity::Follow::new();
    follow.object_props.set_id(format!(
        "{}/communities/{}/followers/{}",
        ctx.host_url_apub, community, local_follower
    ))?;

    let person_ap_id = get_local_person_apub_id(local_follower, &ctx.host_url_apub);

    follow.follow_props.set_actor_xsd_any_uri(person_ap_id)?;

    follow
        .follow_props
        .set_object_xsd_any_uri(community_ap_id.as_ref())?;
    follow.object_props.set_to_xsd_any_uri(community_ap_id)?;

    println!("{:?}", follow);

    let body = serde_json::to_vec(&follow)?.into();

    let req = hyper::Request::post(community_inbox)
        .header(hyper::header::CONTENT_TYPE, ACTIVITY_TYPE)
        .body(body)?;

    let res = crate::res_to_error(ctx.http_client.request(req).await?).await?;

    println!("{:?}", res);

    Ok(())
}

pub fn spawn_announce_community_post(post: &crate::PostInfo<'_>, ctx: Arc<crate::RouteContext>) {
    // since post is borrowed, we can't move it
    // so we convert it to AP form before spawning
    match local_community_post_to_announce_ap(post, &ctx.host_url_apub) {
        Err(err) => {
            eprintln!("Failed to create Announce: {:?}", err);
        }
        Ok(announce) => {
            crate::spawn_task(send_to_community_followers(post.community, announce, ctx));
        }
    }
}

pub async fn announce_community_comment(
    comment: crate::CommentInfo,
    post_ap_id: String,
    parent_ap_id: Option<String>,
    community: i64,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let announce = local_community_comment_to_announce_ap(
        &comment,
        &post_ap_id,
        &parent_ap_id,
        community,
        &ctx.host_url_apub,
    )?;

    send_to_community_followers(community, announce, ctx).await
}

pub async fn send_community_follow_accept(
    local_community: i64,
    follower: i64,
    follow: activitystreams::activity::Follow,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let community_ap_id = get_local_community_apub_id(local_community, &ctx.host_url_apub);

    let follower_inbox = {
        let db = ctx.db_pool.get().await?;

        let row = db
            .query_one(
                "SELECT local, ap_inbox FROM person WHERE id=$1",
                &[&follower],
            )
            .await?;

        let local = row.get(0);
        if local {
            // Shouldn't happen, but fine to ignore it
            return Ok(());
        } else {
            let ap_inbox: Option<String> = row.get(1);

            ap_inbox.ok_or_else(|| {
                crate::Error::InternalStr(format!("Missing apub info for user {}", follower))
            })?
        }
    };

    let mut accept = activitystreams::activity::Accept::new();

    accept.accept_props.set_actor_xsd_any_uri(community_ap_id)?;
    accept.accept_props.set_object_base_box(follow)?;

    println!("{:?}", accept);

    let body = serde_json::to_vec(&accept)?.into();

    let req = hyper::Request::post(follower_inbox)
        .header(hyper::header::CONTENT_TYPE, ACTIVITY_TYPE)
        .body(body)?;

    let res = crate::res_to_error(ctx.http_client.request(req).await?).await?;

    println!("{:?}", res);

    Ok(())
}

pub fn post_to_ap(
    post: &crate::PostInfo<'_>,
    community_ap_id: &str,
    host_url_apub: &str,
) -> Result<activitystreams::BaseBox, crate::Error> {
    use std::convert::TryInto;

    match post.href {
        Some(href) => {
            let mut post_ap = activitystreams::object::Page::new();

            post_ap
                .as_mut()
                .set_id(get_local_post_apub_id(post.id, &host_url_apub))?
                .set_attributed_to_xsd_any_uri(get_local_person_apub_id(
                    post.author.unwrap(),
                    &host_url_apub,
                ))?
                .set_url_xsd_any_uri(href)?
                .set_summary_xsd_string(post.title)?
                .set_published(post.created.clone())?
                .set_to_xsd_any_uri(community_ap_id)?;

            if let Some(content) = post.content_text {
                post_ap.as_mut().set_content_xsd_string(content)?;
            }

            Ok(post_ap.try_into()?)
        }
        None => {
            let mut post_ap = activitystreams::object::Note::new();

            post_ap
                .as_mut()
                .set_id(get_local_post_apub_id(post.id, &host_url_apub))?
                .set_attributed_to_xsd_any_uri(get_local_person_apub_id(
                    post.author.unwrap(),
                    &host_url_apub,
                ))?
                .set_content_xsd_string(post.content_text.unwrap_or(""))?
                .set_summary_xsd_string(post.title)?
                .set_published(post.created.clone())?
                .set_to_xsd_any_uri(community_ap_id)?;

            Ok(post_ap.try_into()?)
        }
    }
}

pub fn local_comment_to_ap(
    comment: &crate::CommentInfo,
    post_ap_id: &str,
    parent_ap_id: Option<&str>,
    community_ap_id: &str,
    host_url_apub: &str,
) -> Result<activitystreams::object::Note, crate::Error> {
    let mut obj = activitystreams::object::Note::new();

    obj.as_mut()
        .set_id(get_local_comment_apub_id(comment.id, &host_url_apub))?
        .set_attributed_to_xsd_any_uri(get_local_person_apub_id(
            comment.author.unwrap(),
            &host_url_apub,
        ))?
        .set_published(comment.created.clone())?
        .set_in_reply_to_xsd_any_uri(parent_ap_id.unwrap_or(post_ap_id))?
        .set_to_xsd_any_uri(community_ap_id)?
        .set_content_xsd_string(comment.content_text.to_owned())?;

    Ok(obj)
}

pub fn local_community_post_to_announce_ap(
    post: &crate::PostInfo<'_>,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Announce, crate::Error> {
    let community_ap_id = get_local_community_apub_id(post.community, host_url_apub);
    let post_ap = post_to_ap(post, &community_ap_id, host_url_apub)?;

    let mut announce = activitystreams::activity::Announce::new();

    announce.object_props.set_id(format!(
        "{}/communities/{}/posts/{}/announce",
        host_url_apub, post.community, post.id
    ))?;

    announce
        .announce_props
        .set_actor_xsd_any_uri(community_ap_id)?;
    announce.announce_props.set_object_base_box(post_ap)?;

    Ok(announce)
}

pub fn local_community_comment_to_announce_ap(
    comment: &crate::CommentInfo,
    post_ap_id: &str,
    parent_ap_id: &Option<String>,
    community: i64,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Announce, crate::Error> {
    let community_ap_id = get_local_community_apub_id(community, host_url_apub);
    let comment_ap = local_comment_to_ap(
        comment,
        post_ap_id,
        parent_ap_id.as_deref(),
        &community_ap_id,
        host_url_apub,
    )?;

    let mut announce = activitystreams::activity::Announce::new();

    announce.object_props.set_id(format!(
        "{}/communities/{}/comments/{}/announce",
        host_url_apub, community, comment.id
    ))?;

    announce
        .announce_props
        .set_actor_xsd_any_uri(community_ap_id)?;
    announce.announce_props.set_object_base_box(comment_ap)?;

    Ok(announce)
}

pub async fn send_post_to_community(
    post: crate::PostInfo<'_>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let (community_ap_id, community_inbox): (String, String) = {
        let db = ctx.db_pool.get().await?;

        let row = db
            .query_one(
                "SELECT local, ap_id, ap_inbox FROM community WHERE id=$1",
                &[&post.community],
            )
            .await?;
        let local = row.get(0);
        if local {
            // no need to send posts for local communities
            return Ok(());
        } else {
            let ap_id = row.get(1);
            let ap_inbox = row.get(2);

            (if let Some(ap_id) = ap_id {
                if let Some(ap_inbox) = ap_inbox {
                    Some((ap_id, ap_inbox))
                } else {
                    None
                }
            } else {
                None
            })
            .ok_or_else(|| {
                crate::Error::InternalStr(format!(
                    "Missing apub info for community {}",
                    post.community
                ))
            })?
        }
    };

    let post_ap = post_to_ap(&post, &community_ap_id, &ctx.host_url_apub)?;

    let mut create = activitystreams::activity::Create::new();
    create.create_props.set_object_base_box(post_ap)?;
    create
        .create_props
        .set_actor_xsd_any_uri(get_local_person_apub_id(
            post.author.unwrap(),
            &ctx.host_url_apub,
        ))?;

    let body = serde_json::to_vec(&create)?.into();

    let req = hyper::Request::post(community_inbox)
        .header(hyper::header::CONTENT_TYPE, ACTIVITY_TYPE)
        .body(body)?;

    let res = crate::res_to_error(ctx.http_client.request(req).await?).await?;

    println!("{:?}", res);

    Ok(())
}

pub async fn send_comment_to_community(
    comment: crate::CommentInfo,
    community_ap_id: &str,
    community_ap_inbox: &str,
    post_ap_id: String,
    parent_ap_id: Option<String>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let comment_ap = local_comment_to_ap(
        &comment,
        &post_ap_id,
        parent_ap_id.as_deref(),
        &community_ap_id,
        &ctx.host_url_apub,
    )?;

    let mut create = activitystreams::activity::Create::new();
    create.create_props.set_object_base_box(comment_ap)?;
    create
        .create_props
        .set_actor_xsd_any_uri(get_local_person_apub_id(
            comment.author.unwrap(),
            &ctx.host_url_apub,
        ))?;

    let body = serde_json::to_vec(&create)?.into();

    let req = hyper::Request::post(community_ap_inbox)
        .header(hyper::header::CONTENT_TYPE, ACTIVITY_TYPE)
        .body(body)?;

    let res = crate::res_to_error(ctx.http_client.request(req).await?).await?;

    println!("{:?}", res);

    Ok(())
}

async fn send_to_community_followers(
    community_id: i64,
    announce: activitystreams::activity::Announce,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    use futures::future::{FutureExt, TryFutureExt};
    use futures::stream::{StreamExt, TryStreamExt};

    let db = ctx.db_pool.get().await?;

    let values: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&community_id];

    let stream = db.query_raw(
        "SELECT ap_inbox FROM community_follow, person WHERE person.id = community_follow.follower AND person.local = FALSE AND community = $1",
        values.iter().map(|s| *s as _)
    ).await?;

    let inboxes: std::collections::HashSet<String> =
        stream.map_ok(|row| row.get(0)).try_collect().await?;

    let body: bytes::Bytes = serde_json::to_vec(&announce)?.into();

    let requests: futures::stream::FuturesUnordered<_> = inboxes
        .into_iter()
        .filter_map(
            |inbox| match hyper::Request::post(inbox).body(body.clone().into()) {
                Err(err) => {
                    eprintln!("Failed to construct inbox post: {:?}", err);

                    None
                }
                Ok(req) => Some(req),
            },
        )
        .map(|req| {
            ctx.http_client
                .request(req)
                .map_err(crate::Error::from)
                .and_then(crate::res_to_error)
                .map(|res| {
                    if let Err(err) = res {
                        eprintln!("Delivery failed: {:?}", err);
                    }
                })
        })
        .collect();

    requests.collect::<()>().await;

    Ok(())
}

pub async fn handle_recieved_object(
    community_local_id: i64,
    object_id: &str,
    obj: activitystreams::object::ObjectBox,
    db: &tokio_postgres::Client,
    host_url_apub: &str,
    http_client: &crate::HttpClient,
) -> Result<(), crate::Error> {
    println!("recieved object: {:?}", obj);

    match obj.kind() {
        Some("Page") => {
            let obj: activitystreams::object::Page = obj.into_concrete().unwrap();
            let title = obj
                .as_ref()
                .get_summary_xsd_string()
                .map(|x| x.as_str())
                .unwrap_or("");
            let href = obj.as_ref().get_url_xsd_any_uri().map(|x| x.as_str());
            let content_text = obj.as_ref().get_content_xsd_string().map(|x| x.as_str());
            let created = obj.as_ref().get_published().map(|x| x.as_datetime());
            // TODO support objects here?
            let author = obj
                .as_ref()
                .get_attributed_to_xsd_any_uri()
                .map(|x| x.as_str());
            // TODO verify that this post is intended to go to this community
            // TODO verify this post actually came from the specified author

            handle_recieved_post(
                object_id,
                title,
                href,
                content_text,
                created,
                author,
                community_local_id,
                db,
                host_url_apub,
                http_client,
            )
            .await?;
        }
        Some("Note") => {
            let obj: activitystreams::object::Note = obj.into_concrete().unwrap();
            let content_text = obj.as_ref().get_content_xsd_string().map(|x| x.as_str());
            let created = obj.as_ref().get_published().map(|x| x.as_datetime());
            let author = obj
                .as_ref()
                .get_attributed_to_xsd_any_uri()
                .map(|x| x.as_str());

            if let Some(in_reply_to) = &obj.as_ref().in_reply_to {
                // it's a reply

                handle_recieved_reply(
                    object_id,
                    content_text.unwrap_or(""),
                    created,
                    author,
                    in_reply_to,
                    db,
                    host_url_apub,
                    http_client,
                )
                .await?;
            } else {
                // not a reply, must be a top-level post
                let title = obj
                    .as_ref()
                    .get_summary_xsd_string()
                    .map(|x| x.as_str())
                    .unwrap_or("");

                handle_recieved_post(
                    object_id,
                    title,
                    None,
                    content_text,
                    created,
                    author,
                    community_local_id,
                    db,
                    host_url_apub,
                    http_client,
                )
                .await?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_recieved_post(
    object_id: &str,
    title: &str,
    href: Option<&str>,
    content_text: Option<&str>,
    created: Option<&chrono::DateTime<chrono::FixedOffset>>,
    author: Option<&str>,
    community_local_id: i64,
    db: &tokio_postgres::Client,
    host_url_apub: &str,
    http_client: &crate::HttpClient,
) -> Result<(), crate::Error> {
    let author = match author {
        Some(author) => {
            Some(get_or_fetch_user_local_id(&author, &db, host_url_apub, http_client).await?)
        }
        None => None,
    };

    db.execute(
        "INSERT INTO post (author, href, content_text, title, created, community, local, ap_id) VALUES ($1, $2, $3, $4, COALESCE($5, current_timestamp), $6, FALSE, $7)",
        &[&author, &href, &content_text, &title, &created, &community_local_id, &object_id],
    ).await?;

    Ok(())
}

async fn handle_recieved_reply(
    object_id: &str,
    content_text: &str,
    created: Option<&chrono::DateTime<chrono::FixedOffset>>,
    author: Option<&str>,
    in_reply_to: &activitystreams::object::properties::ObjectPropertiesInReplyToEnum,
    db: &tokio_postgres::Client,
    host_url_apub: &str,
    http_client: &crate::HttpClient,
) -> Result<(), crate::Error> {
    let author = match author {
        Some(author) => {
            Some(get_or_fetch_user_local_id(&author, &db, host_url_apub, http_client).await?)
        }
        None => None,
    };

    let in_reply_to = match in_reply_to {
        activitystreams::object::properties::ObjectPropertiesInReplyToEnum::Term(term) => {
            either::Either::Left(std::iter::once(term))
        }
        activitystreams::object::properties::ObjectPropertiesInReplyToEnum::Array(terms) => {
            either::Either::Right(terms.iter())
        }
    };

    let mut reply_to_local_posts = Vec::new();
    let mut reply_to_remotes = Vec::new();

    let local_post_id_prefix = format!("{}/posts/", host_url_apub);

    for term in in_reply_to {
        if let activitystreams::object::properties::ObjectPropertiesInReplyToTermEnum::XsdAnyUri(
            term_ap_id,
        ) = term
        {
            let term_ap_id = term_ap_id.as_str();
            if term_ap_id.starts_with(&local_post_id_prefix) {
                let local_term_id = &term_ap_id[local_post_id_prefix.len()..];
                if let Ok(local_term_id) = local_term_id.parse() {
                    reply_to_local_posts.push(local_term_id);
                    continue;
                }
            }

            reply_to_remotes.push(term_ap_id);
        }
    }

    let reply_to_remotes_local_ids: Vec<i64> = if reply_to_remotes.is_empty() {
        vec![]
    } else {
        let rows = db
            .query(
                "SELECT id FROM post WHERE ap_id = ANY($1::TEXT[])",
                &[&reply_to_remotes],
            )
            .await?;

        rows.into_iter().map(|row| row.get(0)).collect()
    };

    let post = reply_to_remotes_local_ids
        .into_iter()
        .chain(reply_to_local_posts)
        .max();
    if let Some(post) = post {
        db.execute(
            "INSERT INTO reply (post, author, content_text, created, local, ap_id) VALUES ($1, $2, $3, COALESCE($4, current_timestamp), FALSE, $5)",
            &[&post, &author, &content_text, &created, &object_id],
        ).await?;
    }

    Ok(())
}
