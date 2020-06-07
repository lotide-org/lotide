use std::sync::Arc;

pub const ACTIVITY_TYPE: &'static str = "application/activity+json";

pub fn get_local_post_apub_id(post: i64, host_url_apub: &str) -> String {
    format!("{}/posts/{}", host_url_apub, post)
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

pub fn post_to_ap(
    post: &crate::PostInfo<'_>,
    community_ap_id: &str,
    host_url_apub: &str,
) -> Result<activitystreams::object::Page, crate::Error> {
    let mut post_ap = activitystreams::object::Page::new();

    post_ap
        .as_mut()
        .set_id(get_local_post_apub_id(post.id, &host_url_apub))?
        .set_attributed_to_xsd_any_uri(get_local_person_apub_id(
            post.author.unwrap(),
            &host_url_apub,
        ))?
        .set_url_xsd_any_uri(post.href)?
        .set_summary_xsd_string(post.title)?
        .set_published(post.created.clone())?
        .set_to_xsd_any_uri(community_ap_id)?;

    Ok(post_ap)
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
