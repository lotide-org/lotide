use crate::ThingLocalRef;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

pub const ACTIVITY_TYPE: &'static str = "application/activity+json";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKey<'a> {
    pub id: Cow<'a, str>,
    pub owner: Cow<'a, str>,
    pub public_key_pem: Cow<'a, str>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeyExtension<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(borrow)]
    pub public_key: Option<PublicKey<'a>>,
}

impl<'a, T: activitystreams::actor::Actor> activitystreams::ext::Extension<T>
    for PublicKeyExtension<'a>
{
}

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

pub fn get_local_person_pubkey_apub_id(person: i64, host_url_apub: &str) -> String {
    format!(
        "{}#main-key",
        get_local_person_apub_id(person, host_url_apub)
    )
}

pub fn get_local_community_pubkey_apub_id(community: i64, host_url_apub: &str) -> String {
    format!(
        "{}#main-key",
        get_local_community_apub_id(community, host_url_apub)
    )
}

pub fn get_local_follow_apub_id(community: i64, follower: i64, host_url_apub: &str) -> String {
    format!(
        "{}/followers/{}",
        get_local_community_apub_id(community, host_url_apub),
        follower
    )
}

pub fn now_http_date() -> hyper::header::HeaderValue {
    chrono::offset::Utc::now()
        .format("%a, %d %b %Y %T GMT")
        .to_string()
        .parse()
        .unwrap()
}

pub fn do_sign(
    key: &openssl::pkey::PKey<openssl::pkey::Private>,
    src: &[u8],
) -> Result<Vec<u8>, openssl::error::ErrorStack> {
    let mut signer = openssl::sign::Signer::new(openssl::hash::MessageDigest::sha256(), &key)?;
    signer.update(&src)?;
    Ok(signer.sign_to_vec()?)
}

pub fn do_verify(
    key: &openssl::pkey::PKey<openssl::pkey::Public>,
    src: &[u8],
    sig: &[u8],
) -> Result<bool, openssl::error::ErrorStack> {
    let mut verifier = openssl::sign::Verifier::new(openssl::hash::MessageDigest::sha256(), &key)?;
    verifier.update(&src)?;
    Ok(verifier.verify(sig)?)
}

pub enum ActorLocalInfo {
    User {
        id: i64,
        public_key: Option<Vec<u8>>,
    },
    Community {
        id: i64,
        public_key: Option<Vec<u8>>,
    },
}

impl ActorLocalInfo {
    pub fn public_key(&self) -> Option<&[u8]> {
        match self {
            ActorLocalInfo::User { public_key, .. } => public_key.as_deref(),
            ActorLocalInfo::Community { public_key, .. } => public_key.as_deref(),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Incoming object failed containment check")]
pub struct NotContained;

pub fn require_containment(object_id: &url::Url, actor_id: &url::Url) -> Result<(), NotContained> {
    if object_id.host() == actor_id.host() && object_id.port() == actor_id.port() {
        Ok(())
    } else {
        Err(NotContained)
    }
}

pub async fn fetch_actor(
    ap_id: &str,
    db: &tokio_postgres::Client,
    http_client: &crate::HttpClient,
) -> Result<ActorLocalInfo, crate::Error> {
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

    let body = hyper::body::to_bytes(res.into_body()).await?;

    let actor: activitystreams::actor::ActorBox = serde_json::from_slice(&body)?;

    match actor.kind() {
        Some("Person") => {
            let person: activitystreams::ext::Ext<
                activitystreams::ext::Ext<
                    activitystreams::actor::Person,
                    activitystreams::actor::properties::ApActorProperties,
                >,
                crate::apub_util::PublicKeyExtension<'_>,
            > = serde_json::from_slice(&body)?;

            let username = person
                .base
                .extension
                .get_preferred_username()
                .map(|x| x.as_str())
                .or_else(|| person.as_ref().get_name_xsd_string().map(|x| x.as_str()))
                .unwrap_or("");
            let inbox = person.base.extension.inbox.as_str();
            let shared_inbox = person
                .base
                .extension
                .get_endpoints()
                .and_then(|endpoints| endpoints.get_shared_inbox())
                .map(|url| url.as_str());
            let public_key = person
                .extension
                .public_key
                .as_ref()
                .map(|key| key.public_key_pem.as_bytes());

            let id = db.query_one(
                "INSERT INTO person (username, local, created_local, ap_id, ap_inbox, ap_shared_inbox, public_key) VALUES ($1, FALSE, localtimestamp, $2, $3, $4, $5) ON CONFLICT (ap_id) DO UPDATE SET ap_inbox=$3, ap_shared_inbox=$4, public_key=$5 RETURNING id",
                &[&username, &ap_id, &inbox, &shared_inbox, &public_key],
            ).await?.get(0);

            Ok(ActorLocalInfo::User {
                id,
                public_key: public_key.map(|x| x.to_owned()),
            })
        }
        Some("Group") => {
            let group: activitystreams::ext::Ext<
                activitystreams::ext::Ext<
                    activitystreams::actor::Group,
                    activitystreams::actor::properties::ApActorProperties,
                >,
                crate::apub_util::PublicKeyExtension<'_>,
            > = serde_json::from_slice(&body)?;

            let name = group
                .as_ref()
                .get_name_xsd_string()
                .map(|x| x.as_str())
                .unwrap_or("");
            let inbox = group.base.extension.inbox.as_str();
            let shared_inbox = group
                .base
                .extension
                .get_endpoints()
                .and_then(|endpoints| endpoints.get_shared_inbox())
                .map(|url| url.as_str());
            let public_key = group
                .extension
                .public_key
                .as_ref()
                .map(|key| key.public_key_pem.as_bytes());

            let id = db.query_one(
                "INSERT INTO community (name, local, ap_id, ap_inbox, ap_shared_inbox, public_key) VALUES ($1, FALSE, $2, $3, $4, $5) ON CONFLICT (ap_id) DO UPDATE SET ap_inbox=$3, ap_shared_inbox=$4, public_key=$5 RETURNING id",
                &[&name, &ap_id, &inbox, &shared_inbox, &public_key],
            ).await?.get(0);

            Ok(ActorLocalInfo::Community {
                id,
                public_key: public_key.map(|x| x.to_owned()),
            })
        }
        _ => Err(crate::Error::InternalStrStatic("Unrecognized actor type")),
    }
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

                let actor = fetch_actor(ap_id, db, http_client).await?;

                if let ActorLocalInfo::User { id, .. } = actor {
                    Ok(id)
                } else {
                    Err(crate::Error::InternalStrStatic("Not a Person"))
                }
            }
        }
    }
}

pub async fn fetch_or_create_local_user_privkey(
    user: i64,
    db: &tokio_postgres::Client,
) -> Result<openssl::pkey::PKey<openssl::pkey::Private>, crate::Error> {
    let row = db
        .query_one(
            "SELECT private_key, local FROM person WHERE id=$1",
            &[&user],
        )
        .await?;
    match row.get(0) {
        Some(bytes) => Ok(openssl::pkey::PKey::private_key_from_pem(bytes)?),
        None => {
            let local: bool = row.get(1);
            if !local {
                Err(crate::Error::InternalStr(format!(
                    "Won't create privkey for user {} because they aren't local",
                    user
                )))
            } else {
                let rsa = openssl::rsa::Rsa::generate(crate::KEY_BITS)?;
                let private_key = rsa.private_key_to_pem()?;
                let public_key = rsa.public_key_to_pem()?;

                db.execute(
                    "UPDATE person SET private_key=$1, public_key=$2 WHERE id=$3",
                    &[&private_key, &public_key, &user],
                )
                .await?;

                Ok(openssl::pkey::PKey::from_rsa(rsa)?)
            }
        }
    }
}

pub async fn fetch_or_create_local_community_privkey(
    community: i64,
    db: &tokio_postgres::Client,
) -> Result<openssl::pkey::PKey<openssl::pkey::Private>, crate::Error> {
    let row = db
        .query_one(
            "SELECT private_key, local FROM community WHERE id=$1",
            &[&community],
        )
        .await?;
    match row.get(0) {
        Some(bytes) => Ok(openssl::pkey::PKey::private_key_from_pem(bytes)?),
        None => {
            let local: bool = row.get(1);
            if !local {
                Err(crate::Error::InternalStr(format!(
                    "Won't create privkey for community {} because they aren't local",
                    community,
                )))
            } else {
                let rsa = openssl::rsa::Rsa::generate(crate::KEY_BITS)?;
                let private_key = rsa.private_key_to_pem()?;
                let public_key = rsa.public_key_to_pem()?;

                db.execute(
                    "UPDATE community SET private_key=$1, public_key=$2 WHERE id=$3",
                    &[&private_key, &public_key, &community],
                )
                .await?;

                Ok(openssl::pkey::PKey::from_rsa(rsa)?)
            }
        }
    }
}

pub async fn fetch_or_create_local_actor_privkey(
    actor_ref: crate::ActorLocalRef,
    db: &tokio_postgres::Client,
    host_url_apub: &str,
) -> Result<(openssl::pkey::PKey<openssl::pkey::Private>, String), crate::Error> {
    Ok(match actor_ref {
        crate::ActorLocalRef::Person(id) => (
            fetch_or_create_local_user_privkey(id, db).await?,
            get_local_person_pubkey_apub_id(id, &host_url_apub),
        ),
        crate::ActorLocalRef::Community(id) => (
            fetch_or_create_local_community_privkey(id, db).await?,
            get_local_community_pubkey_apub_id(id, &host_url_apub),
        ),
    })
}

pub fn spawn_enqueue_send_community_follow(
    community: i64,
    local_follower: i64,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let (community_ap_id, community_inbox): (String, String) = {
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
                    crate::Error::InternalStr(format!(
                        "Missing apub info for community {}",
                        community
                    ))
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

        std::mem::drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: (&community_inbox).into(),
            sign_as: Some(crate::ActorLocalRef::Person(local_follower)),
            object: serde_json::to_string(&follow)?.into(),
        })
        .await?;

        Ok(())
    });
}

pub fn local_community_post_announce_ap(
    community_id: i64,
    post_local_id: i64,
    post_ap_id: &str,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Announce, crate::Error> {
    let mut announce = activitystreams::activity::Announce::new();

    let community_ap_id = get_local_community_apub_id(community_id, host_url_apub);

    announce.object_props.set_id(format!(
        "{}/posts/{}/announce",
        community_ap_id, post_local_id,
    ))?;
    announce
        .object_props
        .set_to_xsd_any_uri(format!("{}/followers", community_ap_id))?;
    announce
        .object_props
        .set_cc_xsd_any_uri(activitystreams::public())?;

    announce
        .announce_props
        .set_actor_xsd_any_uri(community_ap_id)?;
    announce.announce_props.set_object_xsd_any_uri(post_ap_id)?;

    Ok(announce)
}

pub fn local_community_comment_announce_ap(
    community_id: i64,
    comment_local_id: i64,
    comment_ap_id: &str,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Announce, crate::Error> {
    let mut announce = activitystreams::activity::Announce::new();

    let community_ap_id = get_local_community_apub_id(community_id, host_url_apub);

    announce.object_props.set_id(format!(
        "{}/comments/{}/announce",
        community_ap_id, comment_local_id,
    ))?;

    announce
        .announce_props
        .set_actor_xsd_any_uri(community_ap_id)?;
    announce
        .announce_props
        .set_object_xsd_any_uri(comment_ap_id)?;

    Ok(announce)
}

pub fn spawn_announce_community_post(
    community: i64,
    post_local_id: i64,
    post_ap_id: &str,
    ctx: Arc<crate::RouteContext>,
) {
    // since post is borrowed, we can't move it
    // so we convert it to AP form before spawning
    match local_community_post_announce_ap(community, post_local_id, post_ap_id, &ctx.host_url_apub)
    {
        Err(err) => {
            eprintln!("Failed to create Announce: {:?}", err);
        }
        Ok(announce) => {
            crate::spawn_task(enqueue_send_to_community_followers(
                community, announce, ctx,
            ));
        }
    }
}

pub fn spawn_announce_community_comment(
    community: i64,
    comment_local_id: i64,
    comment_ap_id: &str,
    ctx: Arc<crate::RouteContext>,
) {
    let announce = local_community_comment_announce_ap(
        community,
        comment_local_id,
        comment_ap_id,
        &ctx.host_url_apub,
    );

    crate::spawn_task(async move {
        let announce = announce?;
        enqueue_send_to_community_followers(community, announce, ctx).await
    });
}

pub fn community_follow_accept_to_ap(
    community_ap_id: &str,
    follower_local_id: i64,
    follow_ap_id: &str,
) -> Result<activitystreams::activity::Accept, crate::Error> {
    let mut accept = activitystreams::activity::Accept::new();

    accept.object_props.set_id(format!(
        "{}/followers/{}/accept",
        community_ap_id, follower_local_id
    ))?;

    accept.accept_props.set_actor_xsd_any_uri(community_ap_id)?;
    accept.accept_props.set_object_xsd_any_uri(follow_ap_id)?;

    Ok(accept)
}

pub fn spawn_enqueue_send_community_follow_accept(
    local_community: i64,
    follower: i64,
    follow: activitystreams::activity::Follow,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let follow_ap_id = follow
            .object_props
            .get_id()
            .ok_or(crate::Error::InternalStrStatic(
                "Missing ID in Follow activity",
            ))?;

        let community_ap_id = get_local_community_apub_id(local_community, &ctx.host_url_apub);

        let follower_inbox = {
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

        let accept =
            community_follow_accept_to_ap(&community_ap_id, follower, follow_ap_id.as_str())?;
        println!("{:?}", accept);

        let body = serde_json::to_string(&accept)?;

        std::mem::drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: (&follower_inbox).into(),
            sign_as: Some(crate::ActorLocalRef::Community(local_community)),
            object: body.into(),
        })
        .await?;

        Ok(())
    });
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
                .set_to_xsd_any_uri(community_ap_id)?
                .set_cc_xsd_any_uri(activitystreams::public())?;

            if let Some(content) = post.content_text {
                post_ap
                    .as_mut()
                    .set_content_xsd_string(content)?
                    .set_media_type(mime::TEXT_PLAIN)?;
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
                .set_media_type(mime::TEXT_PLAIN)?
                .set_summary_xsd_string(post.title)?
                .set_published(post.created.clone())?
                .set_to_xsd_any_uri(community_ap_id)?
                .set_cc_xsd_any_uri(activitystreams::public())?;

            Ok(post_ap.try_into()?)
        }
    }
}

pub fn local_post_to_create_ap(
    post: &crate::PostInfo<'_>,
    community_ap_id: &str,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let post_ap = post_to_ap(&post, &community_ap_id, &host_url_apub)?;

    let mut create = activitystreams::activity::Create::new();
    create
        .create_props
        .set_object_base_box(post_ap)?
        .set_actor_xsd_any_uri(get_local_person_apub_id(
            post.author.unwrap(),
            &host_url_apub,
        ))?;
    create.object_props.set_id(format!(
        "{}/create",
        get_local_post_apub_id(post.id, host_url_apub)
    ))?;

    Ok(create)
}

pub fn local_comment_to_ap(
    comment: &crate::CommentInfo,
    post_ap_id: &str,
    parent_ap_id: Option<&str>,
    parent_or_post_author_ap_id: Option<&str>,
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
        .set_content_xsd_string(comment.content_text.clone().unwrap())?;

    if let Some(parent_or_post_author_ap_id) = parent_or_post_author_ap_id {
        use std::convert::TryInto;

        obj.as_mut()
            .set_to_xsd_any_uri(parent_or_post_author_ap_id)?
            .set_many_cc_xsd_any_uris(vec![
                activitystreams::public(),
                community_ap_id.try_into()?,
            ])?;
    } else {
        obj.as_mut()
            .set_to_xsd_any_uri(community_ap_id)?
            .set_cc_xsd_any_uri(activitystreams::public())?;
    }

    Ok(obj)
}

pub fn spawn_enqueue_send_local_post_to_community(
    post: crate::PostInfoOwned,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let (community_ap_id, community_inbox): (String, String) = {
            let row = db
                .query_one(
                    "SELECT local, ap_id, COALESCE(ap_shared_inbox, ap_inbox) FROM community WHERE id=$1",
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

        let create =
            local_post_to_create_ap(&(&post).into(), &community_ap_id, &ctx.host_url_apub)?;

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: (&community_inbox).into(),
            sign_as: Some(crate::ActorLocalRef::Person(post.author.unwrap())),
            object: serde_json::to_string(&create)?.into(),
        })
        .await?;

        Ok(())
    });
}

pub fn local_post_delete_to_ap(
    post_id: i64,
    author: i64,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Delete, crate::Error> {
    let mut delete = activitystreams::activity::Delete::new();
    let post_ap_id = get_local_post_apub_id(post_id, host_url_apub);
    delete
        .object_props
        .set_id(format!("{}/delete", post_ap_id))?;
    delete
        .delete_props
        .set_actor_xsd_any_uri(get_local_person_apub_id(author, host_url_apub))?;
    delete.delete_props.set_object_xsd_any_uri(post_ap_id)?;

    Ok(delete)
}

pub fn local_comment_delete_to_ap(
    comment_id: i64,
    author: i64,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Delete, crate::Error> {
    let mut delete = activitystreams::activity::Delete::new();
    let comment_ap_id = get_local_comment_apub_id(comment_id, host_url_apub);
    delete
        .object_props
        .set_id(format!("{}/delete", comment_ap_id))?;
    delete
        .delete_props
        .set_actor_xsd_any_uri(get_local_person_apub_id(author, host_url_apub))?;
    delete.delete_props.set_object_xsd_any_uri(comment_ap_id)?;

    Ok(delete)
}

pub fn local_comment_to_create_ap(
    comment: &crate::CommentInfo,
    post_ap_id: &str,
    parent_ap_id: Option<&str>,
    post_or_parent_author_ap_id: Option<&str>,
    community_ap_id: &str,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let comment_ap = local_comment_to_ap(
        &comment,
        &post_ap_id,
        parent_ap_id,
        post_or_parent_author_ap_id,
        &community_ap_id,
        &host_url_apub,
    )?;

    let author = comment.author.unwrap();

    let mut create = activitystreams::activity::Create::new();
    create.object_props.set_id(format!(
        "{}/create",
        get_local_comment_apub_id(comment.id, host_url_apub)
    ))?;
    create.create_props.set_object_base_box(comment_ap)?;
    create
        .create_props
        .set_actor_xsd_any_uri(get_local_person_apub_id(author, &host_url_apub))?;

    Ok(create)
}

pub fn local_post_like_to_ap(
    post_local_id: i64,
    post_ap_id: &str,
    user: i64,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Like, crate::Error> {
    let mut like = activitystreams::activity::Like::new();
    like.like_props.set_object_xsd_any_uri(post_ap_id)?;
    like.like_props
        .set_actor_xsd_any_uri(crate::apub_util::get_local_person_apub_id(
            user,
            &host_url_apub,
        ))?;
    like.object_props.set_id(format!(
        "{}/likes/{}",
        crate::apub_util::get_local_post_apub_id(post_local_id, &host_url_apub),
        user
    ))?;

    Ok(like)
}

pub fn local_comment_like_to_ap(
    comment_local_id: i64,
    comment_ap_id: &str,
    user: i64,
    host_url_apub: &str,
) -> Result<activitystreams::activity::Like, crate::Error> {
    let mut like = activitystreams::activity::Like::new();
    like.like_props.set_object_xsd_any_uri(comment_ap_id)?;
    like.like_props
        .set_actor_xsd_any_uri(crate::apub_util::get_local_person_apub_id(
            user,
            &host_url_apub,
        ))?;
    like.object_props.set_id(format!(
        "{}/likes/{}",
        crate::apub_util::get_local_comment_apub_id(comment_local_id, &host_url_apub),
        user
    ))?;

    Ok(like)
}

pub fn spawn_enqueue_send_comment_to_community(
    comment: crate::CommentInfo,
    community_ap_id: &str,
    community_ap_inbox: String,
    post_ap_id: String,
    parent_ap_id: Option<String>,
    post_or_parent_author_ap_id: Option<&str>,
    ctx: Arc<crate::RouteContext>,
) {
    let create = local_comment_to_create_ap(
        &comment,
        &post_ap_id,
        parent_ap_id.as_deref(),
        post_or_parent_author_ap_id,
        &community_ap_id,
        &ctx.host_url_apub,
    );

    let author = comment.author.unwrap();

    crate::spawn_task(async move {
        let create = create?;

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: community_ap_inbox.into(),
            sign_as: Some(crate::ActorLocalRef::Person(author)),
            object: serde_json::to_string(&create)?.into(),
        })
        .await?;

        Ok(())
    });
}

pub async fn enqueue_forward_to_community_followers(
    community_id: i64,
    body: String,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    ctx.enqueue_task(&crate::tasks::DeliverToFollowers {
        actor: crate::ActorLocalRef::Community(community_id),
        sign: false,
        object: body,
    })
    .await
}

async fn enqueue_send_to_community_followers(
    community_id: i64,
    announce: activitystreams::activity::Announce,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    ctx.enqueue_task(&crate::tasks::DeliverToFollowers {
        actor: crate::ActorLocalRef::Community(community_id),
        sign: true,
        object: serde_json::to_string(&announce)?,
    })
    .await
}

pub fn maybe_get_local_user_id_from_uri(uri: &str, host_url_apub: &str) -> Option<i64> {
    if uri.starts_with(&host_url_apub) {
        let path = &uri[host_url_apub.len()..];
        if path.starts_with("/communities/") {
            if let Ok(local_community_id) = path[13..].parse() {
                Some(local_community_id)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}

pub async fn handle_recieved_object_for_local_community(
    obj: activitystreams::object::ObjectBox,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    use std::convert::TryInto;

    let (to, in_reply_to, obj_id, obj) = match obj.kind() {
        Some("Page") => {
            let obj = obj
                .into_concrete::<activitystreams::object::Page>()
                .unwrap();
            (
                obj.object_props.to.clone(),
                None,
                obj.object_props.id.clone(),
                obj.try_into().unwrap(),
            )
        }
        Some("Note") => {
            let obj = obj
                .into_concrete::<activitystreams::object::Note>()
                .unwrap();
            (
                obj.object_props.to.clone(),
                obj.object_props.in_reply_to.clone(),
                obj.object_props.id.clone(),
                obj.try_into().unwrap(),
            )
        }
        _ => (None, None, None, obj),
    };

    let local_community_id = match to {
        None => None,
        Some(activitystreams::object::properties::ObjectPropertiesToEnum::Term(term)) => match term
        {
            activitystreams::object::properties::ObjectPropertiesToTermEnum::XsdAnyUri(uri) => {
                maybe_get_local_user_id_from_uri(uri.as_str(), &ctx.host_url_apub)
            }
            _ => None,
        },
        Some(activitystreams::object::properties::ObjectPropertiesToEnum::Array(terms)) => terms
            .iter()
            .filter_map(|term| match term {
                activitystreams::object::properties::ObjectPropertiesToTermEnum::XsdAnyUri(uri) => {
                    maybe_get_local_user_id_from_uri(uri.as_str(), &ctx.host_url_apub)
                }
                _ => None,
            })
            .next(),
    };

    if let Some(local_community_id) = local_community_id {
        handle_recieved_object_for_community(local_community_id, true, obj, ctx).await?;
    } else {
        // not to a community, but might still match as a reply
        if let Some(in_reply_to) = in_reply_to {
            if let Some(obj_id) = obj_id {
                if let Ok(obj) = obj.into_concrete::<activitystreams::object::Note>() {
                    // TODO deduplicate this?

                    let content = obj.as_ref().get_content_xsd_string().map(|x| x.as_str());
                    let media_type = obj.as_ref().get_media_type().map(|x| x.as_ref());
                    let created = obj.as_ref().get_published().map(|x| x.as_datetime());
                    let author = obj.as_ref().get_attributed_to_xsd_any_uri();

                    if let Some(author) = author {
                        require_containment(obj_id.as_url(), author.as_url())?;
                    }

                    let author = author.map(|x| x.as_str());

                    handle_recieved_reply(
                        obj_id.as_ref(),
                        content.unwrap_or(""),
                        media_type,
                        created,
                        author,
                        &in_reply_to,
                        ctx,
                    )
                    .await?;
                }
            }
        }
    }

    Ok(())
}

pub async fn handle_recieved_object_for_community(
    community_local_id: i64,
    community_is_local: bool,
    obj: activitystreams::object::ObjectBox,
    ctx: Arc<crate::RouteContext>,
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
            let content = obj.as_ref().get_content_xsd_string().map(|x| x.as_str());
            let media_type = obj.as_ref().get_media_type().map(|x| x.as_ref());
            let created = obj.as_ref().get_published().map(|x| x.as_datetime());
            // TODO support objects here?
            let author = obj.as_ref().get_attributed_to_xsd_any_uri();
            if let Some(object_id) = &obj.as_ref().id {
                if let Some(author) = author {
                    require_containment(object_id.as_url(), author.as_url())?;
                }

                let author = author.map(|x| x.as_str());

                handle_recieved_post(
                    object_id.as_str(),
                    title,
                    href,
                    content,
                    media_type,
                    created,
                    author,
                    community_local_id,
                    community_is_local,
                    ctx,
                )
                .await?;
            }
        }
        Some("Note") => {
            let obj: activitystreams::object::Note = obj.into_concrete().unwrap();
            let content = obj.as_ref().get_content_xsd_string().map(|x| x.as_str());
            let media_type = obj.as_ref().get_media_type().map(|x| x.as_ref());
            let created = obj.as_ref().get_published().map(|x| x.as_datetime());
            let author = obj
                .as_ref()
                .get_attributed_to_xsd_any_uri()
                .map(|x| x.as_str());

            if let Some(object_id) = &obj.as_ref().id {
                if let Some(in_reply_to) = &obj.as_ref().in_reply_to {
                    // it's a reply

                    handle_recieved_reply(
                        object_id.as_ref(),
                        content.unwrap_or(""),
                        media_type,
                        created,
                        author,
                        in_reply_to,
                        ctx,
                    )
                    .await?;
                } else {
                    // not a reply, must be a top-level post
                    let title = obj
                        .as_ref()
                        .get_summary_xsd_string()
                        .map(|x| x.as_str())
                        .unwrap_or("");

                    if let Some(object_id) = &obj.as_ref().id {
                        handle_recieved_post(
                            object_id.as_str(),
                            title,
                            None,
                            content,
                            media_type,
                            created,
                            author,
                            community_local_id,
                            community_is_local,
                            ctx,
                        )
                        .await?;
                    }
                }
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
    content: Option<&str>,
    media_type: Option<&mime::Mime>,
    created: Option<&chrono::DateTime<chrono::FixedOffset>>,
    author: Option<&str>,
    community_local_id: i64,
    community_is_local: bool,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;
    let author = match author {
        Some(author) => Some(
            get_or_fetch_user_local_id(&author, &db, &ctx.host_url_apub, &ctx.http_client).await?,
        ),
        None => None,
    };

    let content_is_html = media_type.is_none() || media_type == Some(&mime::TEXT_HTML);
    let (content_text, content_html) = if content_is_html {
        (None, Some(content))
    } else {
        (Some(content), None)
    };

    let row = db.query_opt(
        "INSERT INTO post (author, href, content_text, content_html, title, created, community, local, ap_id) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), $7, FALSE, $8) ON CONFLICT (ap_id) DO NOTHING RETURNING id",
        &[&author, &href, &content_text, &content_html, &title, &created, &community_local_id, &object_id],
    ).await?;

    if community_is_local {
        if let Some(row) = row {
            let post_local_id = row.get(0);
            crate::on_community_add_post(community_local_id, post_local_id, object_id, ctx);
        }
    }

    Ok(())
}

async fn handle_recieved_reply(
    object_id: &str,
    content: &str,
    media_type: Option<&mime::Mime>,
    created: Option<&chrono::DateTime<chrono::FixedOffset>>,
    author: Option<&str>,
    in_reply_to: &activitystreams::object::properties::ObjectPropertiesInReplyToEnum,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    let author = match author {
        Some(author) => Some(
            get_or_fetch_user_local_id(&author, &db, &ctx.host_url_apub, &ctx.http_client).await?,
        ),
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

    let last_reply_to = in_reply_to.last(); // TODO maybe not this? Not sure how to interpret inReplyTo

    if let Some(last_reply_to) = last_reply_to {
        if let activitystreams::object::properties::ObjectPropertiesInReplyToTermEnum::XsdAnyUri(
            term_ap_id,
        ) = last_reply_to
        {
            #[derive(Debug)]
            enum ReplyTarget {
                Post { id: i64 },
                Comment { id: i64, post: i64 },
            }

            let term_ap_id = term_ap_id.as_str();
            let target = if term_ap_id.starts_with(&ctx.host_url_apub) {
                let remaining = &term_ap_id[ctx.host_url_apub.len()..];
                if remaining.starts_with("/posts/") {
                    if let Ok(local_post_id) = remaining[7..].parse() {
                        Some(ReplyTarget::Post { id: local_post_id })
                    } else {
                        None
                    }
                } else if remaining.starts_with("/comments/") {
                    if let Ok(local_comment_id) = remaining[10..].parse() {
                        let row = db
                            .query_opt("SELECT post FROM reply WHERE id=$1", &[&local_comment_id])
                            .await?;
                        if let Some(row) = row {
                            Some(ReplyTarget::Comment {
                                id: local_comment_id,
                                post: row.get(0),
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                let row = db
                    .query_opt("(SELECT id, post FROM reply WHERE ap_id=$1) UNION (SELECT NULL, id FROM post WHERE ap_id=$1) LIMIT 1", &[&term_ap_id])
                    .await?;
                row.map(|row| match row.get(0) {
                    Some(reply_id) => ReplyTarget::Comment {
                        id: reply_id,
                        post: row.get(1),
                    },
                    None => ReplyTarget::Post { id: row.get(1) },
                })
            };

            if let Some(target) = target {
                let (post, parent) = match target {
                    ReplyTarget::Post { id } => (id, None),
                    ReplyTarget::Comment { id, post } => (post, Some(id)),
                };

                let local_community_maybe = {
                    let row = db.query_opt("SELECT id FROM community WHERE id=(SELECT community FROM post WHERE id=$1) AND local", &[&post]).await?;
                    row.map(|row| row.get(0))
                };

                let content_is_html = media_type.is_none() || media_type == Some(&mime::TEXT_HTML);
                let (content_text, content_html) = if content_is_html {
                    (None, Some(content))
                } else {
                    (Some(content), None)
                };

                let row = db.query_opt(
                    "INSERT INTO reply (post, parent, author, content_text, content_html, created, local, ap_id) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), FALSE, $7) ON CONFLICT (ap_id) DO NOTHING RETURNING id",
                    &[&post, &parent, &author, &content_text, &content_html, &created, &object_id],
                    ).await?;

                if let Some(row) = row {
                    if let Some(local_community) = local_community_maybe {
                        crate::on_community_add_comment(
                            local_community,
                            row.get(0),
                            object_id,
                            ctx,
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

pub async fn handle_like(
    activity: activitystreams::activity::Like,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    let activity_id = activity
        .object_props
        .get_id()
        .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

    if let Some(actor_id) = activity.like_props.get_actor_xsd_any_uri() {
        require_containment(activity_id.as_url(), actor_id.as_url())?;

        let actor_local_id = get_or_fetch_user_local_id(
            actor_id.as_str(),
            &db,
            &ctx.host_url_apub,
            &ctx.http_client,
        )
        .await?;

        if let Some(object_id) = activity.like_props.get_object_xsd_any_uri() {
            let object_id = object_id.as_str();
            let thing_local_ref = if object_id.starts_with(&ctx.host_url_apub) {
                let remaining = &object_id[ctx.host_url_apub.len()..];
                if remaining.starts_with("/posts/") {
                    if let Ok(local_post_id) = remaining[7..].parse() {
                        Some(ThingLocalRef::Post(local_post_id))
                    } else {
                        None
                    }
                } else if remaining.starts_with("/comments/") {
                    if let Ok(local_comment_id) = remaining[10..].parse() {
                        Some(ThingLocalRef::Comment(local_comment_id))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                let row = db.query_opt(
                    "(SELECT TRUE, id FROM post WHERE ap_id=$1) UNION ALL (SELECT FALSE, id FROM reply WHERE ap_id=$1) LIMIT 1",
                    &[&object_id],
                ).await?;

                if let Some(row) = row {
                    Some(if row.get(0) {
                        ThingLocalRef::Post(row.get(1))
                    } else {
                        ThingLocalRef::Comment(row.get(1))
                    })
                } else {
                    None
                }
            };

            match thing_local_ref {
                Some(ThingLocalRef::Post(post_local_id)) => {
                    let row_count = db.execute(
                        "INSERT INTO post_like (post, person, local, ap_id) VALUES ($1, $2, FALSE, $3) ON CONFLICT (ap_id) DO NOTHING",
                        &[&post_local_id, &actor_local_id, &activity_id.as_str()],
                    ).await?;

                    if row_count > 0 {
                        let row = db.query_opt("SELECT post.community, community.local FROM post, community WHERE post.community = community.id AND post.id=$1", &[&post_local_id]).await?;
                        if let Some(row) = row {
                            let community_local = row.get(1);
                            if community_local {
                                let community_id = row.get(0);
                                let body = serde_json::to_string(&activity)?;
                                enqueue_forward_to_community_followers(community_id, body, ctx)
                                    .await?;
                            }
                        }
                    }
                }
                Some(ThingLocalRef::Comment(comment_local_id)) => {
                    let row_count = db.execute(
                        "INSERT INTO reply_like (reply, person, local, ap_id) VALUES ($1, $2, FALSE, $3) ON CONFLICT (ap_id) DO NOTHING",
                        &[&comment_local_id, &actor_local_id, &activity_id.as_str()],
                    ).await?;

                    if row_count > 0 {
                        let row = db.query_opt("SELECT post.community, community.local FROM reply, post, community WHERE reply.post = post.id AND post.community = community.id AND post.id=$1", &[&comment_local_id]).await?;
                        if let Some(row) = row {
                            let community_local = row.get(1);
                            if community_local {
                                let community_id = row.get(0);
                                let body = serde_json::to_string(&activity)?;
                                enqueue_forward_to_community_followers(community_id, body, ctx)
                                    .await?;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}

pub async fn handle_delete(
    activity: activitystreams::activity::Delete,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    if let Some(object_id) = activity.delete_props.get_object_xsd_any_uri() {
        // TODO verify that this actor actually did this and is allowed to

        let row = db.query_opt(
            "WITH deleted_post AS (UPDATE post SET href=NULL, title='[deleted]', content_text='[deleted]', deleted=TRUE WHERE ap_id=$1 AND deleted=FALSE RETURNING (SELECT id FROM community WHERE community.id = post.community AND community.local)), deleted_reply AS (UPDATE reply SET content_text='[deleted]', deleted=TRUE WHERE ap_id=$1 AND deleted=FALSE RETURNING (SELECT id FROM community WHERE community.id=(SELECT community FROM post WHERE id=reply.post) AND community.local)) (SELECT * FROM deleted_post) UNION ALL (SELECT * FROM deleted_reply) LIMIT 1",
            &[&object_id.as_str()],
            ).await?;

        if let Some(row) = row {
            // Something was deleted
            let local_community = row.get(0);
            if let Some(community_id) = local_community {
                // Community is local, need to forward delete to followers

                let body = serde_json::to_string(&activity)?;

                crate::spawn_task(crate::apub_util::enqueue_forward_to_community_followers(
                    community_id,
                    body,
                    ctx,
                ));
            }
        }
    }

    Ok(())
}

pub async fn check_signature_for_actor(
    signature: &hyper::header::HeaderValue,
    request_method: &hyper::Method,
    request_path_and_query: &str,
    headers: &hyper::header::HeaderMap,
    actor_ap_id: &str,
    db: &tokio_postgres::Client,
    http_client: &crate::HttpClient,
) -> Result<bool, crate::Error> {
    let found_key = db.query_opt("(SELECT public_key FROM person WHERE ap_id=$1) UNION ALL (SELECT public_key FROM community WHERE ap_id=$1) LIMIT 1", &[&actor_ap_id]).await?
        .and_then(|row| row.get::<_, Option<&[u8]>>(0).map(openssl::pkey::PKey::public_key_from_pem)).transpose()?;

    println!("signature: {:?}", signature);

    let signature = hancock::Signature::parse(signature)?;

    if let Some(key) = found_key {
        if signature.verify(
            request_method,
            request_path_and_query,
            headers,
            |bytes, sig| do_verify(&key, bytes, sig),
        )? {
            return Ok(true);
        }
    }

    // Either no key found or failed to verify
    // Try fetching the actor/key

    let actor = fetch_actor(actor_ap_id, db, http_client).await?;

    if let Some(key) = actor.public_key() {
        let key = openssl::pkey::PKey::public_key_from_pem(key)?;
        Ok(signature.verify(
            request_method,
            request_path_and_query,
            headers,
            |bytes, sig| do_verify(&key, bytes, sig),
        )?)
    } else {
        Err(crate::Error::InternalStrStatic(
            "Cannot verify signature, no key found",
        ))
    }
}

pub async fn verify_incoming_activity(
    mut req: hyper::Request<hyper::Body>,
    db: &tokio_postgres::Client,
    http_client: &crate::HttpClient,
    apub_proxy_rewrites: bool,
) -> Result<activitystreams::activity::ActivityBox, crate::Error> {
    let req_body = hyper::body::to_bytes(req.body_mut()).await?;

    match req.headers().get("signature") {
        None => {
            let raw_obj_props: activitystreams::object::properties::ObjectProperties =
                serde_json::from_slice(&req_body)?;

            let ap_id = raw_obj_props.get_id().ok_or_else(|| {
                crate::Error::InternalStrStatic("Missing id in received activity")
            })?;

            let res = crate::res_to_error(
                http_client
                    .request(
                        hyper::Request::get(ap_id.as_str())
                            .header(hyper::header::ACCEPT, ACTIVITY_TYPE)
                            .body(Default::default())?,
                    )
                    .await?,
            )
            .await?;

            println!("{:?}", res);

            let res_body = hyper::body::to_bytes(res.into_body()).await?;

            // TODO verify user and activity domains match
            Ok(serde_json::from_slice(&res_body)?)
        }
        Some(signature) => {
            let raw_activity_props: activitystreams::activity::properties::ActorOptOriginAndTargetProperties = serde_json::from_slice(&req_body)?;

            let actor_ap_id = match raw_activity_props.actor {
                activitystreams::activity::properties::ActorOptOriginAndTargetPropertiesActorEnum::Term(ref term) => {
                    match term {
                        activitystreams::activity::properties::ActorOptOriginAndTargetPropertiesActorTermEnum::XsdAnyUri(uri) => Cow::Borrowed(uri.as_str()),
                        activitystreams::activity::properties::ActorOptOriginAndTargetPropertiesActorTermEnum::BaseBox(raw_actor) => {
                            // TODO not this?
                            Cow::Owned(serde_json::to_value(raw_actor)?.get("id").and_then(serde_json::Value::as_str)
                                .ok_or_else(|| crate::Error::InternalStrStatic("No id found for actor"))?
                                .to_owned())
                        },
                    }
                },
                _ => return Err(crate::Error::InternalStrStatic("Found multiple actors for activity, can't verify signature"))
            };

            let path_and_query = req
                .uri()
                .path_and_query()
                .ok_or(crate::Error::InternalStrStatic(
                    "Missing path, cannot verify signature",
                ))?
                .as_str();

            // path ends up wrong with our recommended proxy config
            let path_and_query = if apub_proxy_rewrites {
                req.headers()
                    .get("x-forwarded-path")
                    .map(|x| x.to_str())
                    .transpose()?
            } else {
                None
            }
            .unwrap_or(path_and_query);

            if check_signature_for_actor(
                signature,
                req.method(),
                path_and_query,
                &req.headers(),
                &actor_ap_id,
                db,
                http_client,
            )
            .await?
            {
                Ok(serde_json::from_slice(&req_body)?)
            } else {
                Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::FORBIDDEN,
                    "Signature check failed",
                )))
            }
        }
    }
}
