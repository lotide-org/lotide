use crate::{BaseURL, CommentLocalID, CommunityLocalID, PostLocalID, ThingLocalRef, UserLocalID};
use activitystreams::prelude::*;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;

pub const ACTIVITY_TYPE: &str = "application/activity+json";

pub const SIGALG_RSA_SHA256: &str = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256";
pub const SIGALG_RSA_SHA512: &str = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha512";

#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct Verified<T: Clone>(pub T);
impl<T: Clone> std::ops::Deref for Verified<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Clone> Verified<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

pub struct Contained<'a, T: activitystreams::markers::Base + Clone>(pub Cow<'a, Verified<T>>);
impl<'a, T: activitystreams::markers::Base + Clone> std::ops::Deref for Contained<'a, T> {
    type Target = Verified<T>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
impl<'a, T: activitystreams::markers::Base + Clone> Contained<'a, T> {
    pub fn into_inner(self) -> Cow<'a, Verified<T>> {
        self.0
    }
    pub fn with_owned(self) -> Contained<'static, T> {
        Contained(Cow::Owned(self.0.into_owned()))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum KnownObject {
    Accept(activitystreams::activity::Accept),
    Announce(activitystreams::activity::Announce),
    Create(activitystreams::activity::Create),
    Delete(activitystreams::activity::Delete),
    Follow(activitystreams::activity::Follow),
    Like(activitystreams::activity::Like),
    Undo(activitystreams::activity::Undo),
    Update(activitystreams::activity::Update),
    Person(
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<activitystreams::actor::Person>,
            PublicKeyExtension<'static>,
        >,
    ),
    Group(
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<activitystreams::actor::Group>,
            PublicKeyExtension<'static>,
        >,
    ),
    Article(activitystreams::object::Article),
    Image(activitystreams::object::Image),
    Page(activitystreams::object::Page),
    Note(activitystreams::object::Note),
}

#[derive(Deserialize)]
pub struct JustMaybeAPID {
    id: Option<BaseURL>,
}

#[derive(Deserialize)]
pub struct JustActor {
    actor: activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKey<'a> {
    pub id: Cow<'a, str>,
    pub owner: Cow<'a, str>,
    pub public_key_pem: Cow<'a, str>,
    pub signature_algorithm: Option<Cow<'a, str>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeyExtension<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_key: Option<PublicKey<'a>>,
}

pub fn try_strip_host<'a>(url: &'a impl AsRef<str>, host_url: &url::Url) -> Option<&'a str> {
    let host_url = host_url.as_str();
    let host_url = host_url.trim_end_matches('/');

    let url = url.as_ref();

    if url.starts_with(host_url) {
        Some(&url[host_url.len()..])
    } else {
        None
    }
}

pub fn get_local_shared_inbox(host_url_apub: &BaseURL) -> BaseURL {
    let mut res = host_url_apub.clone();
    res.path_segments_mut().push("inbox");
    res
}

pub fn get_local_post_apub_id(post: PostLocalID, host_url_apub: &BaseURL) -> BaseURL {
    let mut res = host_url_apub.clone();
    res.path_segments_mut()
        .extend(&["posts", &post.to_string()]);
    res
}

pub fn get_local_post_like_apub_id(
    post_local_id: PostLocalID,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = crate::apub_util::get_local_post_apub_id(post_local_id, &host_url_apub);
    res.path_segments_mut()
        .extend(&["likes", &user.to_string()]);
    res
}

pub fn get_local_comment_apub_id(comment: CommentLocalID, host_url_apub: &BaseURL) -> BaseURL {
    let mut res = host_url_apub.clone();
    res.path_segments_mut()
        .extend(&["comments", &comment.to_string()]);
    res
}

pub fn get_local_comment_like_apub_id(
    comment_local_id: CommentLocalID,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = crate::apub_util::get_local_comment_apub_id(comment_local_id, &host_url_apub);
    res.path_segments_mut()
        .extend(&["likes", &user.to_string()]);
    res
}

pub fn get_local_person_apub_id(person: UserLocalID, host_url_apub: &BaseURL) -> BaseURL {
    let mut res = host_url_apub.clone();
    res.path_segments_mut()
        .extend(&["users", &person.to_string()]);
    res
}

pub fn get_local_person_outbox_apub_id(person: UserLocalID, host_url_apub: &BaseURL) -> BaseURL {
    let mut res = get_local_person_apub_id(person, host_url_apub);
    res.path_segments_mut().push("outbox");
    res
}

pub fn get_local_person_outbox_page_apub_id(
    person: UserLocalID,
    page: &crate::TimestampOrLatest,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = get_local_person_outbox_apub_id(person, host_url_apub);
    res.path_segments_mut().extend(&["page", &page.to_string()]);
    res
}

pub fn get_local_community_apub_id(
    community: CommunityLocalID,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = host_url_apub.clone();
    res.path_segments_mut()
        .extend(&["communities", &community.to_string()]);
    res
}

pub fn get_local_community_outbox_apub_id(
    community: CommunityLocalID,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = get_local_community_apub_id(community, host_url_apub);
    res.path_segments_mut().push("outbox");
    res
}

pub fn get_local_community_outbox_page_apub_id(
    community: CommunityLocalID,
    page: &crate::TimestampOrLatest,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = get_local_community_outbox_apub_id(community, host_url_apub);
    res.path_segments_mut().extend(&["page", &page.to_string()]);
    res
}

pub fn get_local_community_follow_apub_id(
    community: CommunityLocalID,
    follower: UserLocalID,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = get_local_community_apub_id(community, host_url_apub);
    res.path_segments_mut()
        .extend(&["followers", &follower.to_string()]);
    res
}

pub fn get_local_person_pubkey_apub_id(person: UserLocalID, host_url_apub: &BaseURL) -> BaseURL {
    let mut res = get_local_person_apub_id(person, host_url_apub);
    res.set_fragment(Some("main-key"));
    res
}

pub fn get_local_community_pubkey_apub_id(
    community: CommunityLocalID,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = get_local_community_apub_id(community, host_url_apub);
    res.set_fragment(Some("main-key"));
    res
}

pub fn get_local_follow_apub_id(
    community: CommunityLocalID,
    follower: UserLocalID,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = get_local_community_apub_id(community, host_url_apub);
    res.path_segments_mut()
        .extend(&["followers", &follower.to_string()]);
    res
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
    alg: openssl::hash::MessageDigest,
    src: &[u8],
    sig: &[u8],
) -> Result<bool, openssl::error::ErrorStack> {
    let mut verifier = openssl::sign::Verifier::new(alg, &key)?;
    verifier.update(&src)?;
    Ok(verifier.verify(sig)?)
}

pub struct PubKeyInfo {
    algorithm: Option<openssl::hash::MessageDigest>,
    key: Vec<u8>,
}

pub enum ActorLocalInfo {
    User {
        id: UserLocalID,
        public_key: Option<PubKeyInfo>,
    },
    Community {
        id: CommunityLocalID,
        public_key: Option<PubKeyInfo>,
    },
}

impl ActorLocalInfo {
    pub fn public_key(&self) -> Option<&PubKeyInfo> {
        match self {
            ActorLocalInfo::User { public_key, .. } => public_key.as_ref(),
            ActorLocalInfo::Community { public_key, .. } => public_key.as_ref(),
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

pub async fn fetch_ap_object_raw(
    ap_id: &url::Url,
    http_client: &crate::HttpClient,
) -> Result<serde_json::Value, crate::Error> {
    let mut current_id = hyper::Uri::try_from(ap_id.as_str())?;
    for _ in 0..3u8 {
        // avoid infinite loop in malicious or broken cases
        let res = crate::res_to_error(
            http_client
                .request(
                    hyper::Request::get(&current_id)
                        .header(hyper::header::ACCEPT, ACTIVITY_TYPE)
                        .body(Default::default())?,
                )
                .await?,
        )
        .await?;

        let body = hyper::body::to_bytes(res.into_body()).await?;
        let body: serde_json::Value = serde_json::from_slice(&body)?;

        current_id = match body.get("id") {
            None => return Err(crate::Error::InternalStrStatic("Missing id in object")),
            Some(body_id) => match body_id {
                serde_json::Value::String(body_id) => {
                    if current_id == body_id.as_ref() {
                        return Ok(body);
                    }

                    TryFrom::try_from(body_id)?
                }
                _ => return Err(crate::Error::InternalStrStatic("id was not a string")),
            },
        }
    }

    Err(crate::Error::InternalStrStatic("Recursion depth exceeded"))
}

pub async fn fetch_ap_object(
    ap_id: &url::Url,
    http_client: &crate::HttpClient,
) -> Result<Verified<KnownObject>, crate::Error> {
    let value = fetch_ap_object_raw(ap_id, http_client).await?;
    let value: KnownObject = serde_json::from_value(value)?;
    Ok(Verified(value))
}

pub async fn fetch_actor(
    req_ap_id: &url::Url,
    db: &tokio_postgres::Client,
    http_client: &crate::HttpClient,
) -> Result<ActorLocalInfo, crate::Error> {
    let obj = fetch_ap_object(req_ap_id, http_client).await?;
    let ap_id = req_ap_id;

    match obj.deref() {
        KnownObject::Person(person) => {
            let username = person
                .preferred_username()
                .or_else(|| {
                    person
                        .name()
                        .and_then(|maybe| maybe.iter().filter_map(|x| x.as_xsd_string()).next())
                })
                .unwrap_or("");
            let inbox = person.inbox_unchecked().as_str();
            let shared_inbox = person
                .endpoints_unchecked()
                .and_then(|endpoints| endpoints.shared_inbox)
                .map(|url| url.as_str());
            let public_key = person
                .ext_one
                .public_key
                .as_ref()
                .map(|key| key.public_key_pem.as_bytes());
            let public_key_sigalg = person
                .ext_one
                .public_key
                .as_ref()
                .and_then(|key| key.signature_algorithm.as_deref());
            let description = person
                .summary()
                .and_then(|maybe| maybe.iter().filter_map(|x| x.as_xsd_string()).next())
                .unwrap_or("");

            let avatar = person.icon().and_then(|icon| {
                icon.iter()
                    .filter_map(|icon| {
                        if icon.kind_str() == Some("Image") {
                            match activitystreams::object::Image::from_any_base(icon.clone()) {
                                Err(_) | Ok(None) => None,
                                Ok(Some(icon)) => Some(icon),
                            }
                        } else {
                            None
                        }
                    })
                    .next()
            });
            let avatar = avatar
                .as_ref()
                .and_then(|icon| icon.url().and_then(|url| url.as_single_id()))
                .map(|x| x.as_str());

            let id = UserLocalID(db.query_one(
                "INSERT INTO person (username, local, created_local, ap_id, ap_inbox, ap_shared_inbox, public_key, public_key_sigalg, description, avatar) VALUES ($1, FALSE, localtimestamp, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (ap_id) DO UPDATE SET ap_inbox=$3, ap_shared_inbox=$4, public_key=$5, public_key_sigalg=$6, description=$7, avatar=$8 RETURNING id",
                &[&username, &ap_id.as_str(), &inbox, &shared_inbox, &public_key, &public_key_sigalg, &description, &avatar],
            ).await?.get(0));

            Ok(ActorLocalInfo::User {
                id,
                public_key: public_key.map(|key| PubKeyInfo {
                    algorithm: get_message_digest(public_key_sigalg),
                    key: key.to_owned(),
                }),
            })
        }
        KnownObject::Group(group) => {
            let name = group
                .preferred_username()
                .or_else(|| {
                    group
                        .name()
                        .and_then(|maybe| maybe.iter().filter_map(|x| x.as_xsd_string()).next())
                })
                .unwrap_or("");
            let description = group
                .summary()
                .and_then(|maybe| maybe.iter().filter_map(|x| x.as_xsd_string()).next())
                .unwrap_or("");
            let inbox = group.inbox_unchecked().as_str();
            let shared_inbox = group
                .endpoints_unchecked()
                .and_then(|endpoints| endpoints.shared_inbox)
                .map(|url| url.as_str());
            let public_key = group
                .ext_one
                .public_key
                .as_ref()
                .map(|key| key.public_key_pem.as_bytes());
            let public_key_sigalg = group
                .ext_one
                .public_key
                .as_ref()
                .and_then(|key| key.signature_algorithm.as_deref());

            let id = CommunityLocalID(db.query_one(
                "INSERT INTO community (name, local, ap_id, ap_inbox, ap_shared_inbox, public_key, public_key_sigalg, description) VALUES ($1, FALSE, $2, $3, $4, $5, $6, $7) ON CONFLICT (ap_id) DO UPDATE SET ap_inbox=$3, ap_shared_inbox=$4, public_key=$5, public_key_sigalg=$6, description=$7 RETURNING id",
                &[&name, &ap_id.as_str(), &inbox, &shared_inbox, &public_key, &public_key_sigalg, &description],
            ).await?.get(0));

            Ok(ActorLocalInfo::Community {
                id,
                public_key: public_key.map(|key| PubKeyInfo {
                    algorithm: get_message_digest(public_key_sigalg),
                    key: key.to_owned(),
                }),
            })
        }
        _ => Err(crate::Error::InternalStrStatic("Unrecognized actor type")),
    }
}

pub async fn get_or_fetch_user_local_id(
    ap_id: &url::Url,
    db: &tokio_postgres::Client,
    host_url_apub: &BaseURL,
    http_client: &crate::HttpClient,
) -> Result<UserLocalID, crate::Error> {
    if let Some(remaining) = try_strip_host(ap_id, host_url_apub) {
        if remaining.starts_with("/users/") {
            Ok(remaining[7..].parse()?)
        } else {
            Err(crate::Error::InternalStr(format!(
                "Unrecognized local AP ID: {:?}",
                ap_id
            )))
        }
    } else {
        match db
            .query_opt("SELECT id FROM person WHERE ap_id=$1", &[&ap_id.as_str()])
            .await?
        {
            Some(row) => Ok(UserLocalID(row.get(0))),
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
    user: UserLocalID,
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
    community: CommunityLocalID,
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
    host_url_apub: &BaseURL,
) -> Result<(openssl::pkey::PKey<openssl::pkey::Private>, BaseURL), crate::Error> {
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

pub fn spawn_enqueue_send_new_community_update(
    community: CommunityLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let activity =
            local_community_update_to_ap(community, uuid::Uuid::new_v4(), &ctx.host_url_apub)?;
        enqueue_send_to_community_followers(community, activity, ctx).await
    });
}

pub fn spawn_enqueue_send_community_follow(
    community: CommunityLocalID,
    local_follower: UserLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let (community_ap_id, community_inbox): (url::Url, url::Url) = {
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
                let ap_id: Option<&str> = row.get(1);
                let ap_inbox: Option<&str> = row.get(2);

                (if let Some(ap_id) = ap_id {
                    if let Some(ap_inbox) = ap_inbox {
                        Some((ap_id.parse()?, ap_inbox.parse()?))
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

        let person_ap_id = get_local_person_apub_id(local_follower, &ctx.host_url_apub);

        let mut follow =
            activitystreams::activity::Follow::new(person_ap_id, community_ap_id.clone());
        follow
            .set_context(activitystreams::context())
            .set_id(
                get_local_community_follow_apub_id(community, local_follower, &ctx.host_url_apub)
                    .into(),
            )
            .set_to(community_ap_id);

        std::mem::drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(community_inbox),
            sign_as: Some(crate::ActorLocalRef::Person(local_follower)),
            object: serde_json::to_string(&follow)?,
        })
        .await?;

        Ok(())
    });
}

pub fn spawn_enqueue_send_community_follow_undo(
    undo_id: uuid::Uuid,
    community_local_id: CommunityLocalID,
    local_follower: UserLocalID,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let community_inbox: url::Url = {
            let db = ctx.db_pool.get().await?;

            let row = db
                .query_one(
                    "SELECT local, ap_inbox FROM community WHERE id=$1",
                    &[&community_local_id],
                )
                .await?;
            let local = row.get(0);
            if local {
                // no need to send follow state to ourself
                return Ok(());
            } else {
                let ap_inbox: Option<&str> = row.get(1);

                ap_inbox
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!(
                            "Missing apub info for community {}",
                            community_local_id,
                        ))
                    })?
                    .parse()?
            }
        };

        let undo = local_community_follow_undo_to_ap(
            undo_id,
            community_local_id,
            local_follower,
            &ctx.host_url_apub,
        )?;

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(community_inbox),
            sign_as: Some(crate::ActorLocalRef::Person(local_follower)),
            object: serde_json::to_string(&undo)?,
        })
        .await?;

        Ok(())
    });
}

pub fn local_community_post_announce_ap(
    community_id: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Announce, crate::Error> {
    let community_ap_id = get_local_community_apub_id(community_id, host_url_apub);

    let mut announce =
        activitystreams::activity::Announce::new(community_ap_id.clone(), post_ap_id);

    announce
        .set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id.clone();
            res.path_segments_mut()
                .extend(&["posts", &post_local_id.to_string(), "announce"]);
            res.into()
        })
        .set_to({
            let mut res = community_ap_id;
            res.path_segments_mut().push("followers");
            res
        })
        .set_cc(activitystreams::public());

    Ok(announce)
}

pub fn local_community_post_announce_undo_ap(
    community_id: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    uuid: &uuid::Uuid,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let community_ap_id = get_local_community_apub_id(community_id, host_url_apub);

    let announce =
        local_community_post_announce_ap(community_id, post_local_id, post_ap_id, host_url_apub)?;

    let mut undo =
        activitystreams::activity::Undo::new(community_ap_id.clone(), announce.into_any_base()?);

    undo.set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id.clone();
            res.path_segments_mut().extend(&[
                "posts",
                &post_local_id.to_string(),
                "announce",
                "undos",
                &uuid.to_string(),
            ]);
            res.into()
        })
        .set_to({
            let mut res = community_ap_id;
            res.path_segments_mut().push("followers");
            res
        })
        .set_cc(activitystreams::public());

    Ok(undo)
}

pub fn local_community_comment_announce_ap(
    community_id: CommunityLocalID,
    comment_local_id: CommentLocalID,
    comment_ap_id: url::Url,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Announce, crate::Error> {
    let community_ap_id = get_local_community_apub_id(community_id, host_url_apub);

    let mut announce =
        activitystreams::activity::Announce::new(community_ap_id.deref().clone(), comment_ap_id);

    announce.set_context(activitystreams::context()).set_id({
        let mut res = community_ap_id;
        res.path_segments_mut()
            .extend(&["comments", &comment_local_id.to_string(), "announce"]);
        res.into()
    });

    Ok(announce)
}

pub fn spawn_announce_community_post(
    community: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
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

pub fn spawn_enqueue_send_community_post_announce_undo(
    community: CommunityLocalID,
    post: PostLocalID,
    post_ap_id: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let undo = local_community_post_announce_undo_ap(
            community,
            post,
            post_ap_id,
            &uuid::Uuid::new_v4(),
            &ctx.host_url_apub,
        )?;

        enqueue_send_to_community_followers(community, undo, ctx).await
    });
}

pub fn spawn_announce_community_comment(
    community: CommunityLocalID,
    comment_local_id: CommentLocalID,
    comment_ap_id: url::Url,
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

pub fn local_community_update_to_ap(
    community_id: CommunityLocalID,
    update_id: uuid::Uuid,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Update, crate::Error> {
    let community_ap_id = get_local_community_apub_id(community_id, host_url_apub);

    let mut update =
        activitystreams::activity::Update::new(community_ap_id.clone(), community_ap_id.clone());

    update.set_id({
        let mut res = community_ap_id;
        res.path_segments_mut()
            .extend(&["updates", &update_id.to_string()]);
        res.into()
    });

    Ok(update)
}

pub fn local_community_follow_undo_to_ap(
    undo_id: uuid::Uuid,
    community_local_id: CommunityLocalID,
    local_follower: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let mut undo = activitystreams::activity::Undo::new(
        get_local_person_apub_id(local_follower, &host_url_apub),
        get_local_community_follow_apub_id(community_local_id, local_follower, &host_url_apub),
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["community_follow_undos", &undo_id.to_string()]);
        res.into()
    });

    Ok(undo)
}

pub fn community_follow_accept_to_ap(
    community_ap_id: BaseURL,
    follower_local_id: UserLocalID,
    follow_ap_id: url::Url,
) -> Result<activitystreams::activity::Accept, crate::Error> {
    let mut accept = activitystreams::activity::Accept::new(community_ap_id.clone(), follow_ap_id);

    accept.set_context(activitystreams::context()).set_id({
        let mut res = community_ap_id;
        res.path_segments_mut()
            .extend(&["followers", &follower_local_id.to_string(), "accept"]);
        res.into()
    });

    Ok(accept)
}

pub fn spawn_enqueue_send_community_follow_accept(
    local_community: CommunityLocalID,
    follower: UserLocalID,
    follow: Contained<'static, activitystreams::activity::Follow>,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let follow_ap_id = {
            (match follow.into_inner() {
                Cow::Owned(follow) => follow.into_inner().take_id(),
                Cow::Borrowed(follow) => follow.id_unchecked().cloned(),
            })
            .ok_or(crate::Error::InternalStrStatic(
                "Missing ID in Follow activity",
            ))?
        };

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
                let ap_inbox: Option<&str> = row.get(1);

                ap_inbox
                    .ok_or_else(|| {
                        crate::Error::InternalStr(format!(
                            "Missing apub info for user {}",
                            follower
                        ))
                    })?
                    .parse()?
            }
        };

        let accept = community_follow_accept_to_ap(community_ap_id, follower, follow_ap_id)?;
        println!("{:?}", accept);

        let body = serde_json::to_string(&accept)?;

        std::mem::drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(follower_inbox),
            sign_as: Some(crate::ActorLocalRef::Community(local_community)),
            object: body,
        })
        .await?;

        Ok(())
    });
}

pub fn post_to_ap(
    post: &crate::PostInfo<'_>,
    community_ap_id: url::Url,
    ctx: &crate::BaseContext,
) -> Result<activitystreams::base::AnyBase, crate::Error> {
    fn apply_content<
        K,
        O: activitystreams::object::ObjectExt<K> + activitystreams::base::BaseExt<K>,
    >(
        props: &mut activitystreams::object::ApObject<O>,
        post: &crate::PostInfo,
    ) -> Result<(), crate::Error> {
        if let Some(html) = post.content_html {
            props.set_content(html).set_media_type(mime::TEXT_HTML);

            if let Some(md) = post.content_markdown {
                let mut src = activitystreams::object::Object::<()>::new();
                src.set_content(md)
                    .set_media_type("text/markdown".parse().unwrap())
                    .delete_kind();
                props.set_source(src.into_any_base()?);
            }
        } else if let Some(text) = post.content_text {
            props.set_content(text).set_media_type(mime::TEXT_PLAIN);
        }

        Ok(())
    };

    match post.href {
        Some(href) => {
            if href.starts_with("local-media://") {
                let mut post_ap = activitystreams::object::Image::new();

                post_ap
                    .set_context(activitystreams::context())
                    .set_id(get_local_post_apub_id(post.id, &ctx.host_url_apub).into())
                    .set_attributed_to(get_local_person_apub_id(
                        post.author.unwrap(),
                        &ctx.host_url_apub,
                    ))
                    .set_url(ctx.process_href(href, post.id).into_owned())
                    .set_summary(post.title)
                    .set_published(*post.created)
                    .set_to(community_ap_id)
                    .set_cc(activitystreams::public());

                let mut post_ap = activitystreams::object::ApObject::new(post_ap);

                apply_content(&mut post_ap, post)?;

                Ok(post_ap.into_any_base()?)
            } else {
                let mut post_ap = activitystreams::object::Page::new();

                post_ap
                    .set_context(activitystreams::context())
                    .set_id(get_local_post_apub_id(post.id, &ctx.host_url_apub).into())
                    .set_attributed_to(get_local_person_apub_id(
                        post.author.unwrap(),
                        &ctx.host_url_apub,
                    ))
                    .set_url(href.to_owned())
                    .set_summary(post.title)
                    .set_published(*post.created)
                    .set_to(community_ap_id)
                    .set_cc(activitystreams::public());

                let mut post_ap = activitystreams::object::ApObject::new(post_ap);

                apply_content(&mut post_ap, post)?;

                Ok(post_ap.into_any_base()?)
            }
        }
        None => {
            let mut post_ap = activitystreams::object::Note::new();

            post_ap
                .set_context(activitystreams::context())
                .set_id(get_local_post_apub_id(post.id, &ctx.host_url_apub).into())
                .set_attributed_to(Into::<url::Url>::into(get_local_person_apub_id(
                    post.author.unwrap(),
                    &ctx.host_url_apub,
                )))
                .set_summary(post.title)
                .set_published(*post.created)
                .set_to(community_ap_id)
                .set_cc(activitystreams::public());

            let mut post_ap = activitystreams::object::ApObject::new(post_ap);

            apply_content(&mut post_ap, post)?;

            Ok(post_ap.into_any_base()?)
        }
    }
}

pub fn local_post_to_create_ap(
    post: &crate::PostInfo<'_>,
    community_ap_id: url::Url,
    ctx: &crate::BaseContext,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let post_ap = post_to_ap(&post, community_ap_id, &ctx)?;

    let mut create = activitystreams::activity::Create::new(
        get_local_person_apub_id(post.author.unwrap(), &ctx.host_url_apub),
        post_ap,
    );
    create.set_context(activitystreams::context()).set_id({
        let mut res = get_local_post_apub_id(post.id, &ctx.host_url_apub);
        res.path_segments_mut().push("create");
        res.into()
    });

    Ok(create)
}

pub fn local_comment_to_ap(
    comment: &crate::CommentInfo,
    post_ap_id: &url::Url,
    parent_ap_id: Option<url::Url>,
    parent_or_post_author_ap_id: Option<url::Url>,
    community_ap_id: url::Url,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::object::ApObject<activitystreams::object::Note>, crate::Error> {
    let mut obj = activitystreams::object::Note::new();

    obj.set_context(activitystreams::context())
        .set_id(get_local_comment_apub_id(comment.id, &host_url_apub).into())
        .set_attributed_to(url::Url::from(get_local_person_apub_id(
            comment.author.unwrap(),
            &host_url_apub,
        )))
        .set_published(comment.created)
        .set_in_reply_to(parent_ap_id.unwrap_or_else(|| post_ap_id.clone()));

    let mut obj = activitystreams::object::ApObject::new(obj);

    if let Some(html) = &comment.content_html {
        obj.set_content(html.as_ref().to_owned())
            .set_media_type(mime::TEXT_HTML);

        if let Some(md) = &comment.content_markdown {
            let mut src = activitystreams::object::Object::<()>::new();
            src.set_content(md.as_ref())
                .set_media_type("text/markdown".parse().unwrap())
                .delete_kind();
            obj.set_source(src.into_any_base()?);
        }
    } else if let Some(text) = &comment.content_text {
        obj.set_content(text.as_ref().to_owned())
            .set_media_type(mime::TEXT_PLAIN);
    }

    if let Some(parent_or_post_author_ap_id) = parent_or_post_author_ap_id {
        obj.set_to(parent_or_post_author_ap_id)
            .set_many_ccs(vec![activitystreams::public(), community_ap_id]);
    } else {
        obj.set_to(community_ap_id)
            .set_cc(activitystreams::public());
    }

    Ok(obj)
}

pub fn spawn_enqueue_send_local_post_to_community(
    post: crate::PostInfoOwned,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let (community_ap_id, community_inbox): (url::Url, url::Url) = {
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
                let ap_id: Option<&str> = row.get(1);
                let ap_inbox: Option<&str> = row.get(2);

                (if let Some(ap_id) = ap_id {
                    if let Some(ap_inbox) = ap_inbox {
                        Some((ap_id.parse()?, ap_inbox.parse()?))
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

        let create = local_post_to_create_ap(&(&post).into(), community_ap_id, &ctx)?;

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(community_inbox),
            sign_as: Some(crate::ActorLocalRef::Person(post.author.unwrap())),
            object: serde_json::to_string(&create)?,
        })
        .await?;

        Ok(())
    });
}

pub fn local_post_delete_to_ap(
    post_id: PostLocalID,
    author: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Delete, crate::Error> {
    let post_ap_id = get_local_post_apub_id(post_id, host_url_apub);
    let mut delete = activitystreams::activity::Delete::new(
        get_local_person_apub_id(author, host_url_apub),
        post_ap_id.clone(),
    );
    delete.set_context(activitystreams::context()).set_id({
        let mut res = post_ap_id;
        res.path_segments_mut().push("delete");
        res.into()
    });

    Ok(delete)
}

pub fn local_comment_delete_to_ap(
    comment_id: CommentLocalID,
    author: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Delete, crate::Error> {
    let comment_ap_id = get_local_comment_apub_id(comment_id, host_url_apub);

    let mut delete = activitystreams::activity::Delete::new(
        get_local_person_apub_id(author, host_url_apub),
        comment_ap_id.clone(),
    );

    delete.set_context(activitystreams::context()).set_id({
        let mut res = comment_ap_id;
        res.path_segments_mut().push("delete");
        res.into()
    });

    Ok(delete)
}

pub fn local_comment_to_create_ap(
    comment: &crate::CommentInfo,
    post_ap_id: &url::Url,
    parent_ap_id: Option<url::Url>,
    parent_or_post_author_ap_id: Option<url::Url>,
    community_ap_id: url::Url,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let comment_ap = local_comment_to_ap(
        &comment,
        post_ap_id,
        parent_ap_id,
        parent_or_post_author_ap_id.clone(),
        community_ap_id.clone(),
        &host_url_apub,
    )?;

    let author = comment.author.unwrap();

    let mut create = activitystreams::activity::Create::new(
        get_local_person_apub_id(author, host_url_apub),
        comment_ap.into_any_base()?,
    );
    create.set_context(activitystreams::context()).set_id({
        let mut res = get_local_comment_apub_id(comment.id, host_url_apub);
        res.path_segments_mut().push("create");
        res.into()
    });

    if let Some(parent_or_post_author_ap_id) = parent_or_post_author_ap_id {
        create
            .set_to(parent_or_post_author_ap_id)
            .set_many_ccs(vec![activitystreams::public(), community_ap_id]);
    } else {
        create
            .set_to(community_ap_id)
            .set_cc(activitystreams::public());
    }

    Ok(create)
}

pub fn local_post_like_to_ap(
    post_local_id: PostLocalID,
    post_ap_id: BaseURL,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Like, crate::Error> {
    let mut like = activitystreams::activity::Like::new(
        crate::apub_util::get_local_person_apub_id(user, &host_url_apub),
        post_ap_id,
    );
    like.set_context(activitystreams::context())
        .set_id(get_local_post_like_apub_id(post_local_id, user, &host_url_apub).into());

    Ok(like)
}

pub fn local_post_like_undo_to_ap(
    undo_id: uuid::Uuid,
    post_local_id: PostLocalID,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let like_ap_id = get_local_post_like_apub_id(post_local_id, user, &host_url_apub);

    let mut undo = activitystreams::activity::Undo::new(
        get_local_person_apub_id(user, &host_url_apub),
        like_ap_id,
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["post_like_undos", &undo_id.to_string()]);
        res.into()
    });

    Ok(undo)
}

pub fn local_comment_like_to_ap(
    comment_local_id: CommentLocalID,
    comment_ap_id: BaseURL,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Like, crate::Error> {
    let mut like = activitystreams::activity::Like::new(
        crate::apub_util::get_local_person_apub_id(user, &host_url_apub),
        comment_ap_id,
    );
    like.set_context(activitystreams::context())
        .set_id(get_local_comment_like_apub_id(comment_local_id, user, &host_url_apub).into());

    Ok(like)
}

pub fn local_comment_like_undo_to_ap(
    undo_id: uuid::Uuid,
    comment_local_id: CommentLocalID,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let like_ap_id = get_local_comment_like_apub_id(comment_local_id, user, &host_url_apub);

    let mut undo = activitystreams::activity::Undo::new(
        get_local_person_apub_id(user, &host_url_apub),
        like_ap_id,
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["comment_like_undos", &undo_id.to_string()]);
        res.into()
    });

    Ok(undo)
}

pub fn spawn_enqueue_send_comment(
    inboxes: HashSet<url::Url>,
    comment: crate::CommentInfo,
    community_ap_id: url::Url,
    post_ap_id: url::Url,
    parent_ap_id: Option<url::Url>,
    post_or_parent_author_ap_id: Option<url::Url>,
    ctx: Arc<crate::RouteContext>,
) {
    if inboxes.is_empty() {
        return;
    }

    let create = local_comment_to_create_ap(
        &comment,
        &post_ap_id,
        parent_ap_id,
        post_or_parent_author_ap_id,
        community_ap_id,
        &ctx.host_url_apub,
    );

    let author = comment.author.unwrap();

    crate::spawn_task(async move {
        let create = create?;

        // TODO maybe insert these at the same time
        for inbox in inboxes {
            ctx.enqueue_task(&crate::tasks::DeliverToInbox {
                inbox: Cow::Owned(inbox),
                sign_as: Some(crate::ActorLocalRef::Person(author)),
                object: serde_json::to_string(&create)?,
            })
            .await?;
        }

        Ok(())
    });
}

pub async fn enqueue_forward_to_community_followers(
    community_id: CommunityLocalID,
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
    community_id: CommunityLocalID,
    activity: impl serde::Serialize,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    ctx.enqueue_task(&crate::tasks::DeliverToFollowers {
        actor: crate::ActorLocalRef::Community(community_id),
        sign: true,
        object: serde_json::to_string(&activity)?,
    })
    .await
}

pub fn maybe_get_local_community_id_from_uri(
    uri: &url::Url,
    host_url_apub: &BaseURL,
) -> Option<CommunityLocalID> {
    if let Some(path) = try_strip_host(uri, host_url_apub) {
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

pub async fn handle_recieved_object_for_local_community<'a>(
    obj: Verified<KnownObject>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let (to, in_reply_to, obj_id) = match obj.deref() {
        KnownObject::Page(obj) => (obj.to(), None, obj.id_unchecked()),
        KnownObject::Image(obj) => (obj.to(), None, obj.id_unchecked()),
        KnownObject::Article(obj) => (obj.to(), None, obj.id_unchecked()),
        KnownObject::Note(obj) => (obj.to(), obj.in_reply_to(), obj.id_unchecked()),
        _ => (None, None, None),
    };

    let local_community_id = match to {
        None => None,
        Some(maybe) => maybe
            .iter()
            .filter_map(|any| {
                any.as_xsd_any_uri()
                    .and_then(|uri| maybe_get_local_community_id_from_uri(uri, &ctx.host_url_apub))
            })
            .next(),
    };

    if let Some(local_community_id) = local_community_id {
        handle_recieved_object_for_community(local_community_id, true, None, obj, ctx).await?;
    } else {
        // not to a community, but might still match as a reply
        if let Some(in_reply_to) = in_reply_to {
            if let Some(obj_id) = obj_id {
                if let KnownObject::Note(obj) = obj.deref() {
                    // TODO deduplicate this?

                    let content = obj.content().and_then(|x| x.as_single_xsd_string());
                    let media_type = obj.media_type();
                    let created = obj.published();
                    let author = obj.attributed_to().and_then(|x| x.as_single_id());

                    if let Some(author) = author {
                        require_containment(obj_id, author)?;
                    }

                    handle_recieved_reply(
                        obj_id,
                        content.unwrap_or(""),
                        media_type,
                        created.as_ref(),
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

pub async fn handle_received_page_for_community<Kind: Clone + std::fmt::Debug>(
    community_local_id: CommunityLocalID,
    community_is_local: bool,
    is_announce: Option<&url::Url>,
    obj: Verified<activitystreams::object::Object<Kind>>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let title = obj
        .summary()
        .iter()
        .map(|x| x.iter())
        .flatten()
        .filter_map(|maybe| maybe.as_xsd_string())
        .next()
        .unwrap_or("");
    let href = obj
        .url()
        .iter()
        .map(|x| x.iter())
        .flatten()
        .filter_map(|maybe| {
            maybe
                .as_xsd_any_uri()
                .map(|x| x.as_str())
                .or_else(|| maybe.as_xsd_string())
        })
        .next();
    let content = obj.content().and_then(|x| x.as_single_xsd_string());
    let media_type = obj.media_type();
    let created = obj.published();
    let author = obj.attributed_to().and_then(|x| x.as_single_id());

    if let Some(object_id) = obj.id_unchecked() {
        if let Some(author) = author {
            require_containment(&object_id, author)?;
        }

        handle_recieved_post(
            object_id.clone(),
            title,
            href,
            content,
            media_type,
            created.as_ref(),
            author,
            community_local_id,
            community_is_local,
            is_announce,
            ctx,
        )
        .await?;
    }

    Ok(())
}

pub async fn handle_recieved_object_for_community<'a>(
    community_local_id: CommunityLocalID,
    community_is_local: bool,
    is_announce: Option<&url::Url>,
    obj: Verified<KnownObject>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    println!("recieved object: {:?}", obj);

    match obj.into_inner() {
        KnownObject::Page(obj) => {
            handle_received_page_for_community(
                community_local_id,
                community_is_local,
                is_announce,
                Verified(obj),
                ctx,
            )
            .await?
        }
        KnownObject::Image(obj) => {
            handle_received_page_for_community(
                community_local_id,
                community_is_local,
                is_announce,
                Verified(obj),
                ctx,
            )
            .await?
        }
        KnownObject::Article(obj) => {
            handle_received_page_for_community(
                community_local_id,
                community_is_local,
                is_announce,
                Verified(obj),
                ctx,
            )
            .await?
        }
        KnownObject::Note(obj) => {
            let content = obj.content().and_then(|x| x.as_single_xsd_string());
            let media_type = obj.media_type();
            let created = obj.published();
            let author = obj.attributed_to().and_then(|x| x.as_single_id());

            if let Some(object_id) = obj.id_unchecked() {
                if let Some(in_reply_to) = obj.in_reply_to() {
                    // it's a reply

                    handle_recieved_reply(
                        object_id,
                        content.unwrap_or(""),
                        media_type,
                        created.as_ref(),
                        author,
                        in_reply_to,
                        ctx,
                    )
                    .await?;
                } else {
                    // not a reply, must be a top-level post
                    let title = obj
                        .summary()
                        .and_then(|x| x.as_single_xsd_string())
                        .unwrap_or("");

                    // Interpret attachments (usually images) as links
                    let href = obj
                        .attachment()
                        .and_then(|x| x.iter().next())
                        .and_then(
                            |base: &activitystreams::base::AnyBase| match base.kind_str() {
                                Some("Document") => Some(
                                    activitystreams::object::Document::from_any_base(base.clone())
                                        .map(|obj| obj.unwrap().take_url()),
                                ),
                                Some("Image") => Some(
                                    activitystreams::object::Image::from_any_base(base.clone())
                                        .map(|obj| obj.unwrap().take_url()),
                                ),
                                _ => None,
                            },
                        )
                        .transpose()?
                        .flatten();
                    let href = href
                        .as_ref()
                        .and_then(|href| href.iter().filter_map(|x| x.as_xsd_any_uri()).next())
                        .map(|href| href.as_str());

                    if let Some(object_id) = obj.id_unchecked() {
                        handle_recieved_post(
                            object_id.clone(),
                            title,
                            href,
                            content,
                            media_type,
                            created.as_ref(),
                            author,
                            community_local_id,
                            community_is_local,
                            is_announce,
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
    object_id: url::Url,
    title: &str,
    href: Option<&str>,
    content: Option<&str>,
    media_type: Option<&mime::Mime>,
    created: Option<&chrono::DateTime<chrono::FixedOffset>>,
    author: Option<&url::Url>,
    community_local_id: CommunityLocalID,
    community_is_local: bool,
    is_announce: Option<&url::Url>,
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

    let approved = is_announce.is_some() || community_is_local;

    let row = db.query_opt(
        "INSERT INTO post (author, href, content_text, content_html, title, created, community, local, ap_id, approved, approved_ap_id) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), $7, FALSE, $8, $9, $10) ON CONFLICT (ap_id) DO UPDATE SET approved=$9, approved_ap_id=$10 RETURNING id",
        &[&author, &href, &content_text, &content_html, &title, &created, &community_local_id, &object_id.as_str(), &approved, &is_announce.map(|x| x.as_str())],
    ).await?;

    if community_is_local {
        if let Some(row) = row {
            let post_local_id = PostLocalID(row.get(0));
            crate::on_community_add_post(community_local_id, post_local_id, object_id, ctx);
        }
    }

    Ok(())
}

async fn handle_recieved_reply(
    object_id: &url::Url,
    content: &str,
    media_type: Option<&mime::Mime>,
    created: Option<&chrono::DateTime<chrono::FixedOffset>>,
    author: Option<&url::Url>,
    in_reply_to: &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    let author = match author {
        Some(author) => Some(
            get_or_fetch_user_local_id(&author, &db, &ctx.host_url_apub, &ctx.http_client).await?,
        ),
        None => None,
    };

    let last_reply_to = in_reply_to.iter().last(); // TODO maybe not this? Not sure how to interpret inReplyTo

    if let Some(last_reply_to) = last_reply_to {
        if let Some(term_ap_id) = last_reply_to.as_xsd_any_uri() {
            #[derive(Debug)]
            enum ReplyTarget {
                Post {
                    id: PostLocalID,
                },
                Comment {
                    id: CommentLocalID,
                    post: PostLocalID,
                },
            }

            let target = if let Some(remaining) = try_strip_host(&term_ap_id, &ctx.host_url_apub) {
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
                                post: PostLocalID(row.get(0)),
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
                    .query_opt("(SELECT id, post FROM reply WHERE ap_id=$1) UNION (SELECT NULL, id FROM post WHERE ap_id=$1) LIMIT 1", &[&term_ap_id.as_str()])
                    .await?;
                row.map(|row| match row.get::<_, Option<_>>(0).map(CommentLocalID) {
                    Some(reply_id) => ReplyTarget::Comment {
                        id: reply_id,
                        post: PostLocalID(row.get(1)),
                    },
                    None => ReplyTarget::Post {
                        id: PostLocalID(row.get(1)),
                    },
                })
            };

            if let Some(target) = target {
                let (post, parent) = match target {
                    ReplyTarget::Post { id } => (id, None),
                    ReplyTarget::Comment { id, post } => (post, Some(id)),
                };

                let content_is_html = media_type.is_none() || media_type == Some(&mime::TEXT_HTML);
                let (content_text, content_html) = if content_is_html {
                    (None, Some(content))
                } else {
                    (Some(content), None)
                };

                let row = db.query_opt(
                    "INSERT INTO reply (post, parent, author, content_text, content_html, created, local, ap_id) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), FALSE, $7) ON CONFLICT (ap_id) DO NOTHING RETURNING id",
                    &[&post, &parent, &author, &content_text, &content_html, &created, &object_id.as_str()],
                    ).await?;

                if let Some(row) = row {
                    let info = crate::CommentInfo {
                        id: CommentLocalID(row.get(0)),
                        author,
                        post,
                        parent,
                        content_text: content_text.map(|x| Cow::Owned(x.to_owned())),
                        content_markdown: None,
                        content_html: content_html.map(|x| Cow::Owned(x.to_owned())),
                        created: created.copied().unwrap_or_else(|| {
                            chrono::offset::Utc::now()
                                .with_timezone(&chrono::offset::FixedOffset::west(0))
                        }),
                        ap_id: crate::APIDOrLocal::APID(object_id.to_owned()),
                    };

                    crate::on_post_add_comment(info, ctx);
                }
            }
        }
    }

    Ok(())
}

pub async fn handle_like(
    activity: Verified<activitystreams::activity::Like>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    let activity_id = activity
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

    if let Some(actor_id) = activity.actor_unchecked().as_single_id() {
        require_containment(activity_id, actor_id)?;

        let actor_local_id =
            get_or_fetch_user_local_id(actor_id, &db, &ctx.host_url_apub, &ctx.http_client).await?;

        if let Some(object_id) = activity.object().as_single_id() {
            let thing_local_ref = if let Some(remaining) =
                try_strip_host(&object_id, &ctx.host_url_apub)
            {
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
                    &[&object_id.as_str()],
                ).await?;

                if let Some(row) = row {
                    Some(if row.get(0) {
                        ThingLocalRef::Post(PostLocalID(row.get(1)))
                    } else {
                        ThingLocalRef::Comment(CommentLocalID(row.get(1)))
                    })
                } else {
                    None
                }
            };

            match thing_local_ref {
                Some(ThingLocalRef::Post(post_local_id)) => {
                    let row_count = db.execute(
                        "INSERT INTO post_like (post, person, local, ap_id) VALUES ($1, $2, FALSE, $3) ON CONFLICT (post, person) DO NOTHING",
                        &[&post_local_id, &actor_local_id, &activity_id.as_str()],
                    ).await?;

                    if row_count > 0 {
                        let row = db.query_opt("SELECT post.community, community.local FROM post, community WHERE post.community = community.id AND post.id=$1", &[&post_local_id]).await?;
                        if let Some(row) = row {
                            let community_local = row.get(1);
                            if community_local {
                                let community_id = CommunityLocalID(row.get(0));
                                let body = serde_json::to_string(&activity)?;
                                enqueue_forward_to_community_followers(community_id, body, ctx)
                                    .await?;
                            }
                        }
                    }
                }
                Some(ThingLocalRef::Comment(comment_local_id)) => {
                    let row_count = db.execute(
                        "INSERT INTO reply_like (reply, person, local, ap_id) VALUES ($1, $2, FALSE, $3) ON CONFLICT (reply, person) DO NOTHING",
                        &[&comment_local_id, &actor_local_id, &activity_id.as_str()],
                    ).await?;

                    if row_count > 0 {
                        let row = db.query_opt("SELECT post.community, community.local FROM reply, post, community WHERE reply.post = post.id AND post.community = community.id AND post.id=$1", &[&comment_local_id]).await?;
                        if let Some(row) = row {
                            let community_local = row.get(1);
                            if community_local {
                                let community_id = CommunityLocalID(row.get(0));
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
    activity: Verified<activitystreams::activity::Delete>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    let activity_id = activity
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing ID for activity"))?;
    let actor_id = activity
        .actor_unchecked()
        .as_single_id()
        .ok_or(crate::Error::InternalStrStatic("Missing ID for actor"))?;

    if let Some(object_id) = activity.object().as_single_id() {
        require_containment(activity_id, actor_id)?;
        require_containment(object_id, actor_id)?;

        let row = db.query_opt(
            "WITH deleted_post AS (UPDATE post SET href=NULL, title='[deleted]', content_text='[deleted]', content_markdown=NULL, content_html=NULL, deleted=TRUE WHERE ap_id=$1 AND deleted=FALSE RETURNING (SELECT id FROM community WHERE community.id = post.community AND community.local)), deleted_reply AS (UPDATE reply SET content_text='[deleted]', content_markdown=NULL, content_html=NULL, deleted=TRUE WHERE ap_id=$1 AND deleted=FALSE RETURNING (SELECT id FROM community WHERE community.id=(SELECT community FROM post WHERE id=reply.post) AND community.local)) (SELECT * FROM deleted_post) UNION ALL (SELECT * FROM deleted_reply) LIMIT 1",
            &[&object_id.as_str()],
            ).await?;

        if let Some(row) = row {
            // Something was deleted
            let local_community = row.get::<_, Option<_>>(0).map(CommunityLocalID);
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

pub async fn handle_undo(
    activity: Verified<activitystreams::activity::Undo>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let activity_id = activity
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

    let actor_id =
        activity
            .actor_unchecked()
            .as_single_id()
            .ok_or(crate::Error::InternalStrStatic(
                "Missing actor for activity",
            ))?;

    let object_id = activity
        .object()
        .as_single_id()
        .ok_or(crate::Error::InternalStrStatic("Missing object for Undo"))?;

    require_containment(activity_id, actor_id)?;
    require_containment(object_id, actor_id)?;

    let object_id = object_id.as_str();

    let db = ctx.db_pool.get().await?;

    db.execute("DELETE FROM post_like WHERE ap_id=$1", &[&object_id])
        .await?;
    db.execute("DELETE FROM reply_like WHERE ap_id=$1", &[&object_id])
        .await?;
    db.execute("DELETE FROM community_follow WHERE ap_id=$1", &[&object_id])
        .await?;
    db.execute(
        "UPDATE post SET approved=FALSE, approved_ap_id=NULL WHERE approved_ap_id=$1",
        &[&object_id],
    )
    .await?;

    Ok(())
}

fn get_message_digest(src: Option<&str>) -> Option<openssl::hash::MessageDigest> {
    match src {
        None | Some(SIGALG_RSA_SHA256) => Some(openssl::hash::MessageDigest::sha256()),
        Some(SIGALG_RSA_SHA512) => Some(openssl::hash::MessageDigest::sha512()),
        _ => None,
    }
}

pub async fn check_signature_for_actor(
    signature: &hyper::header::HeaderValue,
    request_method: &hyper::Method,
    request_path_and_query: &str,
    headers: &hyper::header::HeaderMap,
    actor_ap_id: &url::Url,
    db: &tokio_postgres::Client,
    http_client: &crate::HttpClient,
) -> Result<bool, crate::Error> {
    let found_key = db.query_opt("(SELECT public_key, public_key_sigalg FROM person WHERE ap_id=$1) UNION ALL (SELECT public_key, public_key_sigalg FROM community WHERE ap_id=$1) LIMIT 1", &[&actor_ap_id.as_str()]).await?
        .and_then(|row| {
            row.get::<_, Option<&[u8]>>(0).map(|key| {
                openssl::pkey::PKey::public_key_from_pem(key)
                    .map(|key| (key, get_message_digest(row.get(1))))
            })
        }).transpose()?;

    println!("signature: {:?}", signature);
    println!("found_key: {:?}", found_key.is_some());

    let signature = hancock::Signature::parse(signature)?;

    if let Some((key, algorithm)) = found_key {
        let algorithm = algorithm.ok_or(crate::Error::InternalStrStatic(
            "Cannot verify signature, unknown algorithm",
        ))?;
        if signature.verify(
            request_method,
            request_path_and_query,
            headers,
            |bytes, sig| do_verify(&key, algorithm, bytes, sig),
        )? {
            return Ok(true);
        }
    }

    // Either no key found or failed to verify
    // Try fetching the actor/key

    let actor = fetch_actor(actor_ap_id, db, http_client).await?;

    if let Some(key_info) = actor.public_key() {
        let key = openssl::pkey::PKey::public_key_from_pem(&key_info.key)?;
        let algorithm = key_info.algorithm.ok_or(crate::Error::InternalStrStatic(
            "Cannot verify signature, unknown algorithm",
        ))?;
        Ok(signature.verify(
            request_method,
            request_path_and_query,
            headers,
            |bytes, sig| do_verify(&key, algorithm, bytes, sig),
        )?)
    } else {
        Err(crate::Error::InternalStrStatic(
            "Cannot verify signature, no key found",
        ))
    }
}

pub async fn verify_incoming_object(
    mut req: hyper::Request<hyper::Body>,
    db: &tokio_postgres::Client,
    http_client: &crate::HttpClient,
    apub_proxy_rewrites: bool,
) -> Result<Verified<KnownObject>, crate::Error> {
    let req_body = hyper::body::to_bytes(req.body_mut()).await?;

    match req.headers().get("signature") {
        None => {
            let obj: JustMaybeAPID = serde_json::from_slice(&req_body)?;
            let ap_id = obj.id.ok_or_else(|| {
                crate::Error::InternalStrStatic("Missing id in received activity")
            })?;

            let res_body = fetch_ap_object(&ap_id, http_client).await?;

            Ok(res_body)
        }
        Some(signature) => {
            let obj: JustActor = serde_json::from_slice(&req_body)?;

            let actor_ap_id = if let Some(actor) = obj.actor.as_one() {
                actor.id().ok_or(crate::Error::InternalStrStatic(
                    "No id found for actor, can't verify signature",
                ))?
            } else {
                return Err(crate::Error::InternalStrStatic(
                    "Found multiple actors for activity, can't verify signature",
                ));
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
                Ok(Verified(serde_json::from_slice(&req_body)?))
            } else {
                Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::FORBIDDEN,
                    "Signature check failed",
                )))
            }
        }
    }
}
