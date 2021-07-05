use crate::{BaseURL, CommentLocalID, CommunityLocalID, PostLocalID, UserLocalID};
use activitystreams::prelude::*;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;

pub mod ingest;

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
    signer.sign_to_vec()
}

pub fn do_verify(
    key: &openssl::pkey::PKey<openssl::pkey::Public>,
    alg: openssl::hash::MessageDigest,
    src: &[u8],
    sig: &[u8],
) -> Result<bool, openssl::error::ErrorStack> {
    let mut verifier = openssl::sign::Verifier::new(alg, &key)?;
    verifier.update(&src)?;
    verifier.verify(sig)
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

    pub fn as_ref(&self) -> crate::ThingLocalRef {
        match self {
            ActorLocalInfo::User { id, .. } => crate::ThingLocalRef::User(*id),
            ActorLocalInfo::Community { id, .. } => crate::ThingLocalRef::Community(*id),
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
    ctx: Arc<crate::BaseContext>,
) -> Result<ActorLocalInfo, crate::Error> {
    let obj = fetch_ap_object(req_ap_id, &ctx.http_client).await?;
    match ingest::ingest_object_boxed(obj, ingest::FoundFrom::Other, ctx).await? {
        Some(ingest::IngestResult::Actor(info)) => Ok(info),
        _ => Err(crate::Error::InternalStrStatic("Unrecognized actor type")),
    }
}

pub async fn get_or_fetch_user_local_id(
    ap_id: &url::Url,
    db: &tokio_postgres::Client,
    ctx: &Arc<crate::BaseContext>,
) -> Result<UserLocalID, crate::Error> {
    if let Some(remaining) = try_strip_host(ap_id, &ctx.host_url_apub) {
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

                let actor = fetch_actor(ap_id, ctx.clone()).await?;

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
            log::error!("Failed to create Announce: {:?}", err);
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
        log::debug!("{:?}", accept);

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
            props
                .set_content(crate::clean_html(&html))
                .set_media_type(mime::TEXT_HTML);

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
    }

    match post.href {
        Some(href) => {
            if href.starts_with("local-media://") {
                let mut attachment = activitystreams::object::Image::new();
                attachment.set_url(ctx.process_href(href, post.id).into_owned());

                let mut post_ap = activitystreams::object::Note::new();

                post_ap
                    .set_context(activitystreams::context())
                    .set_id(get_local_post_apub_id(post.id, &ctx.host_url_apub).into())
                    .set_attributed_to(get_local_person_apub_id(
                        post.author.unwrap(),
                        &ctx.host_url_apub,
                    ))
                    .set_summary(post.title)
                    .set_published(*post.created)
                    .set_to(community_ap_id)
                    .set_cc(activitystreams::public())
                    .add_attachment(attachment.into_any_base()?);

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
    ctx: &crate::BaseContext,
) -> Result<activitystreams::object::ApObject<activitystreams::object::Note>, crate::Error> {
    let mut obj = activitystreams::object::Note::new();

    obj.set_context(activitystreams::context())
        .set_id(get_local_comment_apub_id(comment.id, &ctx.host_url_apub).into())
        .set_attributed_to(url::Url::from(get_local_person_apub_id(
            comment.author.unwrap(),
            &ctx.host_url_apub,
        )))
        .set_published(comment.created)
        .set_in_reply_to(parent_ap_id.unwrap_or_else(|| post_ap_id.clone()));

    if let Some(attachment_href) = ctx.process_attachments_inner(
        comment.attachment_href.as_deref().map(Cow::Borrowed),
        comment.id,
    ) {
        let mut attachment = activitystreams::object::Image::new();
        attachment.set_url(attachment_href.into_owned());

        obj.add_attachment(attachment.into_any_base()?);
    }

    let mut obj = activitystreams::object::ApObject::new(obj);

    if let Some(html) = &comment.content_html {
        obj.set_content(crate::clean_html(&html))
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
    ctx: &crate::BaseContext,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let comment_ap = local_comment_to_ap(
        &comment,
        post_ap_id,
        parent_ap_id,
        parent_or_post_author_ap_id.clone(),
        community_ap_id.clone(),
        ctx,
    )?;

    let author = comment.author.unwrap();

    let mut create = activitystreams::activity::Create::new(
        get_local_person_apub_id(author, &ctx.host_url_apub),
        comment_ap.into_any_base()?,
    );
    create.set_context(activitystreams::context()).set_id({
        let mut res = get_local_comment_apub_id(comment.id, &ctx.host_url_apub);
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
        &ctx,
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
    ctx: &Arc<crate::BaseContext>,
) -> Result<bool, crate::Error> {
    let found_key = db.query_opt("(SELECT public_key, public_key_sigalg FROM person WHERE ap_id=$1) UNION ALL (SELECT public_key, public_key_sigalg FROM community WHERE ap_id=$1) LIMIT 1", &[&actor_ap_id.as_str()]).await?
        .and_then(|row| {
            row.get::<_, Option<&[u8]>>(0).map(|key| {
                openssl::pkey::PKey::public_key_from_pem(key)
                    .map(|key| (key, get_message_digest(row.get(1))))
            })
        }).transpose()?;

    log::debug!("signature: {:?}", signature);
    log::debug!("found_key: {:?}", found_key.is_some());

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

    let actor = fetch_actor(actor_ap_id, ctx.clone()).await?;

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
    ctx: &Arc<crate::BaseContext>,
) -> Result<Verified<KnownObject>, crate::Error> {
    let req_body = hyper::body::to_bytes(req.body_mut()).await?;

    match req.headers().get("signature") {
        None => {
            let obj: JustMaybeAPID = serde_json::from_slice(&req_body)?;
            let ap_id = obj.id.ok_or(crate::Error::InternalStrStatic(
                "Missing id in received activity",
            ))?;

            let res_body = fetch_ap_object(&ap_id, &ctx.http_client).await?;

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
            let path_and_query = if ctx.apub_proxy_rewrites {
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
                &ctx,
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
