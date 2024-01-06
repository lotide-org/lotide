use crate::types::{
    ActorLocalRef, CommentLocalID, CommunityLocalID, FingerRequestQuery, FingerResponse,
    FlagLocalID, ImageHandling, PollLocalID, PollOptionLocalID, PostLocalID, ThingLocalRef,
    UserLocalID,
};
use crate::BaseURL;
use activitystreams::prelude::*;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};
use std::ops::Deref;
use std::sync::Arc;

pub mod ingest;
pub mod local_object_ref;

pub use local_object_ref::LocalObjectRef;

pub const ACTIVITY_TYPE: &str =
    "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"";
pub const ACTIVITY_TYPE_ALT: &str = "application/activity+json";

pub const ACTIVITY_TYPE_HEADER_VALUE: &str = "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\", application/activity+json";

pub const SIGALG_RSA_SHA256: &str = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256";
pub const SIGALG_RSA_SHA512: &str = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha512";

pub const INTERACTIVE_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

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

impl<T: Clone, U: Clone> From<Verified<activitystreams_ext::Ext1<T, U>>> for Verified<T> {
    fn from(src: Verified<activitystreams_ext::Ext1<T, U>>) -> Self {
        Verified(src.0.inner)
    }
}

impl<T: Clone, U1: Clone, U2: Clone> From<Verified<activitystreams_ext::Ext2<T, U1, U2>>>
    for Verified<T>
{
    fn from(src: Verified<activitystreams_ext::Ext2<T, U1, U2>>) -> Self {
        Verified(src.0.inner)
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
    Add(activitystreams::activity::Add),
    Announce(activitystreams::activity::Announce),
    Create(activitystreams::activity::Create),
    Delete(activitystreams::activity::Delete),
    Flag(activitystreams::activity::Flag),
    Follow(activitystreams::activity::Follow),
    Join(activitystreams::activity::Join),
    Leave(activitystreams::activity::Leave),
    Like(activitystreams::activity::Like),
    Undo(activitystreams::activity::Undo),
    Update(activitystreams::activity::Update),
    Person(
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<activitystreams::actor::Person>,
            PublicKeyExtension<'static>,
        >,
    ),
    Remove(activitystreams::activity::Remove),
    Service(
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<activitystreams::actor::Service>,
            PublicKeyExtension<'static>,
        >,
    ),
    Group(
        activitystreams_ext::Ext2<
            activitystreams::actor::ApActor<activitystreams::actor::Group>,
            PublicKeyExtension<'static>,
            FeaturedExtension,
        >,
    ),
    Article(ExtendedPostlike<activitystreams::object::Article>),
    Image(ExtendedPostlike<activitystreams::object::Image>),
    Page(ExtendedPostlike<activitystreams::object::Page>),
    Note(ExtendedPostlike<activitystreams::object::Note>),
    Question(ExtendedPostlike<activitystreams::activity::Question>),
}

impl KnownObject {
    pub fn id(&self) -> Option<&url::Url> {
        match self {
            KnownObject::Accept(obj) => obj.id_unchecked(),
            KnownObject::Add(obj) => obj.id_unchecked(),
            KnownObject::Announce(obj) => obj.id_unchecked(),
            KnownObject::Create(obj) => obj.id_unchecked(),
            KnownObject::Delete(obj) => obj.id_unchecked(),
            KnownObject::Flag(obj) => obj.id_unchecked(),
            KnownObject::Follow(obj) => obj.id_unchecked(),
            KnownObject::Join(obj) => obj.id_unchecked(),
            KnownObject::Leave(obj) => obj.id_unchecked(),
            KnownObject::Like(obj) => obj.id_unchecked(),
            KnownObject::Undo(obj) => obj.id_unchecked(),
            KnownObject::Update(obj) => obj.id_unchecked(),
            KnownObject::Person(obj) => obj.id_unchecked(),
            KnownObject::Remove(obj) => obj.id_unchecked(),
            KnownObject::Service(obj) => obj.id_unchecked(),
            KnownObject::Group(obj) => obj.id_unchecked(),
            KnownObject::Article(obj) => obj.id_unchecked(),
            KnownObject::Image(obj) => obj.id_unchecked(),
            KnownObject::Page(obj) => obj.id_unchecked(),
            KnownObject::Note(obj) => obj.id_unchecked(),
            KnownObject::Question(obj) => obj.id_unchecked(),
        }
    }
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FeaturedExtension {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub featured: Option<url::Url>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TargetExtension {
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct SensitiveExtension {
    sensitive: Option<bool>,
}

pub type ExtendedPostlike<T> = activitystreams_ext::Ext2<T, TargetExtension, SensitiveExtension>;

pub fn make_extended_postlike<T>(src: T) -> ExtendedPostlike<T> {
    ExtendedPostlike::new(src, Default::default(), Default::default())
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum AnyCollection {
    Unordered(activitystreams::collection::UnorderedCollection),
    Ordered(activitystreams::collection::OrderedCollection),
}

impl AnyCollection {
    pub fn total_items(&self) -> Option<u64> {
        match self {
            AnyCollection::Unordered(coll) => coll.total_items(),
            AnyCollection::Ordered(coll) => coll.total_items(),
        }
    }
}

#[derive(Clone)]
pub enum FollowLike {
    Follow(activitystreams::activity::Follow),
    Join(activitystreams::activity::Join),
}

impl activitystreams::markers::Base for FollowLike {}

impl FollowLike {
    pub fn id_unchecked(&self) -> Option<&url::Url> {
        match self {
            FollowLike::Follow(follow) => follow.id_unchecked(),
            FollowLike::Join(join) => join.id_unchecked(),
        }
    }

    pub fn take_id(&mut self) -> Option<url::Url> {
        match self {
            FollowLike::Follow(follow) => follow.take_id(),
            FollowLike::Join(join) => join.take_id(),
        }
    }

    pub fn object(
        &self,
    ) -> &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase> {
        match self {
            FollowLike::Follow(follow) => follow.object(),
            FollowLike::Join(join) => join.object(),
        }
    }

    pub fn actor_unchecked(
        &self,
    ) -> &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase> {
        match self {
            FollowLike::Follow(follow) => follow.actor_unchecked(),
            FollowLike::Join(join) => join.actor_unchecked(),
        }
    }
}

pub fn try_strip_host<'a>(url: &'a impl AsRef<str>, host_url: &url::Url) -> Option<&'a str> {
    let host_url = host_url.as_str();
    let host_url = host_url.trim_end_matches('/');

    let url = url.as_ref();

    url.strip_prefix(host_url)
}

pub fn get_local_person_pubkey_apub_id(person: UserLocalID, host_url_apub: &BaseURL) -> BaseURL {
    let mut res = LocalObjectRef::User(person).to_local_uri(host_url_apub);
    res.set_fragment(Some("main-key"));
    res
}

pub fn get_local_community_pubkey_apub_id(
    community: CommunityLocalID,
    host_url_apub: &BaseURL,
) -> BaseURL {
    let mut res = LocalObjectRef::Community(community).to_local_uri(host_url_apub);
    res.set_fragment(Some("main-key"));
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
    let mut signer = openssl::sign::Signer::new(openssl::hash::MessageDigest::sha256(), key)?;
    signer.update(src)?;
    signer.sign_to_vec()
}

pub fn do_verify(
    key: &openssl::pkey::PKey<openssl::pkey::Public>,
    alg: openssl::hash::MessageDigest,
    src: &[u8],
    sig: &[u8],
) -> Result<bool, openssl::error::ErrorStack> {
    let mut verifier = openssl::sign::Verifier::new(alg, key)?;
    verifier.update(src)?;
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
        remote_url: url::Url,
    },
    Community {
        id: CommunityLocalID,
        public_key: Option<PubKeyInfo>,
        ap_outbox: Option<url::Url>,
    },
}

impl ActorLocalInfo {
    pub fn public_key(&self) -> Option<&PubKeyInfo> {
        match self {
            ActorLocalInfo::User { public_key, .. } => public_key.as_ref(),
            ActorLocalInfo::Community { public_key, .. } => public_key.as_ref(),
        }
    }

    pub fn as_ref(&self) -> ThingLocalRef {
        match self {
            ActorLocalInfo::User { id, .. } => ThingLocalRef::User(*id),
            ActorLocalInfo::Community { id, .. } => ThingLocalRef::Community(*id),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Incoming object failed containment check")]
pub struct NotContained;

pub fn is_contained(object_id: &url::Url, actor_id: &url::Url) -> bool {
    object_id.host() == actor_id.host() && object_id.port() == actor_id.port()
}

pub fn require_containment(object_id: &url::Url, actor_id: &url::Url) -> Result<(), NotContained> {
    if is_contained(object_id, actor_id) {
        Ok(())
    } else {
        Err(NotContained)
    }
}

pub async fn fetch_ap_object_raw(
    ap_id: &url::Url,
    ctx: &crate::BaseContext,
) -> Result<serde_json::Value, crate::Error> {
    let mut current_id = hyper::Uri::try_from(ap_id.as_str())?;
    for _ in 0..3u8 {
        if current_id.scheme() != Some(&http::uri::Scheme::HTTPS) && !ctx.dev_mode {
            return Err(crate::Error::InternalStrStatic(
                "AP URLs must be HTTPS in non-dev mode",
            ));
        }
        // avoid infinite loop in malicious or broken cases
        let res = crate::res_to_error(
            ctx.http_client
                .request(
                    hyper::Request::get(&current_id)
                        .header(hyper::header::USER_AGENT, &ctx.user_agent)
                        .header(hyper::header::ACCEPT, ACTIVITY_TYPE_HEADER_VALUE)
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
    ctx: &crate::BaseContext,
) -> Result<Verified<KnownObject>, crate::Error> {
    let value = fetch_ap_object_raw(ap_id, ctx).await?;
    let value: KnownObject = serde_json::from_value(value)?;
    Ok(Verified(value))
}

pub async fn fetch_or_verify(
    sender_ap_id: &url::Url,
    obj: activitystreams::base::AnyBase,
    ctx: &crate::BaseContext,
    for_inbox: bool,
) -> Result<Verified<KnownObject>, crate::Error> {
    let object_id = obj
        .id()
        .ok_or(crate::Error::InternalStrStatic("Missing ID in object"))?;
    if is_contained(object_id, sender_ap_id) {
        if let Some(base) = obj.as_base() {
            return Ok(serde_json::from_value(serde_json::to_value(base)?)
                .map(Verified)
                .map_err(|err| {
                    if for_inbox {
                        log::debug!("Failed to parse inner object: {:?}", err);
                        crate::Error::UserError(crate::simple_response(
                            hyper::StatusCode::BAD_REQUEST,
                            "Invalid or unsupported data",
                        ))
                    } else {
                        err.into()
                    }
                })?);
        }
    }

    fetch_ap_object(&object_id, ctx).await
}

pub async fn fetch_and_ingest(
    req_ap_id: &url::Url,
    found_from: ingest::FoundFrom,
    ctx: Arc<crate::BaseContext>,
) -> Result<Option<ingest::IngestResult>, crate::Error> {
    let obj = fetch_ap_object(req_ap_id, &ctx).await?;
    ingest::ingest_object_boxed(obj, found_from, ctx, false).await
}

pub async fn fetch_actor(
    req_ap_id: &url::Url,
    ctx: Arc<crate::BaseContext>,
) -> Result<ActorLocalInfo, crate::Error> {
    match fetch_and_ingest(req_ap_id, ingest::FoundFrom::Other, ctx).await? {
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
        if let Some(LocalObjectRef::User(id)) = LocalObjectRef::try_from_path(remaining) {
            Ok(id)
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
    actor_ref: ActorLocalRef,
    db: &tokio_postgres::Client,
    host_url_apub: &BaseURL,
) -> Result<(openssl::pkey::PKey<openssl::pkey::Private>, BaseURL), crate::Error> {
    Ok(match actor_ref {
        ActorLocalRef::Person(id) => (
            fetch_or_create_local_user_privkey(id, db).await?,
            get_local_person_pubkey_apub_id(id, host_url_apub),
        ),
        ActorLocalRef::Community(id) => (
            fetch_or_create_local_community_privkey(id, db).await?,
            get_local_community_pubkey_apub_id(id, host_url_apub),
        ),
    })
}

pub fn spawn_enqueue_fetch_community_featured(
    community: CommunityLocalID,
    featured_url: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        ctx.enqueue_task(&crate::tasks::FetchCommunityFeatured {
            community_id: community,
            featured_url,
        })
        .await
    });
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

        let person_ap_id = LocalObjectRef::User(local_follower).to_local_uri(&ctx.host_url_apub);

        let mut follow =
            activitystreams::activity::Follow::new(person_ap_id.clone(), community_ap_id.clone());
        follow
            .set_context(activitystreams::context())
            .set_id(
                LocalObjectRef::CommunityFollow(community, local_follower)
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            )
            .set_to(community_ap_id.clone());

        let mut join = activitystreams::activity::Join::new(person_ap_id, community_ap_id.clone());
        join.set_context(activitystreams::context())
            .set_id(
                LocalObjectRef::CommunityFollowJoin(community, local_follower)
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            )
            .set_to(community_ap_id);

        std::mem::drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Borrowed(&community_inbox),
            sign_as: Some(ActorLocalRef::Person(local_follower)),
            object: serde_json::to_string(&follow)?,
        })
        .await?;

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(community_inbox),
            sign_as: Some(ActorLocalRef::Person(local_follower)),
            object: serde_json::to_string(&join)?,
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
        let (community_inbox, community_ap_id): (url::Url, url::Url) = {
            let db = ctx.db_pool.get().await?;

            let row = db
                .query_one(
                    "SELECT local, ap_inbox, ap_id FROM community WHERE id=$1",
                    &[&community_local_id],
                )
                .await?;
            let local = row.get(0);
            if local {
                // no need to send follow state to ourself
                return Ok(());
            } else {
                let ap_inbox: Option<&str> = row.get(1);
                let ap_id: Option<&str> = row.get(2);

                (
                    ap_inbox
                        .ok_or_else(|| {
                            crate::Error::InternalStr(format!(
                                "Missing apub info for community {}",
                                community_local_id,
                            ))
                        })?
                        .parse()?,
                    ap_id
                        .ok_or_else(|| {
                            crate::Error::InternalStr(format!(
                                "Missing apub info for community {}",
                                community_local_id,
                            ))
                        })?
                        .parse()?,
                )
            }
        };

        let undo = local_community_follow_undo_to_ap(
            undo_id,
            community_local_id,
            community_ap_id,
            local_follower,
            &ctx.host_url_apub,
        )?;

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(community_inbox),
            sign_as: Some(ActorLocalRef::Person(local_follower)),
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
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

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

pub fn local_community_post_add_ap(
    community_id: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Add, crate::Error> {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let mut add = activitystreams::activity::Add::new(community_ap_id.clone(), post_ap_id);

    add.set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id.clone();
            res.path_segments_mut()
                .extend(&["posts", &post_local_id.to_string(), "add"]);
            res.into()
        })
        .set_target(LocalObjectRef::CommunityOutbox(community_id).to_local_uri(host_url_apub))
        .set_to({
            let mut res = community_ap_id;
            res.path_segments_mut().push("followers");
            res
        })
        .set_cc(activitystreams::public());

    Ok(add)
}

pub fn local_community_post_add_undo_ap(
    community_id: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    uuid: &uuid::Uuid,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let add = local_community_post_add_ap(community_id, post_local_id, post_ap_id, host_url_apub)?;

    let mut undo =
        activitystreams::activity::Undo::new(community_ap_id.clone(), add.into_any_base()?);

    undo.set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id.clone();
            res.path_segments_mut().extend(&[
                "posts",
                &post_local_id.to_string(),
                "add",
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

pub fn local_community_post_announce_undo_ap(
    community_id: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    uuid: &uuid::Uuid,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

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
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let mut announce =
        activitystreams::activity::Announce::new(community_ap_id.deref().clone(), comment_ap_id);

    announce
        .set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id;
            res.path_segments_mut().extend(&[
                "comments",
                &comment_local_id.to_string(),
                "announce",
            ]);
            res.into()
        })
        .set_to(LocalObjectRef::CommunityFollowers(community_id).to_local_uri(host_url_apub))
        .set_cc(activitystreams::public());

    Ok(announce)
}

pub fn spawn_announce_community_post(
    community: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
    match local_community_post_announce_ap(
        community,
        post_local_id,
        post_ap_id.clone(),
        &ctx.host_url_apub,
    ) {
        Err(err) => {
            log::error!("Failed to create Announce: {:?}", err);
        }
        Ok(announce) => {
            crate::spawn_task(enqueue_send_to_community_followers(
                community,
                announce,
                ctx.clone(),
            ));
        }
    }
    match local_community_post_add_ap(community, post_local_id, post_ap_id, &ctx.host_url_apub) {
        Err(err) => {
            log::error!("Failed to create Add: {:?}", err);
        }
        Ok(add) => {
            crate::spawn_task(enqueue_send_to_community_followers(community, add, ctx));
        }
    }
}

pub fn spawn_enqueue_send_community_post_announce_undo(
    community: CommunityLocalID,
    post: PostLocalID,
    post_ap_id: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
    {
        let ctx = ctx.clone();
        let post_ap_id = post_ap_id.clone();

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

    crate::spawn_task(async move {
        let undo = local_community_post_add_undo_ap(
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
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let mut update =
        activitystreams::activity::Update::new(community_ap_id.clone(), community_ap_id.clone());

    update
        .set_id({
            let mut res = community_ap_id;
            res.path_segments_mut()
                .extend(&["updates", &update_id.to_string()]);
            res.into()
        })
        .set_to(LocalObjectRef::CommunityFollowers(community_id).to_local_uri(host_url_apub))
        .set_cc(activitystreams::public());

    Ok(update)
}

pub fn local_community_delete_to_ap(
    community_id: CommunityLocalID,
    host_url_apub: &BaseURL,
) -> activitystreams::activity::Delete {
    let community_ap_id = LocalObjectRef::Community(community_id).to_local_uri(host_url_apub);

    let mut delete =
        activitystreams::activity::Delete::new(community_ap_id.clone(), community_ap_id.clone());
    delete
        .set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id;
            res.path_segments_mut().push("delete");
            res.into()
        })
        .set_to(LocalObjectRef::CommunityFollowers(community_id).to_local_uri(host_url_apub))
        .set_cc(activitystreams::public());

    delete
}

pub fn local_community_follow_undo_to_ap(
    undo_id: uuid::Uuid,
    community_local_id: CommunityLocalID,
    community_ap_id: url::Url,
    local_follower: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(local_follower).to_local_uri(host_url_apub),
        LocalObjectRef::CommunityFollow(community_local_id, local_follower)
            .to_local_uri(host_url_apub),
    );
    undo.set_context(activitystreams::context())
        .set_id({
            let mut res = host_url_apub.clone();
            res.path_segments_mut()
                .extend(&["community_follow_undos", &undo_id.to_string()]);
            res.into()
        })
        .set_to(community_ap_id);

    Ok(undo)
}

pub fn community_follow_accept_to_ap(
    community_ap_id: BaseURL,
    follower_local_id: UserLocalID,
    follower_ap_id: url::Url,
    follow_ap_id: url::Url,
) -> Result<activitystreams::activity::Accept, crate::Error> {
    let mut accept = activitystreams::activity::Accept::new(community_ap_id.clone(), follow_ap_id);

    accept
        .set_context(activitystreams::context())
        .set_id({
            let mut res = community_ap_id;
            res.path_segments_mut().extend(&[
                "followers",
                &follower_local_id.to_string(),
                "accept",
            ]);
            res.into()
        })
        .set_to(follower_ap_id);

    Ok(accept)
}

pub fn spawn_enqueue_send_community_follow_accept(
    local_community: CommunityLocalID,
    follower: UserLocalID,
    follow: Contained<'static, FollowLike>,
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

        let community_ap_id =
            LocalObjectRef::Community(local_community).to_local_uri(&ctx.host_url_apub);

        let (follower_inbox, follower_ap_id) = {
            let row = db
                .query_one(
                    "SELECT local, ap_inbox, ap_id FROM person WHERE id=$1",
                    &[&follower],
                )
                .await?;

            let local = row.get(0);
            if local {
                // Shouldn't happen, but fine to ignore it
                return Ok(());
            } else {
                let ap_inbox: Option<&str> = row.get(1);
                let ap_id: Option<&str> = row.get(2);

                (
                    ap_inbox
                        .ok_or_else(|| {
                            crate::Error::InternalStr(format!(
                                "Missing apub info for user {}",
                                follower
                            ))
                        })?
                        .parse()?,
                    ap_id
                        .ok_or_else(|| {
                            crate::Error::InternalStr(format!(
                                "Missing apub info for user {}",
                                follower
                            ))
                        })?
                        .parse()?,
                )
            }
        };

        let accept =
            community_follow_accept_to_ap(community_ap_id, follower, follower_ap_id, follow_ap_id)?;
        log::debug!("{:?}", accept);

        let body = serde_json::to_string(&accept)?;

        std::mem::drop(db);

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(follower_inbox),
            sign_as: Some(ActorLocalRef::Community(local_community)),
            object: body,
        })
        .await?;

        Ok(())
    });
}

pub fn post_to_ap(
    post: &crate::PostInfo<'_>,
    community_ap_id: url::Url,
    community_ap_outbox: Option<url::Url>,
    community_ap_followers: Option<url::Url>,
    ctx: &crate::BaseContext,
) -> Result<activitystreams::base::AnyBase, crate::Error> {
    fn apply_properties<
        K,
        O: activitystreams::object::ObjectExt<K> + activitystreams::base::BaseExt<K>,
    >(
        props: &mut ExtendedPostlike<activitystreams::object::ApObject<O>>,
        post: &crate::PostInfo,
        community_ap_id: url::Url,
        community_ap_outbox: Option<url::Url>,
        community_ap_followers: Option<url::Url>,
        ctx: &crate::BaseContext,
    ) -> Result<(), crate::Error> {
        props
            .set_id(
                LocalObjectRef::Post(post.id)
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            )
            .set_context(activitystreams::context())
            .set_attributed_to(
                LocalObjectRef::User(post.author.unwrap()).to_local_uri(&ctx.host_url_apub),
            )
            .set_published(post.created)
            .set_to(community_ap_id)
            .set_cc(activitystreams::public());

        if let Some(community_ap_followers) = community_ap_followers {
            props.add_to(community_ap_followers);
        }

        if let Some(community_ap_outbox) = community_ap_outbox {
            props.ext_one.target = Some(activitystreams::primitives::OneOrMany::from_xsd_any_uri(
                community_ap_outbox,
            ));
        }

        props.ext_two.sensitive = Some(post.sensitive);

        if let Some(html) = post.content_html {
            props
                .set_content(crate::clean_html(html, ImageHandling::Preserve))
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

        for mention in post.mentions {
            let mentioned_ap_id = match &mention.ap_id {
                crate::APIDOrLocal::APID(apid) => apid.clone(),
                crate::APIDOrLocal::Local => crate::apub_util::LocalObjectRef::User(mention.person)
                    .to_local_uri(&ctx.host_url_apub)
                    .into(),
            };

            let mut tag = activitystreams::link::Mention::new();

            tag.set_href(mentioned_ap_id.clone());
            tag.set_name(mention.text.clone());

            props.add_tag(tag.into_any_base()?);

            props.add_cc(mentioned_ap_id);
        }

        Ok(())
    }

    match (post.poll.as_ref(), post.href) {
        (Some(poll), _) => {
            // theoretically href and poll are mutually exclusive

            let mut post_ap = activitystreams::activity::Question::new();

            post_ap.set_summary(post.title).set_name(post.title);

            let options: Vec<activitystreams::base::AnyBase> = poll
                .options
                .iter()
                .map(|option| {
                    let mut option_ap = activitystreams::object::Note::new();
                    option_ap.set_name(option.name);

                    let mut replies_ap = activitystreams::collection::UnorderedCollection::new();
                    replies_ap.set_total_items(option.votes);
                    option_ap.set_reply(replies_ap.into_any_base()?);

                    option_ap.into_any_base()
                })
                .collect::<Result<_, _>>()?;

            if poll.multiple {
                post_ap.set_many_any_ofs(options);
            } else {
                post_ap.set_many_one_ofs(options);
            }

            if let Some(closed_at) = poll.closed_at {
                post_ap.set_closed(closed_at.clone());
            }

            let mut post_ap =
                make_extended_postlike(activitystreams::object::ApObject::new(post_ap));

            apply_properties(
                &mut post_ap,
                post,
                community_ap_id,
                community_ap_outbox,
                community_ap_followers,
                &ctx,
            )?;

            Ok(activitystreams::base::AnyBase::from_arbitrary_json(
                post_ap,
            )?)
        }
        (None, Some(href)) => {
            if href.starts_with("local-media://") {
                let mut attachment = activitystreams::object::Image::new();
                attachment.set_url(ctx.process_href(href, post.id).into_owned());

                let mut post_ap = activitystreams::object::Note::new();

                post_ap
                    .set_summary(post.title)
                    .set_name(post.title)
                    .add_attachment(attachment.into_any_base()?);

                let mut post_ap =
                    make_extended_postlike(activitystreams::object::ApObject::new(post_ap));

                apply_properties(
                    &mut post_ap,
                    post,
                    community_ap_id,
                    community_ap_outbox,
                    community_ap_followers,
                    &ctx,
                )?;

                Ok(activitystreams::base::AnyBase::from_arbitrary_json(
                    post_ap,
                )?)
            } else {
                let mut attachment =
                    activitystreams::link::Link::<activitystreams::link::kind::LinkType>::new();
                attachment.set_href(url::Url::try_from(href)?);

                let mut post_ap = activitystreams::object::Note::new();

                post_ap
                    .set_summary(post.title)
                    .set_name(post.title)
                    .add_attachment(attachment.into_any_base()?);

                let mut post_ap =
                    make_extended_postlike(activitystreams::object::ApObject::new(post_ap));

                apply_properties(
                    &mut post_ap,
                    post,
                    community_ap_id,
                    community_ap_outbox,
                    community_ap_followers,
                    &ctx,
                )?;

                Ok(activitystreams::base::AnyBase::from_arbitrary_json(
                    post_ap,
                )?)
            }
        }
        (None, None) => {
            let mut post_ap = activitystreams::object::Note::new();

            post_ap.set_summary(post.title).set_name(post.title);

            let mut post_ap =
                make_extended_postlike(activitystreams::object::ApObject::new(post_ap));

            apply_properties(
                &mut post_ap,
                post,
                community_ap_id,
                community_ap_outbox,
                community_ap_followers,
                &ctx,
            )?;

            Ok(activitystreams::base::AnyBase::from_arbitrary_json(
                post_ap,
            )?)
        }
    }
}

pub fn local_post_to_create_ap(
    post: &crate::PostInfo<'_>,
    community_ap_id: url::Url,
    community_ap_outbox: Option<url::Url>,
    community_ap_followers: Option<url::Url>,
    ctx: &crate::BaseContext,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let post_ap = post_to_ap(
        post,
        community_ap_id.clone(),
        community_ap_outbox,
        community_ap_followers.clone(),
        ctx,
    )?;

    let mut create = activitystreams::activity::Create::new(
        LocalObjectRef::User(post.author.unwrap()).to_local_uri(&ctx.host_url_apub),
        post_ap,
    );
    create.set_context(activitystreams::context()).set_id({
        let mut res = LocalObjectRef::Post(post.id).to_local_uri(&ctx.host_url_apub);
        res.path_segments_mut().push("create");
        res.into()
    });
    create.set_to(community_ap_id);
    create.set_cc(activitystreams::public());

    if let Some(community_ap_followers) = community_ap_followers {
        create.add_to(community_ap_followers);
    }

    Ok(create)
}

pub fn local_comment_to_ap(
    comment: &crate::CommentInfo,
    post_ap_id: &url::Url,
    parent_ap_id: Option<url::Url>,
    parent_or_post_author_ap_id: Option<url::Url>,
    community_ap_id: url::Url,
    ctx: &crate::BaseContext,
) -> Result<
    activitystreams_ext::Ext1<
        activitystreams::object::ApObject<activitystreams::object::Note>,
        SensitiveExtension,
    >,
    crate::Error,
> {
    let mut obj = activitystreams::object::Note::new();

    obj.set_context(activitystreams::context())
        .set_id(
            LocalObjectRef::Comment(comment.id)
                .to_local_uri(&ctx.host_url_apub)
                .into(),
        )
        .set_attributed_to(url::Url::from(
            LocalObjectRef::User(comment.author.unwrap()).to_local_uri(&ctx.host_url_apub),
        ))
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
        obj.set_content(crate::clean_html(html, ImageHandling::Preserve))
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

    for mention in comment.mentions.as_ref() {
        let mentioned_ap_id = match &mention.ap_id {
            crate::APIDOrLocal::APID(apid) => apid.clone(),
            crate::APIDOrLocal::Local => crate::apub_util::LocalObjectRef::User(mention.person)
                .to_local_uri(&ctx.host_url_apub)
                .into(),
        };

        let mut tag = activitystreams::link::Mention::new();

        tag.set_href(mentioned_ap_id.clone());
        tag.set_name(mention.text.clone());

        obj.add_tag(tag.into_any_base()?);

        obj.add_cc(mentioned_ap_id);
    }

    Ok(activitystreams_ext::Ext1::new(
        obj,
        SensitiveExtension {
            sensitive: Some(comment.sensitive),
        },
    ))
}

pub fn spawn_enqueue_send_local_post_to_community(
    post: crate::PostInfoOwned,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let (community_ap_id, community_inbox, community_outbox, community_followers): (
            url::Url,
            url::Url,
            Option<url::Url>,
            Option<url::Url>,
        ) = {
            let row = db
                .query_one(
                    "SELECT local, ap_id, COALESCE(ap_shared_inbox, ap_inbox), ap_outbox, ap_followers FROM community WHERE id=$1",
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
                let ap_outbox: Option<&str> = row.get(3);
                let ap_followers: Option<&str> = row.get(4);

                (if let Some(ap_id) = ap_id {
                    if let Some(ap_inbox) = ap_inbox {
                        Some((
                            ap_id.parse()?,
                            ap_inbox.parse()?,
                            ap_outbox.and_then(|x| x.parse().ok()),
                            ap_followers.and_then(|x| x.parse().ok()),
                        ))
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

        let create = local_post_to_create_ap(
            &(&post).into(),
            community_ap_id,
            community_outbox,
            community_followers,
            &ctx,
        )?;

        ctx.enqueue_task(&crate::tasks::DeliverToInbox {
            inbox: Cow::Owned(community_inbox),
            sign_as: Some(ActorLocalRef::Person(post.author.unwrap())),
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
    let post_ap_id = LocalObjectRef::Post(post_id).to_local_uri(host_url_apub);
    let mut delete = activitystreams::activity::Delete::new(
        LocalObjectRef::User(author).to_local_uri(host_url_apub),
        post_ap_id.clone(),
    );
    delete
        .set_context(activitystreams::context())
        .set_id({
            let mut res = post_ap_id;
            res.path_segments_mut().push("delete");
            res.into()
        })
        .set_to(activitystreams::public());

    Ok(delete)
}

pub fn local_comment_delete_to_ap(
    comment_id: CommentLocalID,
    author: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Delete, crate::Error> {
    let comment_ap_id = LocalObjectRef::Comment(comment_id).to_local_uri(host_url_apub);

    let mut delete = activitystreams::activity::Delete::new(
        LocalObjectRef::User(author).to_local_uri(host_url_apub),
        comment_ap_id.clone(),
    );

    delete
        .set_context(activitystreams::context())
        .set_id({
            let mut res = comment_ap_id;
            res.path_segments_mut().push("delete");
            res.into()
        })
        .set_to(activitystreams::public());

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
        comment,
        post_ap_id,
        parent_ap_id,
        parent_or_post_author_ap_id.clone(),
        community_ap_id.clone(),
        ctx,
    )?;

    let author = comment.author.unwrap();

    let mut create = activitystreams::activity::Create::new(
        LocalObjectRef::User(author).to_local_uri(&ctx.host_url_apub),
        activitystreams::base::AnyBase::from_arbitrary_json(comment_ap)?,
    );
    create.set_context(activitystreams::context()).set_id({
        let mut res = LocalObjectRef::Comment(comment.id).to_local_uri(&ctx.host_url_apub);
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

pub fn local_post_flag_to_ap(
    flag_local_id: FlagLocalID,
    content_text: Option<&str>,
    user_id: UserLocalID,
    post_ap_id: BaseURL,
    community_info: Option<&(CommunityLocalID, bool, Option<BaseURL>)>,
    to_community: bool,
    host_url_apub: &BaseURL,
) -> activitystreams::activity::Flag {
    let mut flag = activitystreams::activity::Flag::new(
        crate::apub_util::LocalObjectRef::User(user_id).to_local_uri(host_url_apub),
        post_ap_id,
    );

    flag.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["flags", &flag_local_id.to_string()]);
        res.into()
    });

    if let Some(content_text) = content_text {
        flag.set_content(content_text);
    }

    if to_community {
        if let Some((_, _, community_ap_id)) = community_info {
            if let Some(community_ap_id) = community_ap_id {
                flag.set_to(community_ap_id.deref().clone());
            }
        }
    }

    flag
}

pub fn local_post_like_to_ap(
    post_local_id: PostLocalID,
    post_ap_id: BaseURL,
    author_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Like, crate::Error> {
    let mut like = activitystreams::activity::Like::new(
        crate::apub_util::LocalObjectRef::User(user).to_local_uri(host_url_apub),
        post_ap_id,
    );
    like.set_context(activitystreams::context()).set_id(
        LocalObjectRef::PostLike(post_local_id, user)
            .to_local_uri(host_url_apub)
            .into(),
    );

    if let Some(author_ap_id) = author_ap_id {
        like.set_to(author_ap_id);
    }

    like.set_cc(activitystreams::public());

    Ok(like)
}

pub fn local_post_like_undo_to_ap(
    undo_id: uuid::Uuid,
    post_local_id: PostLocalID,
    author_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let like_ap_id = LocalObjectRef::PostLike(post_local_id, user).to_local_uri(host_url_apub);

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(user).to_local_uri(host_url_apub),
        like_ap_id,
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["post_like_undos", &undo_id.to_string()]);
        res.into()
    });

    if let Some(author_ap_id) = author_ap_id {
        undo.set_to(author_ap_id);
    }

    undo.set_cc(activitystreams::public());

    Ok(undo)
}

pub fn local_comment_like_to_ap(
    comment_local_id: CommentLocalID,
    comment_ap_id: BaseURL,
    author_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Like, crate::Error> {
    let like_ap_id =
        LocalObjectRef::CommentLike(comment_local_id, user).to_local_uri(host_url_apub);
    let mut like = activitystreams::activity::Like::new(
        crate::apub_util::LocalObjectRef::User(user).to_local_uri(host_url_apub),
        comment_ap_id,
    );
    like.set_context(activitystreams::context())
        .set_id(like_ap_id.into());

    if let Some(author_ap_id) = author_ap_id {
        like.set_to(author_ap_id);
    }

    Ok(like)
}

pub fn local_comment_like_undo_to_ap(
    undo_id: uuid::Uuid,
    comment_local_id: CommentLocalID,
    author_ap_id: Option<url::Url>,
    user: UserLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let like_ap_id =
        LocalObjectRef::CommentLike(comment_local_id, user).to_local_uri(host_url_apub);

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(user).to_local_uri(host_url_apub),
        like_ap_id,
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["comment_like_undos", &undo_id.to_string()]);
        res.into()
    });

    if let Some(author_ap_id) = author_ap_id {
        undo.set_to(author_ap_id);
    }

    Ok(undo)
}

pub fn local_poll_vote_to_ap(
    poll_id: PollLocalID,
    poll_ap_id: BaseURL,
    author_ap_id: Option<url::Url>,
    user: UserLocalID,
    option_id: PollOptionLocalID,
    option_name: String,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Create, crate::Error> {
    let id = LocalObjectRef::PollVote(poll_id, user, option_id).to_local_uri(host_url_apub);
    let note_id = {
        let mut res = id.clone();
        res.path_segments_mut().push("note");
        res
    };

    let actor = crate::apub_util::LocalObjectRef::User(user).to_local_uri(host_url_apub);

    let mut note = activitystreams::object::Note::new();
    note.set_id(note_id.into())
        .set_in_reply_to(poll_ap_id)
        .set_name(option_name)
        .set_attributed_to(actor.clone());

    if let Some(ref author_ap_id) = author_ap_id {
        note.set_to(author_ap_id.clone());
    }

    let mut create = activitystreams::activity::Create::new(actor, note.into_any_base()?);
    create
        .set_context(activitystreams::context())
        .set_id(id.into());

    if let Some(author_ap_id) = author_ap_id {
        create.set_to(author_ap_id);
    }

    Ok(create)
}

pub fn local_poll_vote_undo_to_ap(
    poll_id: PollLocalID,
    author_ap_id: Option<url::Url>,
    user: UserLocalID,
    option_id: PollOptionLocalID,
    host_url_apub: &BaseURL,
) -> Result<activitystreams::activity::Undo, crate::Error> {
    let undo_id = uuid::Uuid::new_v4(); // activity is temporary

    let mut undo = activitystreams::activity::Undo::new(
        LocalObjectRef::User(user).to_local_uri(host_url_apub),
        LocalObjectRef::PollVote(poll_id, user, option_id).to_local_uri(host_url_apub),
    );
    undo.set_context(activitystreams::context()).set_id({
        let mut res = host_url_apub.clone();
        res.path_segments_mut()
            .extend(&["tmp_objects", &undo_id.to_string()]);
        res.into()
    });

    if let Some(author_ap_id) = author_ap_id {
        undo.set_to(author_ap_id);
    }

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
                sign_as: Some(ActorLocalRef::Person(author)),
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
        actor: ActorLocalRef::Community(community_id),
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
        actor: ActorLocalRef::Community(community_id),
        sign: true,
        object: serde_json::to_string(&activity)?,
    })
    .await
}

fn get_message_digest(src: Option<&str>) -> Option<openssl::hash::MessageDigest> {
    match src {
        None | Some(SIGALG_RSA_SHA256) => Some(openssl::hash::MessageDigest::sha256()),
        Some(SIGALG_RSA_SHA512) => Some(openssl::hash::MessageDigest::sha512()),
        _ => None,
    }
}

pub async fn check_signature_for_actor(
    request: &hyper::Request<hyper::Body>,
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

    log::debug!("found_key: {:?}", found_key.is_some());

    let signatures = hancock::HttpSignature::parse_from_request(request)?;

    if let Some((key, algorithm)) = found_key {
        let algorithm = algorithm.ok_or(crate::Error::InternalStrStatic(
            "Cannot verify signature, unknown algorithm",
        ))?;

        for signature in &signatures {
            if signature.verify_request(&request, |bytes, sig| {
                log::debug!("verifying: {:?} {:?}", std::str::from_utf8(bytes), sig);
                do_verify(&key, algorithm, bytes, sig)
            })? {
                return Ok(true);
            } else {
                log::debug!("signature does not match");
            }
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

        for signature in &signatures {
            if signature.verify_request(&request, |bytes, sig| {
                do_verify(&key, algorithm, bytes, sig)
            })? {
                return Ok(true);
            }
        }

        return Ok(false);
    } else {
        Err(crate::Error::InternalStrStatic(
            "Cannot verify signature, no key found",
        ))
    }
}

pub fn check_digest(body: &[u8], digest_header: &http::header::HeaderValue) -> bool {
    let digest_header = match digest_header.to_str() {
        Ok(value) => value,
        Err(_) => {
            log::warn!("Digest header was not ASCII, ignoring");
            return true;
        }
    };

    for segment in digest_header.split(',') {
        let segment = segment.trim();

        if let Some(idx) = segment.find('=') {
            let algorithm_id = &segment[..idx].to_lowercase();
            let digest_value = &segment[(idx + 1)..];

            let expected_value = match algorithm_id.deref() {
                "sha-256" => {
                    use sha2::Digest;

                    let mut hasher = sha2::Sha256::new();
                    hasher.update(body);
                    let result = hasher.finalize();
                    Some(base64::encode(result))
                }
                "sha-512" => {
                    use sha2::Digest;

                    let mut hasher = sha2::Sha512::new();
                    hasher.update(body);
                    let result = hasher.finalize();
                    Some(base64::encode(result))
                }
                _ => None,
            };

            if let Some(expected_value) = expected_value {
                if digest_value != expected_value {
                    return false;
                }

                log::debug!("digest matches");
            }
        }
    }

    true
}

pub async fn verify_incoming_object(
    mut req: hyper::Request<hyper::Body>,
    db: &tokio_postgres::Client,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Verified<KnownObject>, crate::Error> {
    let req_body = hyper::body::to_bytes(req.body_mut()).await?;

    if req.headers().contains_key(hancock::SIGNATURE_HEADER) {
        let obj: JustActor = serde_json::from_slice(&req_body)?;

        let actor_ap_id = if let Some(actor) = obj.actor.as_one() {
            actor
                .id()
                .ok_or(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    "No id found for actor, can't verify signature",
                )))?
        } else {
            return Err(crate::Error::InternalStrStatic(
                "Found multiple actors for activity, can't verify signature",
            ));
        };

        // path ends up wrong with our recommended proxy config
        if ctx.apub_proxy_rewrites {
            if let Some(path) = req
                .headers()
                .get("x-forwarded-path")
                .map(|x| x.to_str())
                .transpose()?
            {
                let mut path_and_query = path.to_owned();
                let uri = req.uri_mut();

                let mut tmp = http::Uri::default();

                *uri = http::Uri::from_parts({
                    {
                        let query = uri.query();
                        if let Some(query) = query {
                            path_and_query.push('?');
                            path_and_query.push_str(query);
                        }
                    }

                    std::mem::swap(uri, &mut tmp);

                    let mut parts = tmp.into_parts();
                    parts.path_and_query = Some(path_and_query.try_into().unwrap());

                    parts
                })?;
            }
        }

        if check_signature_for_actor(&req, actor_ap_id, db, ctx).await? {
            if let Some(digest) = req.headers().get("digest") {
                if !check_digest(&req_body, digest) {
                    return Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        "Mismatched Digest header",
                    )));
                }
            }
            log::debug!(
                "Received remote object: {}",
                String::from_utf8_lossy(&req_body)
            );
            Ok(Verified(serde_json::from_slice(&req_body).map_err(
                |err| {
                    log::debug!("Failed to parse incoming message: {:?}", err);
                    crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        "Invalid or unsupported data",
                    ))
                },
            )?))
        } else {
            Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                "Signature check failed",
            )))
        }
    } else {
        let obj: JustMaybeAPID = serde_json::from_slice(&req_body).map_err(|_| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Unable to parse request body",
            ))
        })?;
        let ap_id = obj
            .id
            .ok_or(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                "Missing id in received activity",
            )))?;

        let res_body = fetch_ap_object(&ap_id, &ctx).await?;

        Ok(res_body)
    }
}

pub async fn fetch_from_webfinger(
    userpart: &str,
    host: &str,
    ctx: Arc<crate::BaseContext>,
) -> Result<ingest::IngestResult, crate::Error> {
    let url = fetch_url_from_webfinger(userpart, host, &ctx)
        .await?
        .ok_or(crate::Error::InternalStrStatic("No AP object found"))?;
    fetch_and_ingest(&url, ingest::FoundFrom::Other, ctx)
        .await?
        .ok_or(crate::Error::InternalStrStatic(
            "No local object produced from ingest",
        ))
}

pub async fn fetch_url_from_webfinger(
    userpart: &str,
    host: &str,
    ctx: &Arc<crate::BaseContext>,
) -> Result<Option<url::Url>, crate::Error> {
    let query = serde_urlencoded::to_string(FingerRequestQuery {
        resource: format!("acct:{}@{}", userpart, host).into(),
        rel: Some("self".into()),
    })?;

    let uri = format!("https://{}/.well-known/webfinger?{}", host, query,);
    log::debug!("{}", uri);
    let res = ctx
        .http_client
        .request(
            hyper::Request::get(uri)
                .header(hyper::header::USER_AGENT, &ctx.user_agent)
                .body(Default::default())?,
        )
        .await;

    let res = if ctx.dev_mode {
        match res {
            Ok(res) => res,
            Err(_) => {
                // In dev mode, so we try HTTP too

                let uri = format!("http://{}/.well-known/webfinger?{}", host, query,);
                log::debug!("{}", uri);
                ctx.http_client
                    .request(
                        hyper::Request::get(uri)
                            .header(hyper::header::USER_AGENT, &ctx.user_agent)
                            .body(Default::default())?,
                    )
                    .await?
            }
        }
    } else {
        res?
    };

    if res.status() == hyper::StatusCode::NOT_FOUND {
        log::debug!("not found");
        Ok(None)
    } else {
        let res = crate::res_to_error(res).await?;

        let res = hyper::body::to_bytes(res.into_body()).await?;
        let res: FingerResponse = serde_json::from_slice(&res)?;

        let mut found_uri = None;
        for entry in res.links {
            if entry.rel == "self"
                && (entry.type_.as_deref() == Some(ACTIVITY_TYPE)
                    || entry.type_.as_deref() == Some(ACTIVITY_TYPE_ALT))
            {
                if let Some(href) = entry.href {
                    found_uri = Some(href.parse()?);
                    break;
                }
            }
        }

        Ok(found_uri)
    }
}
