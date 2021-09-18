use crate::types::{ActorLocalRef, CommunityLocalID, PostLocalID};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;

#[async_trait]
pub trait TaskDef: Serialize + std::fmt::Debug + Sync {
    const KIND: &'static str;
    const MAX_ATTEMPTS: i16 = 8;
    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error>;
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DeliverToInbox<'a> {
    pub inbox: Cow<'a, url::Url>,
    pub sign_as: Option<ActorLocalRef>,
    pub object: String,
}

#[async_trait]
impl<'a> TaskDef for DeliverToInbox<'a> {
    const KIND: &'static str = "deliver_to_inbox";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;

        let signing_info: Option<(_, _)> = match self.sign_as {
            None => None,
            Some(actor_ref) => Some(
                crate::apub_util::fetch_or_create_local_actor_privkey(
                    actor_ref,
                    &db,
                    &ctx.host_url_apub,
                )
                .await?,
            ),
        };

        let digest =
            openssl::hash::hash(openssl::hash::MessageDigest::sha256(), self.object.as_ref())?;
        let mut digest_header = "SHA-256=".to_owned();
        base64::encode_config_buf(digest, base64::STANDARD, &mut digest_header);

        let inbox_uri = self.inbox.as_str().parse::<hyper::Uri>()?;

        let mut req = hyper::Request::post(&inbox_uri)
            .header(hyper::header::CONTENT_TYPE, crate::apub_util::ACTIVITY_TYPE)
            .header("Digest", digest_header)
            .body(self.object.into())?;

        req.headers_mut()
            .entry(hyper::header::HOST)
            .or_insert_with(|| {
                let uri = inbox_uri;

                let hostname = uri.host().expect("authority implies host");
                if let Some(port) = uri.port() {
                    let s = format!("{}:{}", hostname, port);
                    hyper::header::HeaderValue::from_str(&s)
                } else {
                    hyper::header::HeaderValue::from_str(hostname)
                }
                .expect("uri host is valid header value")
            });

        if let Ok(path_and_query) = crate::get_path_and_query(&self.inbox) {
            req.headers_mut()
                .insert(hyper::header::DATE, crate::apub_util::now_http_date());

            if let Some((privkey, key_id)) = signing_info {
                let signature = hancock::Signature::create_legacy(
                    key_id.as_str(),
                    &hyper::Method::POST,
                    &path_and_query,
                    req.headers(),
                    |src| crate::apub_util::do_sign(&privkey, &src),
                )?;

                req.headers_mut().insert("Signature", signature.to_header());
            }
        }

        let res = crate::res_to_error(ctx.http_client.request(req).await?).await?;

        log::debug!("{:?}", res);

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DeliverToFollowers {
    pub actor: ActorLocalRef,
    pub sign: bool,
    pub object: String,
}

#[async_trait]
impl TaskDef for DeliverToFollowers {
    const KIND: &'static str = "deliver_to_followers";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let community_id = match self.actor {
            ActorLocalRef::Community(id) => id,
            ActorLocalRef::Person(_) => return Ok(()), // We don't have user followers at this point
        };

        let db = ctx.db_pool.get().await?;

        db.execute(
            "INSERT INTO task (kind, params, max_attempts, created_at) SELECT $1, json_build_object('sign_as', $2::JSON, 'object', $3::TEXT, 'inbox', inbox), $4, current_timestamp FROM (SELECT DISTINCT COALESCE(ap_shared_inbox, ap_inbox) AS inbox FROM community_follow, person WHERE person.id = community_follow.follower AND person.local = FALSE AND community = $5) AS result",
            &[&DeliverToInbox::KIND, &postgres_types::Json(&if self.sign { Some(self.actor) } else { None }), &self.object, &DeliverToInbox::MAX_ATTEMPTS, &community_id],
        ).await?;

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FetchActor<'a> {
    pub actor_ap_id: Cow<'a, url::Url>,
}

#[async_trait]
impl<'a> TaskDef for FetchActor<'a> {
    const KIND: &'static str = "fetch_actor";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        crate::apub_util::fetch_actor(&self.actor_ap_id, ctx).await?;

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FetchCommunityFeatured {
    pub community_id: CommunityLocalID,
    pub featured_url: url::Url,
}

#[async_trait]
impl TaskDef for FetchCommunityFeatured {
    const KIND: &'static str = "fetch_community_featured";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        use activitystreams::prelude::*;

        let obj =
            crate::apub_util::fetch_ap_object_raw(&self.featured_url, &ctx.http_client).await?;
        let obj: crate::apub_util::AnyCollection = serde_json::from_value(obj)?;

        let items = match &obj {
            crate::apub_util::AnyCollection::Unordered(obj) => obj.items(),
            crate::apub_util::AnyCollection::Ordered(obj) => obj.ordered_items(),
        };

        let (local_items, remote_items) = match items {
            None => (Vec::new(), Vec::new()),
            Some(items) => items
                .iter()
                .map(|item| item.as_xsd_any_uri())
                .flatten()
                .map(|x| x.as_str())
                .partition(|x| x.starts_with(ctx.host_url_apub.as_str())),
        };

        let local_items: Vec<PostLocalID> = local_items
            .into_iter()
            .filter_map(|ap_id| {
                let rest = crate::apub_util::try_strip_host(&ap_id, &ctx.host_url_apub).unwrap();
                if let Some(rest) = rest.strip_prefix("/posts/") {
                    rest.parse().ok()
                } else {
                    None
                }
            })
            .collect();

        let db = ctx.db_pool.get().await?;

        db.execute(
            "UPDATE post SET sticky=COALESCE((ap_id = ANY($1)) OR (id = ANY($2)), FALSE) WHERE community=$3",
            &[&remote_items, &local_items, &self.community_id],
        ).await?;

        Ok(())
    }
}
