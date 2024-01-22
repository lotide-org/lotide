use crate::lang;
use crate::types::{
    ActorLocalRef, CommentLocalID, CommunityLocalID, NotificationID, NotificationSubscriptionID,
    PostLocalID, UserLocalID,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::Write;
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
            .header(hyper::header::USER_AGENT, &ctx.user_agent)
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
                if ctx.break_stuff {
                    let signature = hancock::httpbis::HttpbisSignature::create_for_request(
                        "signature",
                        hancock::httpbis::SignatureParams {
                            keyid: Some(key_id.as_str().into()),
                            alg: Some("hmac-sha256".into()),
                            ..hancock::httpbis::SignatureParams::new_now(5 * 60) // 5 minutes
                        },
                        hancock::httpbis::cover_all_components_for_request(&req),
                        &req,
                        |src| crate::apub_util::do_sign(&privkey, &src),
                    )?;

                    signature.apply_headers(&mut req.headers_mut())?;
                } else {
                    let signature = hancock::richanna::RichannaSignature::create_legacy(
                        key_id.as_str(),
                        &hyper::Method::POST,
                        &path_and_query,
                        req.headers(),
                        |src| crate::apub_util::do_sign(&privkey, &src),
                    )?;

                    req.headers_mut().insert("Signature", signature.to_header());
                }
            }
        }

        let res = crate::res_to_error(ctx.http_client.request(req).await?).await?;

        log::debug!("{:?}", res);

        Ok(())
    }
}

mod deprecated {
    // workaround for https://github.com/serde-rs/serde/issues/2195
    #![allow(deprecated)]

    use super::*;

    #[derive(Deserialize, Serialize, Debug)]
    #[deprecated]
    pub struct DeliverToFollowers {
        pub actor: ActorLocalRef,
        pub sign: bool,
        pub object: String,
    }

    #[async_trait]
    #[allow(deprecated)]
    impl TaskDef for DeliverToFollowers {
        const KIND: &'static str = "deliver_to_followers";

        async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
            DeliverToAudience {
                sign_as: if self.sign { Some(self.actor) } else { None },
                object: self.object,
                audience: (&[AudienceItem::Followers(self.actor)][..]).into(),
            }
            .perform(ctx)
            .await
        }
    }
}

pub use deprecated::*;

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
pub enum AudienceItem {
    Followers(ActorLocalRef),
    Single(ActorLocalRef),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DeliverToAudience<'a> {
    pub sign_as: Option<ActorLocalRef>,
    pub object: String,
    pub audience: Cow<'a, [AudienceItem]>,
}

#[async_trait]
impl<'a> TaskDef for DeliverToAudience<'a> {
    const KIND: &'static str = "deliver_to_audience";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;

        let sign_as_value = postgres_types::Json(&self.sign_as);

        let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![
            &DeliverToInbox::KIND,
            &sign_as_value,
            &self.object,
            &DeliverToInbox::MAX_ATTEMPTS,
        ];
        let mut sql = "INSERT INTO task (kind, params, max_attempts, created_at) SELECT $1, json_build_object('sign_as', $2::JSON, 'object', $3::TEXT, 'inbox', inbox), $4, current_timestamp FROM (SELECT DISTINCT COALESCE(ap_shared_inbox, ap_inbox) AS inbox FROM person WHERE local=FALSE AND (FALSE".to_owned();

        for item in self.audience.iter() {
            match item {
                AudienceItem::Followers(actor) => {
                    match actor {
                        ActorLocalRef::Person(_) => {
                            // do nothing, user followers don't currently exist
                        }
                        ActorLocalRef::Community(community_id) => {
                            values.push(community_id);
                            write!(sql, " OR id IN (SELECT follower FROM community_follow WHERE community=${})", values.len()).unwrap();
                        }
                    }
                }
                AudienceItem::Single(actor) => match actor {
                    ActorLocalRef::Person(user_id) => {
                        values.push(user_id);
                        write!(sql, " OR id=${}", values.len()).unwrap();
                    }
                    ActorLocalRef::Community(_) => {
                        return Err(crate::Error::InternalStrStatic(
                            "Including communities as single audience is not implemented",
                        ));
                    }
                },
            }
        }

        sql += ")) AS result";

        db.execute(&sql, &values).await?;

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

        let obj = crate::apub_util::fetch_ap_object_raw(&self.featured_url, &ctx).await?;
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
                if let Some(crate::apub_util::LocalObjectRef::Post(id)) =
                    ap_id.parse().ok().and_then(|uri| {
                        crate::apub_util::LocalObjectRef::try_from_uri(&uri, &ctx.host_url_apub)
                    })
                {
                    Some(id)
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

#[derive(Deserialize, Serialize, Debug)]
pub struct SendNotification {
    pub notification: NotificationID,
}

#[async_trait]
impl TaskDef for SendNotification {
    const KIND: &'static str = "send_notification";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        enum NotificationSendInfo<'a> {
            PostReply {
                href: crate::BaseURL,
                reply_content: &'a str,
                post_title: &'a str,
            },
            ReplyReply {
                href: crate::BaseURL,
                reply_content: &'a str,
                post_title: &'a str,
            },
        }

        let db = ctx.db_pool.get().await?;

        let row = db.query_one("SELECT notification.kind, notification.to_user, reply.id, reply.content_text, reply.content_markdown, reply.content_html, parent_post.title FROM notification LEFT OUTER JOIN reply ON (reply.id = notification.reply) LEFT OUTER JOIN post AS parent_post ON (parent_post.id = notification.parent_post) WHERE notification.id=$1", &[&self.notification]).await?;

        let user = UserLocalID(row.get(1));

        let subscriptions_rows = db
            .query(
                "SELECT id, language FROM person_notification_subscription WHERE person=$1",
                &[&user],
            )
            .await?;

        let build_content = |info| -> Vec<_> {
            subscriptions_rows
                .into_iter()
                .map(|row| {
                    let id = NotificationSubscriptionID(row.get(0));
                    let language_req: Option<&str> = row.get(1);

                    let lang = crate::get_lang_for_header(language_req);

                    match &info {
                        NotificationSendInfo::PostReply {
                            href,
                            reply_content,
                            post_title,
                        } => SendNotificationForSubscription {
                            subscription: id,
                            href: Cow::Owned(href.to_string()),
                            title: Cow::Owned(
                                lang.tr(&lang::notification_title_post_reply(*post_title))
                                    .into_owned(),
                            ),
                            body: Cow::Borrowed(reply_content),
                        },
                        NotificationSendInfo::ReplyReply {
                            href,
                            reply_content,
                            post_title,
                        } => SendNotificationForSubscription {
                            subscription: id,
                            href: Cow::Owned(href.to_string()),
                            title: Cow::Owned(
                                lang.tr(&lang::notification_title_reply_reply(*post_title))
                                    .into_owned(),
                            ),
                            body: Cow::Borrowed(reply_content),
                        },
                    }
                })
                .collect()
        };

        let new_tasks = match row.get(0) {
            "post_reply" => {
                let content = row
                    .get::<_, Option<&str>>(3)
                    .or_else(|| row.get(4))
                    .or_else(|| row.get(5));

                if let Some(content) = content {
                    let id = CommentLocalID(row.get(2));

                    let post_title: Option<&str> = row.get(6);
                    if let Some(post_title) = post_title {
                        Some(build_content(NotificationSendInfo::PostReply {
                            href: crate::apub_util::LocalObjectRef::Comment(id)
                                .to_local_uri(&ctx.host_url_apub),
                            reply_content: content,
                            post_title: post_title,
                        }))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            "reply_reply" => {
                let content = row
                    .get::<_, Option<&str>>(3)
                    .or_else(|| row.get(4))
                    .or_else(|| row.get(5));

                if let Some(content) = content {
                    let id = CommentLocalID(row.get(2));

                    let post_title: Option<&str> = row.get(6);
                    if let Some(post_title) = post_title {
                        Some(build_content(NotificationSendInfo::ReplyReply {
                            href: crate::apub_util::LocalObjectRef::Comment(id)
                                .to_local_uri(&ctx.host_url_apub),
                            reply_content: content,
                            post_title: post_title,
                        }))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        };

        if let Some(new_tasks) = new_tasks {
            ctx.enqueue_tasks(&new_tasks).await?;
        }

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SendNotificationForSubscription<'a> {
    pub subscription: NotificationSubscriptionID,
    pub title: Cow<'a, str>,
    pub body: Cow<'a, str>,
    pub href: Cow<'a, str>,
}

#[async_trait]
impl<'a> TaskDef for SendNotificationForSubscription<'a> {
    const KIND: &'static str = "send_notification_for_subscription";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        let db = ctx.db_pool.get().await?;

        let row = db.query_one("SELECT endpoint, p256dh_key, auth_key FROM person_notification_subscription WHERE id=$1", &[&self.subscription]).await?;
        let endpoint: &str = row.get(0);
        let p256dh_key: &str = row.get(1);
        let auth_key: &str = row.get(2);

        let payload = serde_json::json!({
            "title": (self.title),
            "body": (self.body),
            "href": (self.href),
        });
        let payload = serde_json::to_vec(&payload)?;

        let subscription = web_push::SubscriptionInfo::new(endpoint, p256dh_key, auth_key);

        let mut builder = web_push::WebPushMessageBuilder::new(&subscription)?;
        builder.set_vapid_signature(
            ctx.vapid_signature_builder
                .clone()
                .add_sub_info(&subscription)
                .build()?,
        );
        builder.set_payload(web_push::ContentEncoding::Aes128Gcm, &payload);

        let message = builder.build()?;

        let req = web_push::request_builder::build_request(message);
        let res = ctx.http_client.request(req).await?;
        let code = res.status();
        let body = hyper::body::to_bytes(res.into_body()).await?;

        web_push::request_builder::parse_response(code, body.to_vec())?;

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct IngestObjectFromInbox<'a> {
    pub object: Cow<'a, str>,
}

#[async_trait]
impl<'a> TaskDef for IngestObjectFromInbox<'a> {
    const KIND: &'static str = "ingest_object_from_inbox";

    async fn perform(self, ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
        // should already have been verified when creating the task
        let object = crate::apub_util::Verified(serde_json::from_str(&self.object)?);

        crate::apub_util::ingest::ingest_object(
            object,
            crate::apub_util::ingest::FoundFrom::Other,
            ctx,
            true,
        )
        .await?;

        Ok(())
    }
}
