use super::{FollowLike, KnownObject, Verified};
use crate::types::{CommentLocalID, CommunityLocalID, PostLocalID, ThingLocalRef, UserLocalID};
use activitystreams::prelude::*;
use std::borrow::Cow;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

pub enum FoundFrom {
    Announce {
        url: url::Url,
        community_local_id: CommunityLocalID,
        community_is_local: bool,
    },
    Other,
}

impl FoundFrom {
    pub fn as_announce(&self) -> Option<&url::Url> {
        match self {
            FoundFrom::Announce { url, .. } => Some(url),
            _ => None,
        }
    }
}

pub enum IngestResult {
    Actor(super::ActorLocalInfo),
    Other(ThingLocalRef),
}

impl IngestResult {
    pub fn into_ref(self) -> ThingLocalRef {
        match self {
            IngestResult::Actor(info) => info.as_ref(),
            IngestResult::Other(x) => x,
        }
    }
}

pub async fn ingest_object(
    object: Verified<KnownObject>,
    found_from: FoundFrom,
    ctx: Arc<crate::BaseContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    let db = ctx.db_pool.get().await?;
    match object.into_inner() {
        KnownObject::Accept(activity) => {
            let activity_id = activity
                .id_unchecked()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let actor_ap_id = activity
                .actor_unchecked()
                .as_single_id()
                .ok_or(crate::Error::InternalStrStatic("Missing actor for Accept"))?;

            crate::apub_util::require_containment(activity_id, actor_ap_id)?;

            let actor_ap_id = actor_ap_id.as_str();

            let community_local_id: Option<CommunityLocalID> = {
                db.query_opt("SELECT id FROM community WHERE ap_id=$1", &[&actor_ap_id])
                    .await?
                    .map(|row| CommunityLocalID(row.get(0)))
            };

            if let Some(community_local_id) = community_local_id {
                let object_id = activity
                    .object()
                    .as_single_id()
                    .ok_or(crate::Error::InternalStrStatic("Missing object for Accept"))?;

                if let Some(remaining) =
                    crate::apub_util::try_strip_host(&object_id, &ctx.host_url_apub)
                {
                    if let Some(remaining) = remaining.strip_prefix("/communities/") {
                        let next_expected = format!("{}/followers/", community_local_id);
                        if remaining.starts_with(&next_expected) {
                            let remaining = &remaining[next_expected.len()..];
                            let follower_local_id: UserLocalID = remaining.parse()?;

                            db.execute(
                                "UPDATE community_follow SET accepted=TRUE WHERE community=$1 AND follower=$2",
                                &[&community_local_id, &follower_local_id],
                            ).await?;
                        }
                    }
                }
            }

            Ok(None)
        }
        KnownObject::Announce(activity) => {
            let activity_id = activity
                .id_unchecked()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let community_ap_id = activity.actor_unchecked().as_single_id().ok_or(
                crate::Error::InternalStrStatic("Missing actor for Announce"),
            )?;

            let community_local_info = db
                .query_opt(
                    "SELECT id, local FROM community WHERE ap_id=$1",
                    &[&community_ap_id.as_str()],
                )
                .await?
                .map(|row| (CommunityLocalID(row.get(0)), row.get(1)));

            if let Some((community_local_id, community_is_local)) = community_local_info {
                crate::apub_util::require_containment(activity_id, community_ap_id)?;

                let object_id = activity.object().as_single_id();

                if let Some(object_id) = object_id {
                    if let Some(remaining) =
                        crate::apub_util::try_strip_host(&object_id, &ctx.host_url_apub)
                    {
                        if let Some(remaining) = remaining.strip_prefix("/posts/") {
                            if let Ok(local_post_id) = remaining.parse::<PostLocalID>() {
                                db.execute(
                                    "UPDATE post SET approved=TRUE, approved_ap_id=$1 WHERE id=$2 AND community=$3",
                                    &[&activity_id.as_str(), &local_post_id, &community_local_id],
                                ).await?;
                            }
                        }
                    } else {
                        // don't need announces for local objects
                        let obj =
                            crate::apub_util::fetch_ap_object(object_id, &ctx.http_client).await?;

                        ingest_object_boxed(
                            obj,
                            FoundFrom::Announce {
                                url: activity_id.clone(),
                                community_local_id,
                                community_is_local,
                            },
                            ctx,
                        )
                        .await?;
                    }
                }
            }
            Ok(None)
        }
        KnownObject::Article(obj) => {
            ingest_postlike(Verified(KnownObject::Article(obj)), found_from, ctx).await
        }
        KnownObject::Create(activity) => {
            ingest_create(Verified(activity), ctx).await?;
            Ok(None)
        }
        KnownObject::Delete(activity) => {
            ingest_delete(Verified(activity), ctx).await?;
            Ok(None)
        }
        KnownObject::Follow(follow) => {
            ingest_followlike(Verified(FollowLike::Follow(follow)), ctx).await?;

            Ok(None)
        }
        KnownObject::Group(group) => {
            let ap_id = group
                .id_unchecked()
                .ok_or(crate::Error::InternalStrStatic("Missing ID in Group"))?;

            let name = group
                .preferred_username()
                .or_else(|| {
                    group
                        .name()
                        .and_then(|maybe| maybe.iter().filter_map(|x| x.as_xsd_string()).next())
                })
                .unwrap_or("");
            let description_html = group
                .summary()
                .and_then(|maybe| maybe.iter().filter_map(|x| x.as_xsd_string()).next());
            let inbox = group.inbox_unchecked().as_str();
            let outbox = group.outbox_unchecked().map(|x| x.as_str());
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
                "INSERT INTO community (name, local, ap_id, ap_inbox, ap_shared_inbox, public_key, public_key_sigalg, description_html, created_local, ap_outbox) VALUES ($1, FALSE, $2, $3, $4, $5, $6, $7, current_timestamp, $8) ON CONFLICT (ap_id) DO UPDATE SET ap_inbox=$3, ap_shared_inbox=$4, public_key=$5, public_key_sigalg=$6, description_html=$7, ap_outbox=$8 RETURNING id",
                &[&name, &ap_id.as_str(), &inbox, &shared_inbox, &public_key, &public_key_sigalg, &description_html, &outbox],
            ).await?.get(0));

            if let Some(featured_url) = group.ext_two.featured {
                crate::apub_util::spawn_enqueue_fetch_community_featured(id, featured_url, ctx);
            }

            Ok(Some(IngestResult::Actor(
                super::ActorLocalInfo::Community {
                    id,
                    public_key: public_key.map(|key| super::PubKeyInfo {
                        algorithm: super::get_message_digest(public_key_sigalg),
                        key: key.to_owned(),
                    }),
                },
            )))
        }
        KnownObject::Image(obj) => {
            ingest_postlike(Verified(KnownObject::Image(obj)), found_from, ctx).await
        }
        KnownObject::Join(follow) => {
            ingest_followlike(Verified(FollowLike::Join(follow)), ctx).await?;

            Ok(None)
        }
        KnownObject::Leave(activity) => {
            let activity_id = activity
                .id_unchecked()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let actor_id = activity.actor_unchecked().as_single_id().ok_or(
                crate::Error::InternalStrStatic("Missing actor for activity"),
            )?;

            let target_id = activity.object().as_single_id();

            super::require_containment(activity_id, actor_id)?;

            if let Some(target_id) = target_id {
                if let Some(community_id) =
                    super::maybe_get_local_community_id_from_uri(target_id, &ctx.host_url_apub)
                {
                    let follower_local_id = {
                        let row = db
                            .query_opt(
                                "SELECT id FROM person WHERE ap_id=$1",
                                &[&actor_id.as_str()],
                            )
                            .await?;
                        row.map(|row| UserLocalID(row.get(0)))
                    };
                    if let Some(follower_local_id) = follower_local_id {
                        db.execute(
                            "DELETE FROM community_follow WHERE community=$1 AND follower=$2",
                            &[&community_id, &follower_local_id],
                        )
                        .await?;
                    }
                }
            }

            Ok(None)
        }
        KnownObject::Like(activity) => {
            ingest_like(Verified(activity), ctx).await?;
            Ok(None)
        }
        KnownObject::Note(obj) => {
            ingest_postlike(Verified(KnownObject::Note(obj)), found_from, ctx).await
        }
        KnownObject::Page(obj) => {
            ingest_postlike(Verified(KnownObject::Page(obj)), found_from, ctx).await
        }
        KnownObject::Person(person) => ingest_personlike(Verified(person), false, ctx).await,
        KnownObject::Service(obj) => ingest_personlike(Verified(obj), true, ctx).await,
        KnownObject::Undo(activity) => {
            ingest_undo(Verified(activity), ctx).await?;
            Ok(None)
        }
        KnownObject::Update(activity) => {
            let activity_id = activity
                .id_unchecked()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let object_id =
                activity
                    .object()
                    .as_single_id()
                    .ok_or(crate::Error::InternalStrStatic(
                        "Missing object ID for Update",
                    ))?;

            crate::apub_util::require_containment(activity_id, object_id)?;

            let object_id = object_id.clone();

            crate::spawn_task(async move {
                let row = db
                    .query_opt(
                        "SELECT 1 FROM community WHERE ap_id=$1 LIMIT 1",
                        &[&object_id.as_str()],
                    )
                    .await?;
                if row.is_some() {
                    ctx.enqueue_task(&crate::tasks::FetchActor {
                        actor_ap_id: Cow::Owned(object_id),
                    })
                    .await?;
                }

                Ok(())
            });

            Ok(None)
        }
    }
}

pub fn ingest_object_boxed(
    object: Verified<KnownObject>,
    found_from: FoundFrom,
    ctx: Arc<crate::BaseContext>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<Option<IngestResult>, crate::Error>> + Send>> {
    Box::pin(ingest_object(object, found_from, ctx))
}

pub async fn ingest_like(
    activity: Verified<activitystreams::activity::Like>,
    ctx: Arc<crate::RouteContext>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    let activity_id = activity
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

    if let Some(actor_id) = activity.actor_unchecked().as_single_id() {
        super::require_containment(activity_id, actor_id)?;

        let actor_local_id = super::get_or_fetch_user_local_id(actor_id, &db, &ctx).await?;

        if let Some(object_id) = activity.object().as_single_id() {
            let thing_local_ref = if let Some(remaining) =
                super::try_strip_host(&object_id, &ctx.host_url_apub)
            {
                if let Some(remaining) = remaining.strip_prefix("/posts/") {
                    if let Ok(local_post_id) = remaining.parse() {
                        Some(ThingLocalRef::Post(local_post_id))
                    } else {
                        None
                    }
                } else if let Some(remaining) = remaining.strip_prefix("/comments/") {
                    if let Ok(local_comment_id) = remaining.parse() {
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

                row.map(|row| {
                    if row.get(0) {
                        ThingLocalRef::Post(PostLocalID(row.get(1)))
                    } else {
                        ThingLocalRef::Comment(CommentLocalID(row.get(1)))
                    }
                })
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
                                super::enqueue_forward_to_community_followers(
                                    community_id,
                                    body,
                                    ctx,
                                )
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
                                super::enqueue_forward_to_community_followers(
                                    community_id,
                                    body,
                                    ctx,
                                )
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

pub async fn ingest_delete(
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
        super::require_containment(activity_id, actor_id)?;
        super::require_containment(object_id, actor_id)?;

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

pub async fn ingest_undo(
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

    super::require_containment(activity_id, actor_id)?;
    super::require_containment(object_id, actor_id)?;

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

pub async fn ingest_create(
    activity: Verified<activitystreams::activity::Create>,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    for req_obj in activity.object().iter() {
        let object_id = req_obj.id();

        if let Some(object_id) = object_id {
            let obj = crate::apub_util::fetch_ap_object(object_id, &ctx.http_client).await?;

            ingest_object_boxed(obj, FoundFrom::Other, ctx.clone()).await?;
        }
    }

    Ok(())
}

/// Ingestion flow for Page, Image, Article, and Note. Should not be called with any other objects.
async fn ingest_postlike(
    obj: Verified<KnownObject>,
    found_from: FoundFrom,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    let (ext, to, in_reply_to, obj_id) = match obj.deref() {
        KnownObject::Page(obj) => (&obj.ext_one, obj.to(), None, obj.id_unchecked()),
        KnownObject::Image(obj) => (&obj.ext_one, obj.to(), None, obj.id_unchecked()),
        KnownObject::Article(obj) => (&obj.ext_one, obj.to(), None, obj.id_unchecked()),
        KnownObject::Note(obj) => (
            &obj.ext_one,
            obj.to(),
            obj.in_reply_to(),
            obj.id_unchecked(),
        ),
        _ => return Ok(None), // shouldn't happen?
    };
    let target = &ext.target;

    let community_found = match target
        .as_ref()
        .and_then(|target| target.as_one().and_then(|x| x.id()))
        .map(|target_id| {
            super::maybe_get_local_community_id_from_outbox_uri(target_id, &ctx.host_url_apub)
        }) {
        Some(Some(community_local_id)) => Some((community_local_id, true)),
        Some(None) => None,
        None => match found_from {
            FoundFrom::Announce {
                community_local_id,
                community_is_local,
                ..
            } => Some((community_local_id, community_is_local)),
            _ => match to {
                None => None,
                Some(maybe) => maybe
                    .iter()
                    .filter_map(|any| {
                        any.as_xsd_any_uri()
                            .and_then(|uri| {
                                super::maybe_get_local_community_id_from_uri(
                                    uri,
                                    &ctx.host_url_apub,
                                )
                            })
                            .map(|id| (id, true))
                    })
                    .next(),
            },
        },
    };

    if let Some((community_local_id, community_is_local)) = community_found {
        match obj.into_inner() {
            KnownObject::Page(obj) => Ok(handle_received_page_for_community(
                community_local_id,
                community_is_local,
                found_from.as_announce(),
                Verified(obj),
                ctx,
            )
            .await?
            .map(|id| IngestResult::Other(ThingLocalRef::Post(id)))),
            KnownObject::Image(obj) => Ok(handle_received_page_for_community(
                community_local_id,
                community_is_local,
                found_from.as_announce(),
                Verified(obj),
                ctx,
            )
            .await?
            .map(|id| IngestResult::Other(ThingLocalRef::Post(id)))),
            KnownObject::Article(obj) => Ok(handle_received_page_for_community(
                community_local_id,
                community_is_local,
                found_from.as_announce(),
                Verified(obj),
                ctx,
            )
            .await?
            .map(|id| IngestResult::Other(ThingLocalRef::Post(id)))),
            KnownObject::Note(obj) => {
                let content = obj.content();
                let content = content.as_ref().and_then(|x| x.as_single_xsd_string());
                let media_type = obj.media_type();
                let created = obj.published();
                let author = obj.attributed_to().and_then(|x| x.as_single_id());

                // fetch first attachment
                let attachment_href = obj
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
                let attachment_href = attachment_href
                    .as_ref()
                    .and_then(|href| href.iter().filter_map(|x| x.as_xsd_any_uri()).next())
                    .map(|href| href.as_str());

                if let Some(object_id) = obj.id_unchecked() {
                    if let Some(in_reply_to) = obj.in_reply_to() {
                        // it's a reply

                        Ok(handle_recieved_reply(
                            object_id,
                            content.unwrap_or(""),
                            media_type,
                            created.as_ref(),
                            author,
                            in_reply_to,
                            attachment_href,
                            ctx,
                        )
                        .await?
                        .map(|id| IngestResult::Other(ThingLocalRef::Comment(id))))
                    } else {
                        // not a reply, must be a top-level post
                        let summary = obj.summary();
                        let name = obj.name();

                        let title = name
                            .as_ref()
                            .and_then(|x| x.as_single_xsd_string())
                            .or_else(|| summary.as_ref().and_then(|x| x.as_single_xsd_string()))
                            .unwrap_or("");

                        // Interpret attachments (usually images) as links
                        let href = obj
                            .attachment()
                            .and_then(|x| x.iter().next())
                            .and_then(|base: &activitystreams::base::AnyBase| {
                                match base.kind_str() {
                                    Some("Document") => Some(
                                        activitystreams::object::Document::from_any_base(
                                            base.clone(),
                                        )
                                        .map(|obj| obj.unwrap().take_url()),
                                    ),
                                    Some("Image") => Some(
                                        activitystreams::object::Image::from_any_base(base.clone())
                                            .map(|obj| obj.unwrap().take_url()),
                                    ),
                                    _ => None,
                                }
                            })
                            .transpose()?
                            .flatten();
                        let href = href
                            .as_ref()
                            .and_then(|href| href.iter().filter_map(|x| x.as_xsd_any_uri()).next())
                            .map(|href| href.as_str());

                        Ok(Some(IngestResult::Other(ThingLocalRef::Post(
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
                                found_from.as_announce(),
                                ctx,
                            )
                            .await?,
                        ))))
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Err(crate::Error::InternalStrStatic(
                "ingest_postlike called with an unknown object",
            )),
        }
    } else {
        // not to a community, but might still match as a reply
        if let Some(in_reply_to) = in_reply_to {
            if let Some(obj_id) = obj_id {
                if let KnownObject::Note(obj) = obj.deref() {
                    // TODO deduplicate this?

                    let content = obj.content();
                    let content = content.as_ref().and_then(|x| x.as_single_xsd_string());
                    let media_type = obj.media_type();
                    let created = obj.published();
                    let author = obj.attributed_to().and_then(|x| x.as_single_id());

                    if let Some(author) = author {
                        super::require_containment(obj_id, author)?;
                    }

                    // fetch first attachment
                    let attachment_href = obj
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
                    let attachment_href = attachment_href
                        .as_ref()
                        .and_then(|href| href.iter().filter_map(|x| x.as_xsd_any_uri()).next())
                        .map(|href| href.as_str());

                    let id = handle_recieved_reply(
                        obj_id,
                        content.unwrap_or(""),
                        media_type,
                        created.as_ref(),
                        author,
                        in_reply_to,
                        attachment_href,
                        ctx,
                    )
                    .await?;

                    Ok(id.map(|id| IngestResult::Other(ThingLocalRef::Comment(id))))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

async fn ingest_followlike(
    follow: Verified<FollowLike>,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    let follower_ap_id = follow.actor_unchecked().as_single_id();
    let target = follow.object().as_single_id();

    if let Some(follower_ap_id) = follower_ap_id {
        let activity_ap_id = follow
            .id_unchecked()
            .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

        crate::apub_util::require_containment(activity_ap_id, follower_ap_id)?;
        let follow = crate::apub_util::Contained(Cow::Borrowed(&follow));

        let db = ctx.db_pool.get().await?;

        let follower_local_id =
            crate::apub_util::get_or_fetch_user_local_id(follower_ap_id, &db, &ctx).await?;

        if let Some(target) = target {
            if let Some(community_id) =
                super::maybe_get_local_community_id_from_uri(target, &ctx.host_url_apub)
            {
                let row = db
                    .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
                    .await?;
                if let Some(row) = row {
                    let local: bool = row.get(0);
                    if local {
                        db.execute("INSERT INTO community_follow (community, follower, local, ap_id, accepted) VALUES ($1, $2, FALSE, $3, TRUE) ON CONFLICT (community, follower) DO NOTHING", &[&community_id, &follower_local_id, &activity_ap_id.as_str()]).await?;

                        crate::apub_util::spawn_enqueue_send_community_follow_accept(
                            community_id,
                            follower_local_id,
                            follow.with_owned(),
                            ctx,
                        );
                    }
                } else {
                    log::error!("Warning: recieved follow for unknown community");
                }
            }
        }
    }

    Ok(())
}

async fn ingest_personlike<
    T,
    K: activitystreams::base::AsBase<T>
        + activitystreams::object::AsObject<T>
        + activitystreams::markers::Actor
        + Clone,
>(
    person: Verified<
        activitystreams_ext::Ext1<
            activitystreams::actor::ApActor<K>,
            super::PublicKeyExtension<'static>,
        >,
    >,
    is_bot: bool,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    let ap_id = person
        .id_unchecked()
        .ok_or(crate::Error::InternalStrStatic("Missing ID in Person"))?;

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
    let description_html = person
        .summary()
        .and_then(|maybe| maybe.iter().filter_map(|x| x.as_xsd_string()).next());

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

    let db = ctx.db_pool.get().await?;

    let id = UserLocalID(db.query_one(
        "INSERT INTO person (username, local, created_local, ap_id, ap_inbox, ap_shared_inbox, public_key, public_key_sigalg, description_html, avatar, is_bot) VALUES ($1, FALSE, localtimestamp, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (ap_id) DO UPDATE SET ap_inbox=$3, ap_shared_inbox=$4, public_key=$5, public_key_sigalg=$6, description_html=$7, avatar=$8, is_bot=$9 RETURNING id",
        &[&username, &ap_id.as_str(), &inbox, &shared_inbox, &public_key, &public_key_sigalg, &description_html, &avatar, &is_bot],
    ).await?.get(0));

    Ok(Some(IngestResult::Actor(super::ActorLocalInfo::User {
        id,
        public_key: public_key.map(|key| super::PubKeyInfo {
            algorithm: super::get_message_digest(public_key_sigalg),
            key: key.to_owned(),
        }),
    })))
}

async fn handle_recieved_reply(
    object_id: &url::Url,
    content: &str,
    media_type: Option<&mime::Mime>,
    created: Option<&chrono::DateTime<chrono::FixedOffset>>,
    author: Option<&url::Url>,
    in_reply_to: &activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase>,
    attachment_href: Option<&str>,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<CommentLocalID>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let author = match author {
        Some(author) => Some(super::get_or_fetch_user_local_id(author, &db, &ctx).await?),
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

            let target = if let Some(remaining) =
                super::try_strip_host(&term_ap_id, &ctx.host_url_apub)
            {
                if let Some(remaining) = remaining.strip_prefix("/posts/") {
                    if let Ok(local_post_id) = remaining.parse() {
                        Some(ReplyTarget::Post { id: local_post_id })
                    } else {
                        None
                    }
                } else if let Some(remaining) = remaining.strip_prefix("/comments/") {
                    if let Ok(local_comment_id) = remaining.parse() {
                        let row = db
                            .query_opt("SELECT post FROM reply WHERE id=$1", &[&local_comment_id])
                            .await?;
                        row.map(|row| ReplyTarget::Comment {
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
                    "INSERT INTO reply (post, parent, author, content_text, content_html, created, local, ap_id, attachment_href) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), FALSE, $7, $8) ON CONFLICT (ap_id) DO NOTHING RETURNING id",
                    &[&post, &parent, &author, &content_text, &content_html, &created, &object_id.as_str(), &attachment_href],
                    ).await?;

                if let Some(row) = row {
                    let id = CommentLocalID(row.get(0));
                    let info = crate::CommentInfo {
                        id,
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
                        attachment_href: attachment_href.map(|x| Cow::Owned(x.to_owned())),
                    };

                    crate::on_post_add_comment(info, ctx);

                    Ok(Some(id))
                } else {
                    // not new, try to fetch id
                    // will probably be unnecessary when we implement comment editing

                    let row = db
                        .query_opt(
                            "SELECT id FROM reply WHERE ap_id=$1",
                            &[&object_id.as_str()],
                        )
                        .await?;
                    Ok(row.map(|row| CommentLocalID(row.get(0))))
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

async fn handle_received_page_for_community<Kind: Clone + std::fmt::Debug>(
    community_local_id: CommunityLocalID,
    community_is_local: bool,
    is_announce: Option<&url::Url>,
    obj: Verified<super::ExtendedPostlike<activitystreams::object::Object<Kind>>>,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<PostLocalID>, crate::Error> {
    let title = obj
        .name()
        .iter()
        .chain(obj.summary().iter())
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
    let content = obj.content();
    let content = content.as_ref().and_then(|x| x.as_single_xsd_string());
    let media_type = obj.media_type();
    let created = obj.published();
    let author = obj.attributed_to().and_then(|x| x.as_single_id());

    if let Some(object_id) = obj.id_unchecked() {
        if let Some(author) = author {
            super::require_containment(object_id, author)?;
        }

        Ok(Some(
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
            .await?,
        ))
    } else {
        Ok(None)
    }
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
) -> Result<PostLocalID, crate::Error> {
    let db = ctx.db_pool.get().await?;
    let author = match author {
        Some(author) => Some(super::get_or_fetch_user_local_id(author, &db, &ctx).await?),
        None => None,
    };

    let content_is_html = media_type.is_none() || media_type == Some(&mime::TEXT_HTML);
    let (content_text, content_html) = if content_is_html {
        (None, Some(content))
    } else {
        (Some(content), None)
    };

    let approved = is_announce.is_some() || community_is_local;

    let row = db.query_one(
        "INSERT INTO post (author, href, content_text, content_html, title, created, community, local, ap_id, approved, approved_ap_id) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), $7, FALSE, $8, $9, $10) ON CONFLICT (ap_id) DO UPDATE SET approved=$9, approved_ap_id=$10 RETURNING id",
        &[&author, &href, &content_text, &content_html, &title, &created, &community_local_id, &object_id.as_str(), &approved, &is_announce.map(|x| x.as_str())],
    ).await?;

    let post_local_id = PostLocalID(row.get(0));

    if community_is_local {
        crate::on_local_community_add_post(community_local_id, post_local_id, object_id, ctx);
    }

    Ok(post_local_id)
}
