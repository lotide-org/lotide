use super::{ExtendedPostlike, FollowLike, KnownObject, Verified};
use crate::types::{
    CommentLocalID, CommunityLocalID, PollOptionLocalID, PostLocalID, ThingLocalRef, UserLocalID,
};
use activitystreams::prelude::*;
use serde::Deserialize;
use std::borrow::Cow;
use std::convert::TryInto;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum FoundFrom {
    Announce {
        url: url::Url,
        community_local_id: CommunityLocalID,
        community_is_local: bool,
    },
    Refresh,
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
    Post(PostIngestResult),
    Other(ThingLocalRef),
}

impl IngestResult {
    pub fn into_ref(self) -> ThingLocalRef {
        match self {
            IngestResult::Actor(info) => info.as_ref(),
            IngestResult::Post(info) => ThingLocalRef::Post(info.id),
            IngestResult::Other(x) => x,
        }
    }
}

pub struct PostIngestResult {
    pub id: PostLocalID,
    pub poll: Option<crate::PollInfoOwned>,
}

pub async fn ingest_object(
    object: Verified<KnownObject>,
    found_from: FoundFrom,
    ctx: Arc<crate::BaseContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    let mut db = ctx.db_pool.get().await?;
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
                    let obj_ref = super::LocalObjectRef::try_from_path(remaining);
                    match obj_ref {
                        Some(super::LocalObjectRef::CommunityFollow(_, follower_local_id))
                        | Some(super::LocalObjectRef::CommunityFollowJoin(_, follower_local_id)) => {
                            db.execute(
                                "UPDATE community_follow SET accepted=TRUE WHERE community=$1 AND follower=$2",
                                &[&community_local_id, &follower_local_id],
                            ).await?;
                        }
                        _ => {}
                    }
                }
            }

            Ok(None)
        }
        KnownObject::Add(activity) => {
            let (actor, object, _origin, target, activity) = activity.into_parts();

            let activity_id = activity
                .id_unchecked()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let target = target
                .as_ref()
                .and_then(|x| x.as_single_id())
                .ok_or(crate::Error::InternalStrStatic("Missing target for Add"))?;

            let community_ap_id = actor
                .as_single_id()
                .ok_or(crate::Error::InternalStrStatic("Missing actor for Add"))?;

            let res = db
                .query_opt(
                    "SELECT id, local, ap_outbox FROM community WHERE ap_id=$1",
                    &[&community_ap_id.as_str()],
                )
                .await?;
            let community_local_info: Option<(CommunityLocalID, bool, Option<&str>)> = res
                .as_ref()
                .map(|row| (CommunityLocalID(row.get(0)), row.get(1), row.get(2)));

            if let Some((community_local_id, community_is_local, ap_outbox)) = community_local_info
            {
                let target_is_outbox = if let Some(ap_outbox) = ap_outbox {
                    ap_outbox == target.as_str()
                } else {
                    let actor = crate::apub_util::fetch_actor(community_ap_id, ctx.clone()).await?;

                    if let crate::apub_util::ActorLocalInfo::Community { ap_outbox, .. } = actor {
                        if let Some(ap_outbox) = ap_outbox {
                            ap_outbox == *target
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if target_is_outbox {
                    crate::apub_util::require_containment(activity_id, community_ap_id)?;
                    crate::apub_util::require_containment(target, community_ap_id)?;

                    let object_id = object.as_single_id();

                    if let Some(object_id) = object_id {
                        if let Some(remaining) =
                            crate::apub_util::try_strip_host(&object_id, &ctx.host_url_apub)
                        {
                            if let Some(crate::apub_util::LocalObjectRef::Post(local_post_id)) =
                                crate::apub_util::LocalObjectRef::try_from_path(remaining)
                            {
                                db.execute(
                                    "UPDATE post SET approved=TRUE, approved_ap_id=$1, rejected=FALSE, rejected_ap_id=NULL WHERE id=$2 AND community=$3",
                                    &[&activity_id.as_str(), &local_post_id, &community_local_id],
                                ).await?;
                            }
                        } else {
                            let obj = crate::apub_util::fetch_or_verify(
                                community_ap_id,
                                object.one().unwrap(),
                                &ctx,
                            )
                            .await?;

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
            }

            Ok(None)
        }
        KnownObject::Announce(activity) => {
            let (actor, object, _target, activity) = activity.into_parts();

            let activity_id = activity
                .id_unchecked()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let community_ap_id = actor.as_single_id().ok_or(crate::Error::InternalStrStatic(
                "Missing actor for Announce",
            ))?;

            let community_local_info = db
                .query_opt(
                    "SELECT id, local FROM community WHERE ap_id=$1",
                    &[&community_ap_id.as_str()],
                )
                .await?
                .map(|row| (CommunityLocalID(row.get(0)), row.get(1)));

            if let Some((community_local_id, community_is_local)) = community_local_info {
                crate::apub_util::require_containment(activity_id, community_ap_id)?;

                let object_id = object.as_single_id();

                if let Some(object_id) = object_id {
                    if let Some(remaining) =
                        crate::apub_util::try_strip_host(&object_id, &ctx.host_url_apub)
                    {
                        if let Some(crate::apub_util::LocalObjectRef::Post(local_post_id)) =
                            crate::apub_util::LocalObjectRef::try_from_path(remaining)
                        {
                            db.execute(
                                "UPDATE post SET approved=TRUE, approved_ap_id=$1, rejected=FALSE, rejected_ap_id=NULL WHERE id=$2 AND community=$3",
                                &[&activity_id.as_str(), &local_post_id, &community_local_id],
                            ).await?;
                        }
                    } else {
                        let obj = crate::apub_util::fetch_or_verify(
                            community_ap_id,
                            object.one().unwrap(),
                            &ctx,
                        )
                        .await?;

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
            ingest_create(Verified(activity), found_from, ctx).await?;
            Ok(None)
        }
        KnownObject::Delete(activity) => {
            ingest_delete(Verified(activity), ctx).await?;
            Ok(None)
        }
        KnownObject::Flag(activity) => {
            let activity_id = activity
                .id_unchecked()
                .ok_or(crate::Error::InternalStrStatic("Missing ID in activity"))?;

            let actor_ap_id = activity.actor_unchecked().as_single_id().ok_or(
                crate::Error::InternalStrStatic("Missing actor for activity"),
            )?;

            crate::apub_util::require_containment(activity_id, actor_ap_id)?;

            let actor_local_id =
                crate::apub_util::get_or_fetch_user_local_id(actor_ap_id, &db, &ctx).await?;

            let target =
                activity
                    .object()
                    .as_single_id()
                    .ok_or(crate::Error::InternalStrStatic(
                        "Missing target in activity",
                    ))?;

            let target_found = if let Some(remaining) =
                super::try_strip_host(target, &ctx.host_url_apub)
            {
                super::LocalObjectRef::try_from_path(remaining).map(|x| (x, None))
            } else {
                let row = db.query_opt(
                    "SELECT post.id, community.id, community.local, community.ap_id FROM post LEFT OUTER JOIN community ON (community.id = post.community) WHERE post.ap_id = $1",
                    &[&target.as_str()],
                ).await?;

                row.map(|row| {
                    let post_id = PostLocalID(row.get(0));

                    let community_ap_id = if let Some(community_local) = row.get(2) {
                        if community_local {
                            let community_id = CommunityLocalID(row.get(1));

                            Some(Some(
                                super::LocalObjectRef::Community(community_id)
                                    .to_local_uri(&ctx.host_url_apub),
                            ))
                        } else {
                            Some(row.get::<_, Option<&str>>(3).and_then(|x| x.parse().ok()))
                        }
                    } else {
                        Some(None)
                    };

                    (super::LocalObjectRef::Post(post_id), community_ap_id)
                })
            };

            let content = activity
                .content()
                .as_ref()
                .and_then(|x| x.as_one())
                .and_then(|x| x.as_xsd_string());

            if let Some((target_local_id, community_ap_id)) = target_found {
                match target_local_id {
                    super::LocalObjectRef::Post(post_id) => {
                        let community_ap_id = match community_ap_id {
                            Some(community_ap_id) => community_ap_id,
                            None => {
                                let row = db.query_opt(
                                    "SELECT id, local, ap_id FROM community WHERE id = (SELECT community FROM post WHERE id=$1)",
                                    &[&post_id],
                                ).await?;

                                row.and_then(|row| {
                                    if let Some(community_local) = row.get(1) {
                                        if community_local {
                                            let community_id = CommunityLocalID(row.get(0));

                                            Some(
                                                super::LocalObjectRef::Community(community_id)
                                                    .to_local_uri(&ctx.host_url_apub),
                                            )
                                        } else {
                                            row.get::<_, Option<&str>>(2)
                                                .and_then(|x| x.parse().ok())
                                        }
                                    } else {
                                        None
                                    }
                                })
                            }
                        };

                        let to_community = match community_ap_id {
                            None => false,
                            Some(community_ap_id) => {
                                if let Some(to) = activity.to() {
                                    to.iter()
                                        .any(|x| x.as_xsd_any_uri() == Some(&community_ap_id))
                                } else {
                                    false
                                }
                            }
                        };

                        db.execute(
                            "INSERT INTO flag (kind, person, post, content_text, to_community, to_remote_site_admin, created_local, local, ap_id) VALUES ('post', $1, $2, $3, $4, TRUE, current_timestamp, FALSE, $5) ON CONFLICT (ap_id) DO UPDATE SET kind='post', person=$1, post=$2, content_text=$3, to_community=$4",
                            &[&actor_local_id, &post_id, &content, &to_community, &activity_id.as_str()],
                        ).await?;
                    }
                    _ => {
                        log::warn!("unsupported flag target: {:?}", target_local_id);
                    }
                }
            }

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
            let outbox = group.outbox_unchecked();
            let followers = group.followers_unchecked().map(|x| x.as_str());
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
                "INSERT INTO community (name, local, ap_id, ap_inbox, ap_shared_inbox, public_key, public_key_sigalg, description_html, created_local, ap_outbox, ap_followers) VALUES ($1, FALSE, $2, $3, $4, $5, $6, $7, current_timestamp, $8, $9) ON CONFLICT (ap_id) DO UPDATE SET ap_inbox=$3, ap_shared_inbox=$4, public_key=$5, public_key_sigalg=$6, description_html=$7, ap_outbox=$8, ap_followers=$9 RETURNING id",
                &[&name, &ap_id.as_str(), &inbox, &shared_inbox, &public_key, &public_key_sigalg, &description_html, &outbox.map(|x| x.as_str()), &followers],
            ).await?.get(0));

            let outbox = outbox.map(|x| x.to_owned());

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
                    ap_outbox: outbox,
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
                if let Some(super::LocalObjectRef::Community(community_id)) =
                    super::LocalObjectRef::try_from_uri(target_id, &ctx.host_url_apub)
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
            // try to handle poll response
            if let Some(in_reply_to) = obj.in_reply_to().and_then(|x| x.as_single_id()) {
                if let Some(crate::apub_util::LocalObjectRef::Post(post_id)) =
                    crate::apub_util::LocalObjectRef::try_from_uri(in_reply_to, &ctx.host_url_apub)
                {
                    if let Some(name) = obj
                        .name()
                        .as_ref()
                        .and_then(|x| x.as_one())
                        .and_then(|x| x.as_xsd_string())
                    {
                        if let Some(actor_id) = obj.attributed_to().and_then(|x| x.as_single_id()) {
                            super::require_containment(
                                obj.id_unchecked().ok_or(crate::Error::InternalStrStatic(
                                    "Missing activity ID",
                                ))?,
                                actor_id,
                            )?;

                            let row = db.query_opt("SELECT poll_option.id, poll.id, poll.multiple, COALESCE(poll.is_closed, poll.closed_at <= current_timestamp, FALSE) FROM poll_option INNER JOIN poll ON (poll.id = poll_option.poll_id) WHERE poll_id=(SELECT poll_id FROM post WHERE id=$1 AND local) AND name=$2", &[&post_id, &name]).await?;
                            if let Some(row) = row {
                                let option_id: i64 = row.get(0);
                                let poll_id: i64 = row.get(1);
                                let multiple: bool = row.get(2);
                                let closed: bool = row.get(3);

                                if closed {
                                    // ignore
                                } else {
                                    let actor_local_id =
                                        super::get_or_fetch_user_local_id(&actor_id, &db, &ctx)
                                            .await?;

                                    {
                                        let trans = db.transaction().await?;

                                        if !multiple {
                                            trans
                                                .execute(
                                                    "DELETE FROM poll_vote WHERE person=$1",
                                                    &[&actor_local_id],
                                                )
                                                .await?;
                                        }

                                        trans.execute("INSERT INTO poll_vote (poll_id, option_id, person) VALUES ($1, $2, $3)", &[&poll_id, &option_id, &actor_local_id]).await?;

                                        trans.commit().await?;
                                    }
                                }

                                return Ok(None);
                            }
                        }
                    }
                }
            }

            ingest_postlike(Verified(KnownObject::Note(obj)), found_from, ctx).await
        }
        KnownObject::Page(obj) => {
            ingest_postlike(Verified(KnownObject::Page(obj)), found_from, ctx).await
        }
        KnownObject::Person(person) => ingest_personlike(Verified(person), false, ctx).await,
        KnownObject::Question(obj) => {
            ingest_postlike(Verified(KnownObject::Question(obj)), found_from, ctx).await
        }
        KnownObject::Remove(activity) => {
            let activity_id = activity
                .id_unchecked()
                .ok_or(crate::Error::InternalStrStatic("Missing activity ID"))?;

            let target = activity
                .target()
                .and_then(|x| x.as_single_id())
                .ok_or(crate::Error::InternalStrStatic("Missing target for Remove"))?;

            let community_ap_id = activity
                .actor_unchecked()
                .as_single_id()
                .ok_or(crate::Error::InternalStrStatic("Missing actor for Remove"))?;

            let res = db
                .query_opt(
                    "SELECT id, ap_outbox FROM community WHERE ap_id=$1",
                    &[&community_ap_id.as_str()],
                )
                .await?;
            let community_local_info: Option<(CommunityLocalID, Option<&str>)> = res
                .as_ref()
                .map(|row| (CommunityLocalID(row.get(0)), row.get(1)));

            if let Some((community_local_id, ap_outbox)) = community_local_info {
                let target_is_outbox = if let Some(ap_outbox) = ap_outbox {
                    ap_outbox == target.as_str()
                } else {
                    let actor = crate::apub_util::fetch_actor(community_ap_id, ctx.clone()).await?;

                    if let crate::apub_util::ActorLocalInfo::Community { ap_outbox, .. } = actor {
                        if let Some(ap_outbox) = ap_outbox {
                            ap_outbox == *target
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if target_is_outbox {
                    crate::apub_util::require_containment(activity_id, community_ap_id)?;
                    crate::apub_util::require_containment(target, community_ap_id)?;

                    let object_id = activity.object().as_single_id();

                    if let Some(object_id) = object_id {
                        if let Some(local_id) =
                            super::LocalObjectRef::try_from_uri(object_id, &ctx.host_url_apub)
                        {
                            if let super::LocalObjectRef::Post(local_post_id) = local_id {
                                db.execute(
                                    "UPDATE post SET approved=FALSE, approved_ap_id=NULL, rejected=TRUE, rejected_ap_id=$3 WHERE id=$1 AND community=$2",
                                    &[&local_post_id, &community_local_id, &activity_id.as_str()],
                                ).await?;
                            }
                        } else {
                            db.execute("UPDATE post SET approved=FALSE, approved_ap_id=NULL, rejected=TRUE, rejected_ap_id=$2 WHERE ap_id=$1", &[&object_id.as_str(), &activity_id.as_str()])
                                .await?;
                        }
                    }
                }
            }

            Ok(None)
        }
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
            let thing_local_ref = if let Some(local_id) =
                super::LocalObjectRef::try_from_uri(&object_id, &ctx.host_url_apub)
            {
                match local_id {
                    super::LocalObjectRef::Post(id) => Some(ThingLocalRef::Post(id)),
                    super::LocalObjectRef::Comment(id) => Some(ThingLocalRef::Comment(id)),
                    _ => None,
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

        // maybe it's a post or reply
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
        } else {
            // maybe it's a community
            db.execute("UPDATE community SET deleted=TRUE, old_name=name, name='[deleted]', description=NULL, description_html=NULL, description_markdown=NULL, created_by=NULL, public_key=NULL WHERE ap_id=$1", &[&object_id.as_str()]).await?;
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
        "UPDATE post SET approved=FALSE, approved_ap_id=NULL, rejected=TRUE, rejected_ap_id=$2 WHERE approved_ap_id=$1",
        &[&object_id, &activity_id.as_str()],
    )
    .await?;

    Ok(())
}

pub async fn ingest_create(
    activity: Verified<activitystreams::activity::Create>,
    found_from: FoundFrom,
    ctx: Arc<crate::BaseContext>,
) -> Result<(), crate::Error> {
    for req_obj in activity.object().iter() {
        let object_id = req_obj.id();

        if let Some(object_id) = object_id {
            let obj = if if let Some(activity_id) = activity.id_unchecked() {
                crate::apub_util::is_contained(activity_id, object_id)
            } else {
                false
            } {
                Verified(serde_json::from_value(serde_json::to_value(&req_obj)?)?)
            } else {
                crate::apub_util::fetch_ap_object(object_id, &ctx).await?
            };

            ingest_object_boxed(obj, found_from.clone(), ctx.clone()).await?;
        }
    }

    Ok(())
}

pub struct PollIngestInfo {
    multiple: bool,
    is_closed: Option<bool>,
    closed_at: Option<chrono::DateTime<chrono::FixedOffset>>,
    options: Vec<(String, Option<i32>)>,
}

/// Ingestion flow for Page, Image, Article, and Note. Should not be called with any other objects.
async fn ingest_postlike(
    obj: Verified<KnownObject>,
    found_from: FoundFrom,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<IngestResult>, crate::Error> {
    let (ext, to, in_reply_to, obj_id, poll_info) = match obj.deref() {
        KnownObject::Page(obj) => (Some(&obj.ext_one), obj.to(), None, obj.id_unchecked(), None),
        KnownObject::Image(obj) => (Some(&obj.ext_one), obj.to(), None, obj.id_unchecked(), None),
        KnownObject::Article(obj) => (Some(&obj.ext_one), obj.to(), None, obj.id_unchecked(), None),
        KnownObject::Note(obj) => (
            Some(&obj.ext_one),
            obj.to(),
            obj.in_reply_to(),
            obj.id_unchecked(),
            None,
        ),
        KnownObject::Question(obj) => (
            None,
            obj.to(),
            obj.in_reply_to(),
            obj.id_unchecked(),
            Some({
                #[derive(Deserialize)]
                struct OptionObject {
                    name: String,
                    replies: Option<crate::apub_util::AnyCollection>,
                }

                let (multiple, options) = if let Some(any_of) = obj.any_of() {
                    (true, any_of)
                } else if let Some(one_of) = obj.one_of() {
                    (false, one_of)
                } else {
                    return Err(crate::Error::InternalStrStatic("Invalid poll"));
                };

                let options = options
                    .iter()
                    .map(|value| serde_json::from_value(serde_json::to_value(value)?))
                    .collect::<Result<Vec<OptionObject>, _>>()?;
                let options = options
                    .into_iter()
                    .map(|value| {
                        let remote_count = value.replies.and_then(|coll| coll.total_items());
                        (value.name, remote_count.map(|x| x as i32))
                    })
                    .collect();

                let (is_closed, closed_at) = match obj.closed() {
                    Some(value) => match value {
                        activitystreams::primitives::ClosedValue::ObjectOrLink(_) => (None, None),
                        activitystreams::primitives::ClosedValue::DateTime(timestamp) => {
                            (None, Some(timestamp.as_datetime().clone()))
                        }
                        activitystreams::primitives::ClosedValue::Boolean(value) => {
                            (Some(*value), None)
                        }
                    },
                    None => (None, None),
                };

                PollIngestInfo {
                    multiple,
                    options,
                    is_closed,
                    closed_at,
                }
            }),
        ),
        _ => return Ok(None), // shouldn't happen?
    };
    let target = ext.as_ref().and_then(|x| x.target.as_ref());

    let community_found = match target
        .as_ref()
        .and_then(|target| target.as_one().and_then(|x| x.id()))
        .map(|target_id| {
            if let Some(super::LocalObjectRef::CommunityOutbox(community_local_id)) =
                super::LocalObjectRef::try_from_uri(target_id, &ctx.host_url_apub)
            {
                Some(community_local_id)
            } else {
                None
            }
        }) {
        Some(Some(community_local_id)) => Some((community_local_id, true)),
        Some(None) | None => match found_from {
            FoundFrom::Announce {
                community_local_id,
                community_is_local,
                ..
            } => Some((community_local_id, community_is_local)),
            FoundFrom::Refresh => {
                if let Some(obj_id) = obj_id {
                    let db = ctx.db_pool.get().await?;

                    let row = db.query_opt("SELECT id, local FROM community WHERE id=(SELECT community FROM post WHERE ap_id=$1)", &[&obj_id.as_str()]).await?;
                    row.map(|row| (CommunityLocalID(row.get(0)), row.get(1)))
                } else {
                    None
                }
            }
            _ => match to {
                None => None,
                Some(maybe) => maybe
                    .iter()
                    .filter_map(|any| {
                        any.as_xsd_any_uri()
                            .and_then(|uri| {
                                if let Some(super::LocalObjectRef::Community(community_id)) =
                                    super::LocalObjectRef::try_from_uri(uri, &ctx.host_url_apub)
                                {
                                    Some(community_id)
                                } else {
                                    None
                                }
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
                poll_info,
                Verified(obj).into(),
                ctx,
            )
            .await?
            .map(IngestResult::Post)),
            KnownObject::Image(obj) => Ok(handle_received_page_for_community(
                community_local_id,
                community_is_local,
                found_from.as_announce(),
                poll_info,
                Verified(obj).into(),
                ctx,
            )
            .await?
            .map(IngestResult::Post)),
            KnownObject::Article(obj) => Ok(handle_received_page_for_community(
                community_local_id,
                community_is_local,
                found_from.as_announce(),
                poll_info,
                Verified(obj).into(),
                ctx,
            )
            .await?
            .map(IngestResult::Post)),
            KnownObject::Question(obj) => Ok(handle_received_page_for_community(
                community_local_id,
                community_is_local,
                found_from.as_announce(),
                poll_info,
                Verified(try_transform_inner(obj)?),
                ctx,
            )
            .await?
            .map(IngestResult::Post)),
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
                    let sensitive = obj.ext_two.sensitive;

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
                            sensitive,
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
                                        .map(|obj| {
                                            obj.unwrap()
                                                .take_url()
                                                .as_ref()
                                                .and_then(|href| {
                                                    href.iter()
                                                        .filter_map(|x| x.as_xsd_any_uri())
                                                        .next()
                                                })
                                                .map(|href| href.as_str().to_owned())
                                        }),
                                    ),
                                    Some("Image") => Some(
                                        activitystreams::object::Image::from_any_base(base.clone())
                                            .map(|obj| {
                                                obj.unwrap()
                                                    .take_url()
                                                    .as_ref()
                                                    .and_then(|href| {
                                                        href.iter()
                                                            .filter_map(|x| x.as_xsd_any_uri())
                                                            .next()
                                                    })
                                                    .map(|href| href.as_str().to_owned())
                                            }),
                                    ),
                                    Some("Link") => Some(
                                        activitystreams::link::Link::<
                                            activitystreams::link::kind::LinkType,
                                        >::from_any_base(
                                            base.clone()
                                        )
                                        .map(|obj| {
                                            obj.unwrap()
                                                .take_href()
                                                .map(|href| href.as_str().to_owned())
                                        }),
                                    ),
                                    _ => None,
                                }
                            })
                            .transpose()?
                            .flatten();
                        let sensitive = obj.ext_two.sensitive;

                        Ok(Some(IngestResult::Post(
                            handle_recieved_post(
                                object_id.clone(),
                                title,
                                href.as_deref(),
                                content,
                                media_type,
                                created.as_ref(),
                                author,
                                community_local_id,
                                community_is_local,
                                found_from.as_announce(),
                                poll_info,
                                sensitive,
                                ctx,
                            )
                            .await?,
                        )))
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
                    let sensitive = obj.ext_two.sensitive;

                    let id = handle_recieved_reply(
                        obj_id,
                        content.unwrap_or(""),
                        media_type,
                        created.as_ref(),
                        author,
                        in_reply_to,
                        attachment_href,
                        sensitive,
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
            log::debug!("Couldn't find community for post");
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
            if let Some(super::LocalObjectRef::Community(community_id)) =
                super::LocalObjectRef::try_from_uri(target, &ctx.host_url_apub)
            {
                let row = db
                    .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
                    .await?;
                if let Some(row) = row {
                    let local: bool = row.get(0);
                    if local {
                        db.execute("INSERT INTO community_follow (community, follower, local, ap_id, accepted) VALUES ($1, $2, FALSE, $3, TRUE) ON CONFLICT (community, follower) DO UPDATE SET ap_id = $3, accepted = TRUE", &[&community_id, &follower_local_id, &activity_ap_id.as_str()]).await?;

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
        remote_url: ap_id.clone(),
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
    sensitive: Option<bool>,
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

            let target = if let Some(local_id) =
                super::LocalObjectRef::try_from_uri(&term_ap_id, &ctx.host_url_apub)
            {
                match local_id {
                    super::LocalObjectRef::Post(post_id) => Some(ReplyTarget::Post { id: post_id }),
                    super::LocalObjectRef::Comment(local_comment_id) => {
                        let row = db
                            .query_opt("SELECT post FROM reply WHERE id=$1", &[&local_comment_id])
                            .await?;
                        row.map(|row| ReplyTarget::Comment {
                            id: local_comment_id,
                            post: PostLocalID(row.get(0)),
                        })
                    }
                    _ => None,
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

                let sensitive = sensitive.unwrap_or(false);

                let row = db.query_opt(
                    "INSERT INTO reply (post, parent, author, content_text, content_html, created, local, ap_id, attachment_href, sensitive) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), FALSE, $7, $8, $9) ON CONFLICT (ap_id) DO NOTHING RETURNING id",
                    &[&post, &parent, &author, &content_text, &content_html, &created, &object_id.as_str(), &attachment_href, &sensitive],
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
                        sensitive,
                        mentions: Cow::Borrowed(&[]),
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
    poll_info: Option<PollIngestInfo>,
    obj: Verified<ExtendedPostlike<activitystreams::object::Object<Kind>>>,
    ctx: Arc<crate::RouteContext>,
) -> Result<Option<PostIngestResult>, crate::Error> {
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
    let sensitive = obj.ext_two.sensitive;

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
                poll_info,
                sensitive,
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
    poll_info: Option<PollIngestInfo>,
    sensitive: Option<bool>,
    ctx: Arc<crate::RouteContext>,
) -> Result<PostIngestResult, crate::Error> {
    let mut db = ctx.db_pool.get().await?;
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

    let sensitive = sensitive.unwrap_or(false);

    let (post_local_id, poll_output) = {
        let trans = db.transaction().await?;
        let row = trans.query_one(
            "INSERT INTO post (author, href, content_text, content_html, title, created, community, local, ap_id, approved, approved_ap_id, updated_local, sensitive) VALUES ($1, $2, $3, $4, $5, COALESCE($6, current_timestamp), $7, FALSE, $8, $9, $10, current_timestamp, $11) ON CONFLICT (ap_id) DO UPDATE SET approved=($9 OR post.approved), approved_ap_id=(CASE WHEN $9 THEN $10 ELSE post.approved_ap_id END), updated_local=current_timestamp, sensitive=$11 RETURNING id, poll_id",
            &[&author, &href, &content_text, &content_html, &title, &created, &community_local_id, &object_id.as_str(), &approved, &is_announce.map(|x| x.as_str()), &sensitive],
        ).await?;
        let post_local_id = PostLocalID(row.get(0));
        let existing_poll_id: Option<i64> = row.get(1);

        let poll_output = if let Some(poll_id) = existing_poll_id {
            if let Some(poll_info) = &poll_info {
                let names: Vec<&str> = poll_info
                    .options
                    .iter()
                    .map(|(name, _)| name.deref())
                    .collect();
                let counts: Vec<Option<i32>> = poll_info
                    .options
                    .iter()
                    .map(|(_, count)| count.clone())
                    .collect();
                let indices: Vec<i32> = (0..(poll_info.options.len() as i32)).collect();

                let is_closed: bool = trans
                    .query_one(
                        "UPDATE poll SET multiple=$1, is_closed=$3, closed_at=$4 WHERE id=$2 RETURNING COALESCE(is_closed, closed_at < current_timestamp, FALSE)",
                        &[
                            &poll_info.multiple,
                            &poll_id,
                            &poll_info.is_closed,
                            &poll_info.closed_at,
                        ],
                    )
                    .await?
                    .get(0);
                trans
                    .execute(
                        "DELETE FROM poll_option WHERE poll_id=$1 AND NOT (name = ANY($2::TEXT[]))",
                        &[&poll_id, &names],
                    )
                    .await?;

                let options_rows = trans.query("INSERT INTO poll_option (poll_id, name, position, remote_vote_count) SELECT $1, * FROM UNNEST($2::TEXT[], $3::INTEGER[], $4::INTEGER[]) ON CONFLICT (poll_id, name) DO UPDATE SET position = excluded.position, remote_vote_count = excluded.remote_vote_count RETURNING id, position", &[&poll_id, &names, &indices, &counts]).await?;

                let mut options: Vec<_> = options_rows
                    .into_iter()
                    .map(|row| (PollOptionLocalID(row.get(0)), row.get::<_, i32>(1)))
                    .collect();
                options.sort_unstable_by_key(|x| x.1);

                Some((options, is_closed))
            } else {
                trans
                    .execute(
                        "UPDATE post SET poll_id=NULL WHERE id=$1",
                        &[&post_local_id],
                    )
                    .await?;
                trans
                    .execute("DELETE FROM poll WHERE id=$1", &[&poll_id])
                    .await?;

                None
            }
        } else {
            if let Some(poll_info) = &poll_info {
                let names: Vec<&str> = poll_info
                    .options
                    .iter()
                    .map(|(name, _)| name.deref())
                    .collect();
                let counts: Vec<Option<i32>> = poll_info
                    .options
                    .iter()
                    .map(|(_, count)| count.clone())
                    .collect();
                let indices: Vec<i32> = (0..(poll_info.options.len() as i32)).collect();

                let row = trans
                    .query_one(
                        "INSERT INTO poll (multiple, is_closed, closed_at) VALUES ($1, $2, $3) RETURNING id, COALESCE(is_closed, closed_at < current_timestamp, FALSE)",
                        &[&poll_info.multiple, &poll_info.is_closed, &poll_info.closed_at],
                    )
                    .await?;
                let poll_id: i64 = row.get(0);
                let is_closed: bool = row.get(1);

                let options_rows = trans.query("INSERT INTO poll_option (poll_id, name, position, remote_vote_count) SELECT $1, * FROM UNNEST($2::TEXT[], $3::INTEGER[], $4::INTEGER[]) RETURNING id, position", &[&poll_id, &names, &indices, &counts]).await?;
                trans
                    .execute(
                        "UPDATE post SET poll_id=$1 WHERE id=$2",
                        &[&poll_id, &post_local_id],
                    )
                    .await?;

                let mut options: Vec<_> = options_rows
                    .into_iter()
                    .map(|row| (PollOptionLocalID(row.get(0)), row.get::<_, i32>(1)))
                    .collect();
                options.sort_unstable_by_key(|x| x.1);

                Some((options, is_closed))
            } else {
                None
            }
        };

        trans.commit().await?;

        (post_local_id, poll_output)
    };

    if community_is_local {
        crate::on_local_community_add_post(community_local_id, post_local_id, object_id, ctx);
    }

    let poll = poll_output.map(|(options, is_closed)| {
        let info = poll_info.unwrap();

        crate::PollInfoOwned {
            multiple: info.multiple,
            options: options
                .into_iter()
                .zip(info.options)
                .map(|((id, _), (name, votes))| crate::PollOptionOwned {
                    id,
                    name,
                    votes: votes.unwrap_or(0) as u32,
                })
                .collect(),
            is_closed,
            closed_at: info.closed_at,
        }
    });

    Ok(PostIngestResult {
        id: post_local_id,
        poll,
    })
}

fn try_transform_inner<T: TryInto<U>, U>(
    l: ExtendedPostlike<T>,
) -> Result<ExtendedPostlike<U>, T::Error> {
    Ok(ExtendedPostlike::new(
        l.inner.try_into()?,
        l.ext_one,
        l.ext_two,
    ))
}
