use crate::types::{
    ActorLocalRef, CommunityLocalID, FingerLink, FingerRequestQuery, FingerResponse, UserLocalID,
};
use std::borrow::Cow;
use std::sync::Arc;

pub fn route_well_known() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child(
            "nodeinfo",
            crate::RouteNode::new().with_handler_async("GET", handler_nodeinfo_get),
        )
        .with_child(
            "webfinger",
            crate::RouteNode::new().with_handler_async("GET", handler_webfinger_get),
        )
}

async fn handler_nodeinfo_get(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let body = serde_json::to_vec(&serde_json::json!({
        "links": [
            {
                "rel": "http://nodeinfo.diaspora.software/ns/schema/2.0",
                "href": format!("{}/unstable/nodeinfo/2.0", ctx.host_url_api),
            }
        ]
    }))?
    .into();

    Ok(hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/jrd+json")
        .body(body)?)
}

async fn handler_webfinger_get(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let query: FingerRequestQuery<'_> =
        serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    enum LocalRef<'a> {
        UserID(UserLocalID),
        CommunityID(CommunityLocalID),
        Name(&'a str),
    }

    let found_ref =
        if let Some(rest) = crate::apub_util::try_strip_host(&query.resource, &ctx.host_url_apub) {
            if rest.starts_with("/users/") {
                if let Ok(id) = rest[7..].parse() {
                    Some(LocalRef::UserID(id))
                } else {
                    None
                }
            } else if rest.starts_with("/communities/") {
                if let Ok(id) = rest[13..].parse() {
                    Some(LocalRef::CommunityID(id))
                } else {
                    None
                }
            } else {
                None
            }
        } else if query.resource.starts_with("acct:")
            && query
                .resource
                .ends_with(&format!("@{}", ctx.local_hostname))
        {
            let name = &query.resource[5..(query.resource.len() - (ctx.local_hostname.len() + 1))];

            Some(LocalRef::Name(name))
        } else {
            None
        };

    let db = ctx.db_pool.get().await?;

    let found: Option<(ActorLocalRef, Cow<'_, str>)> = match found_ref {
        Some(LocalRef::UserID(id)) => {
            let row = db
                .query_opt("SELECT username FROM person WHERE id=$1 AND local", &[&id])
                .await?;
            row.map(|row| (ActorLocalRef::Person(id), Cow::Owned(row.get(0))))
        }
        Some(LocalRef::CommunityID(id)) => {
            let row = db
                .query_opt("SELECT name FROM community WHERE id=$1 AND local", &[&id])
                .await?;
            row.map(|row| (ActorLocalRef::Community(id), Cow::Owned(row.get(0))))
        }
        Some(LocalRef::Name(name)) => {
            let row = db.query_opt("(SELECT FALSE, id, username FROM person WHERE LOWER(username)=LOWER($1) AND local) UNION ALL (SELECT TRUE, id, name FROM community WHERE LOWER(name)=LOWER($1) AND local) LIMIT 1", &[&name]).await?;
            row.map(|row| {
                let id = row.get(1);
                (
                    if row.get(0) {
                        ActorLocalRef::Community(CommunityLocalID(id))
                    } else {
                        ActorLocalRef::Person(UserLocalID(id))
                    },
                    Cow::Owned(row.get(2)),
                )
            })
        }
        None => None,
    };

    Ok(match found {
        None => {
            crate::simple_response(hyper::StatusCode::NOT_FOUND, "Nothing found for that query")
        }
        Some((actor_ref, name)) => {
            let subject = format!("acct:{}@{}", name, ctx.local_hostname);
            let alias = match actor_ref {
                ActorLocalRef::Person(id) => {
                    crate::apub_util::get_local_person_apub_id(id, &ctx.host_url_apub)
                }
                ActorLocalRef::Community(id) => {
                    crate::apub_util::get_local_community_apub_id(id, &ctx.host_url_apub)
                }
            };
            let alias = alias.as_str();

            let body = FingerResponse {
                subject: subject.into(),
                aliases: vec![alias.into()],
                links: vec![FingerLink {
                    rel: "self".into(),
                    type_: Some(crate::apub_util::ACTIVITY_TYPE.into()),
                    href: Some(alias.into()),
                }],
            };

            let body = serde_json::to_vec(&body)?;
            let body = body.into();

            hyper::Response::builder()
                .header(hyper::header::CONTENT_TYPE, "application/jrd+json")
                .body(body)?
        }
    })
}
