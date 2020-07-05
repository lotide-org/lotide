use serde_derive::{Deserialize, Serialize};
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

#[derive(Deserialize, Serialize, Debug)]
pub struct FingerRequestQuery<'a> {
    pub resource: Cow<'a, str>,
    pub rel: Option<Cow<'a, str>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FingerLink<'a> {
    pub rel: Cow<'a, str>,
    #[serde(rename = "type")]
    pub type_: Option<Cow<'a, str>>,
    pub href: Option<Cow<'a, str>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FingerResponse<'a> {
    pub subject: Cow<'a, str>,
    #[serde(default)]
    pub aliases: Vec<Cow<'a, str>>,
    #[serde(default)]
    pub links: Vec<FingerLink<'a>>,
}

async fn handler_webfinger_get(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let query: FingerRequestQuery<'_> =
        serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    enum LocalRef<'a> {
        UserID(i64),
        CommunityID(i64),
        Name(&'a str),
    }

    #[derive(Debug)]
    enum LTActorType {
        User,
        Community,
    }

    let local_hostname = crate::get_url_host(&ctx.host_url_apub).unwrap();

    let found_ref = if query.resource.starts_with(&ctx.host_url_apub) {
        let rest = &query.resource[ctx.host_url_apub.len()..];
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
        && query.resource.ends_with(&format!("@{}", local_hostname))
    {
        let name = &query.resource[5..(query.resource.len() - (local_hostname.len() + 1))];

        Some(LocalRef::Name(name))
    } else {
        None
    };

    let db = ctx.db_pool.get().await?;

    let found: Option<(LTActorType, i64, Cow<'_, str>)> = match found_ref {
        Some(LocalRef::UserID(id)) => {
            let row = db
                .query_opt("SELECT username FROM person WHERE id=$1 AND local", &[&id])
                .await?;
            row.map(|row| (LTActorType::User, id, Cow::Owned(row.get(0))))
        }
        Some(LocalRef::CommunityID(id)) => {
            let row = db
                .query_opt("SELECT name FROM community WHERE id=$1 AND local", &[&id])
                .await?;
            row.map(|row| (LTActorType::Community, id, Cow::Owned(row.get(0))))
        }
        Some(LocalRef::Name(name)) => {
            let row = db.query_opt("(SELECT FALSE, id, username FROM person WHERE LOWER(username)=LOWER($1) AND local) UNION ALL (SELECT TRUE, id, name FROM community WHERE LOWER(name)=LOWER($1) AND local) LIMIT 1", &[&name]).await?;
            row.map(|row| {
                (
                    if row.get(0) {
                        LTActorType::Community
                    } else {
                        LTActorType::User
                    },
                    row.get(1),
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
        Some((ty, id, name)) => {
            let subject = format!("acct:{}@{}", name, local_hostname);
            let alias = match ty {
                LTActorType::User => {
                    crate::apub_util::get_local_person_apub_id(id, &ctx.host_url_apub)
                }
                LTActorType::Community => {
                    crate::apub_util::get_local_community_apub_id(id, &ctx.host_url_apub)
                }
            };

            let body = FingerResponse {
                subject: subject.into(),
                aliases: vec![(&alias).into()],
                links: vec![FingerLink {
                    rel: "self".into(),
                    type_: Some(crate::apub_util::ACTIVITY_TYPE.into()),
                    href: Some((&alias).into()),
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
