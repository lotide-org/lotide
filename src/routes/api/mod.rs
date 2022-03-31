use crate::lang;
use crate::types::{
    CommentLocalID, CommunityLocalID, FingerRequestQuery, FingerResponse, JustURL, PostLocalID,
    RespAvatarInfo, RespList, RespLoginUserInfo, RespMinimalAuthorInfo, RespMinimalCommentInfo,
    RespMinimalCommunityInfo, RespMinimalPostInfo, RespPostCommentInfo, RespPostListPost,
    RespSiteModlogEvent, RespSiteModlogEventDetails, UserLocalID,
};
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

mod comments;
mod communities;
mod flags;
mod forgot_password;
mod media;
mod posts;
mod stable;
mod users;

lazy_static::lazy_static! {
    static ref USERNAME_ALLOWED_CHARS: HashSet<char> = {
        use unic_char_range::chars;
        chars!('a'..='z').into_iter().chain(chars!('A'..='Z')).chain(chars!('0'..='9')).chain(std::iter::once('_'))
            .collect()
    };
}

#[derive(Debug)]
struct InvalidNumber58;

fn parse_number_58(src: &str) -> Result<i64, InvalidNumber58> {
    let mut buf = [0; 8];
    match bs58::decode(src).into(&mut buf) {
        Err(_) => Err(InvalidNumber58),
        Ok(count) => {
            if count == 8 {
                Ok(i64::from_be_bytes(buf))
            } else {
                Err(InvalidNumber58)
            }
        }
    }
}

fn format_number_58(src: i64) -> String {
    bs58::encode(src.to_be_bytes()).into_string()
}

pub struct ValueConsumer<'a> {
    targets: Vec<&'a mut Option<Box<dyn tokio_postgres::types::ToSql + Send + Sync>>>,
    start_idx: usize,
    used: usize,
}

impl<'a> ValueConsumer<'a> {
    fn push(&mut self, value: impl tokio_postgres::types::ToSql + Sync + Send + 'static) -> usize {
        *self.targets[self.used] = Some(Box::new(value));
        self.used += 1;

        self.start_idx + self.used
    }
}

pub struct InvalidPage;
impl InvalidPage {
    fn into_user_error(self) -> crate::Error {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            "Invalid page",
        ))
    }
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SortType {
    Hot,
    New,
    Top,
}

impl SortType {
    pub fn post_sort_sql(&self) -> &'static str {
        match self {
            SortType::Hot => "hot_rank((SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author), post.created) DESC",
            SortType::New => "post.created DESC, post.id DESC",
            SortType::Top => "(SELECT COUNT(*) FROM post_like WHERE post = post.id AND person != post.author) DESC, post.id DESC",
        }
    }

    pub fn comment_sort_sql(&self) -> &'static str {
        match self {
            SortType::Hot => "hot_rank((SELECT COUNT(*) FROM reply_like WHERE reply = reply.id AND person != reply.author), reply.created) DESC",
            SortType::New => "reply.created DESC",
            SortType::Top => "(SELECT COUNT(*) FROM reply_like WHERE reply = reply.id AND person != reply.author) DESC, reply.id DESC",
        }
    }

    pub fn handle_page(
        &self,
        page: Option<&str>,
        table: &str,
        sort_sticky: bool,
        mut value_out: ValueConsumer,
    ) -> Result<(Option<String>, Option<String>), InvalidPage> {
        match page {
            None => Ok((None, None)),
            Some(page) => match self {
                SortType::Hot | SortType::Top => {
                    let page: i64 = parse_number_58(page).map_err(|_| InvalidPage)?;
                    let idx = value_out.push(page);
                    Ok((None, Some(format!(" OFFSET ${}", idx))))
                }
                SortType::New => {
                    let page: (
                        Option<bool>,
                        chrono::DateTime<chrono::offset::FixedOffset>,
                        i64,
                    ) = {
                        let mut spl = page.split(',');

                        let sticky = if sort_sticky {
                            Some(spl.next().ok_or(InvalidPage)?)
                        } else {
                            None
                        };
                        let ts = spl.next().ok_or(InvalidPage)?;
                        let u = spl.next().ok_or(InvalidPage)?;
                        if spl.next().is_some() {
                            return Err(InvalidPage);
                        } else {
                            use chrono::TimeZone;

                            let sticky: Option<bool> = sticky
                                .map(|x| x.parse().map_err(|_| InvalidPage))
                                .transpose()?;
                            let ts: i64 = ts.parse().map_err(|_| InvalidPage)?;
                            let u: i64 = u.parse().map_err(|_| InvalidPage)?;

                            let ts = chrono::offset::Utc.timestamp_nanos(ts);

                            (sticky, ts.into(), u)
                        }
                    };

                    let idx1 = value_out.push(page.1);
                    let idx2 = value_out.push(page.2);

                    let base = format!(
                        "({2}.created < ${0} OR ({2}.created = ${0} AND {2}.id <= ${1}))",
                        idx1, idx2, table,
                    );

                    Ok((
                        Some(match page.0 {
                            None => format!(" AND {}", base),
                            Some(true) => format!(" AND ((NOT {}.sticky) OR {})", table, base),
                            Some(false) => format!(" AND ((NOT {}.sticky) AND {})", table, base),
                        }),
                        None,
                    ))
                }
            },
        }
    }

    fn get_next_comments_page(
        &self,
        comment: RespPostCommentInfo,
        limit: u8,
        current_page: Option<&str>,
    ) -> String {
        match self {
            SortType::Hot | SortType::Top => format_number_58(
                i64::from(limit)
                    + match current_page {
                        None => 0,
                        Some(current_page) => parse_number_58(current_page).unwrap(),
                    },
            ),
            SortType::New => {
                let ts: chrono::DateTime<chrono::offset::FixedOffset> =
                    comment.created.parse().unwrap();
                format!("{},{}", ts.timestamp_nanos(), comment.base.id)
            }
        }
    }

    fn get_next_posts_page(
        &self,
        post: &RespPostListPost<'_>,
        sort_sticky: bool,
        limit: u8,
        current_page: Option<&str>,
    ) -> String {
        match self {
            SortType::Hot | SortType::Top => format_number_58(
                i64::from(limit)
                    + match current_page {
                        None => 0,
                        Some(current_page) => parse_number_58(current_page).unwrap(),
                    },
            ),
            SortType::New => {
                let ts: chrono::DateTime<chrono::offset::FixedOffset> =
                    post.created.parse().unwrap();

                let ts = ts.timestamp_nanos();

                if sort_sticky {
                    format!("{},{},{}", post.sticky, ts, post.id)
                } else {
                    format!("{},{}", ts, post.id)
                }
            }
        }
    }
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum CommunitiesSortType {
    OldLocal,
    Alphabetic,
}

impl CommunitiesSortType {
    pub fn sort_sql(&self) -> &'static str {
        match self {
            Self::OldLocal => "community.id ASC",
            Self::Alphabetic => "community.name ASC, ap_id ASC",
        }
    }

    pub fn handle_page(
        &self,
        page: Option<&str>,
        mut value_out: ValueConsumer,
    ) -> Result<(Option<String>, Option<String>), InvalidPage> {
        match page {
            None => Ok((None, None)),
            Some(page) => match self {
                Self::OldLocal => {
                    let start_id: i64 = parse_number_58(page).map_err(|_| InvalidPage)?;
                    let idx = value_out.push(start_id);
                    Ok((Some(format!(" AND community.id >= ${}", idx)), None))
                }
                Self::Alphabetic => {
                    let mut spl = page.split(',');

                    let name = spl.next().ok_or(InvalidPage)?;
                    let name =
                        String::from_utf8(bs58::decode(name).into_vec().map_err(|_| InvalidPage)?)
                            .map_err(|_| InvalidPage)?;

                    match spl.next() {
                        None => {
                            let idx = value_out.push(name);
                            Ok((Some(format!(" AND community.name >= ${}", idx)), None))
                        }
                        Some(apid) => {
                            let apid = String::from_utf8(
                                bs58::decode(apid).into_vec().map_err(|_| InvalidPage)?,
                            )
                            .map_err(|_| InvalidPage)?;

                            if spl.next().is_some() {
                                return Err(InvalidPage);
                            }

                            let idx1 = value_out.push(name);
                            let idx2 = value_out.push(apid);

                            Ok((Some(format!(" AND community.name > ${} OR (community.name = ${} AND community.ap_id >= ${})", idx1, idx1, idx2)), None))
                        }
                    }
                }
            },
        }
    }

    pub fn get_next_page(
        &self,
        community: &RespMinimalCommunityInfo,
        _current_page: Option<&str>,
    ) -> String {
        match self {
            Self::OldLocal => format_number_58(community.id.raw()),
            Self::Alphabetic => {
                let mut result = bs58::encode(community.name.as_bytes()).into_string();

                if !community.local {
                    if let Some(url) = &community.remote_url {
                        result.push(',');
                        result.push_str(&bs58::encode(url.as_bytes()).into_string());
                    }
                }

                result
            }
        }
    }
}

pub fn default_replies_depth() -> u8 {
    3
}

pub fn default_replies_limit() -> u8 {
    30
}

pub fn default_comment_sort() -> SortType {
    SortType::Hot
}

pub fn route_api() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child(
            "unstable",
            crate::RouteNode::new()
                .with_child(
                    "actors:lookup",
                    crate::RouteNode::new().with_child_str(
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, route_unstable_actors_lookup),
                    ),
                )
                .with_child("flags", flags::route_flags())
                .with_child(
                    "logins",
                    crate::RouteNode::new()
                        .with_handler_async(hyper::Method::POST, route_unstable_logins_create)
                        .with_child(
                            "~current",
                            crate::RouteNode::new()
                                .with_handler_async(
                                    hyper::Method::GET,
                                    route_unstable_logins_current_get,
                                )
                                .with_handler_async(
                                    hyper::Method::DELETE,
                                    route_unstable_logins_current_delete,
                                ),
                        ),
                )
                .with_child("media", media::route_media())
                .with_child(
                    "nodeinfo/2.0",
                    crate::RouteNode::new()
                        .with_handler_async(hyper::Method::GET, route_unstable_nodeinfo_20_get),
                )
                .with_child(
                    "objects:lookup",
                    crate::RouteNode::new().with_child_str(
                        crate::RouteNode::new()
                            .with_handler_async(hyper::Method::GET, route_unstable_objects_lookup),
                    ),
                )
                .with_child("communities", communities::route_communities())
                .with_child(
                    "instance",
                    crate::RouteNode::new()
                        .with_handler_async(hyper::Method::GET, route_unstable_instance_get)
                        .with_handler_async(hyper::Method::PATCH, route_unstable_instance_patch)
                        .with_child(
                            "modlog",
                            crate::RouteNode::new().with_child(
                                "events",
                                crate::RouteNode::new().with_handler_async(
                                    hyper::Method::GET,
                                    route_unstable_instance_modlog_events_list,
                                ),
                            ),
                        ),
                )
                .with_child(
                    "misc",
                    crate::RouteNode::new().with_child(
                        "render_markdown",
                        crate::RouteNode::new().with_handler_async(
                            hyper::Method::POST,
                            route_unstable_misc_render_markdown,
                        ),
                    ),
                )
                .with_child("posts", posts::route_posts())
                .with_child("comments", comments::route_comments())
                .with_child("users", users::route_users())
                .with_child("forgot_password", forgot_password::route_forgot_password()),
        )
        .with_child("stable", stable::route_stable())
}

async fn insert_token(
    user_id: UserLocalID,
    db: &tokio_postgres::Client,
) -> Result<uuid::Uuid, tokio_postgres::Error> {
    let token = uuid::Uuid::new_v4();
    db.execute(
        "INSERT INTO login (token, person, created) VALUES ($1, $2, current_timestamp)",
        &[&token, &user_id],
    )
    .await?;

    Ok(token)
}

enum Lookup<'a> {
    Url(url::Url),
    WebFinger { user: &'a str, host: &'a str },
}

fn parse_lookup(src: &str) -> Result<Lookup, crate::Error> {
    if src.starts_with("http") {
        return Ok(Lookup::Url(src.parse()?));
    }
    if let Some(at_idx) = src.rfind('@') {
        let mut user = &src[..at_idx];
        let host = &src[(at_idx + 1)..];
        if user.starts_with("acct:") {
            user = &user[5..];
        } else if user.starts_with('@') {
            user = &user[1..];
        }
        return Ok(Lookup::WebFinger { user, host });
    }

    Err(crate::Error::InternalStrStatic(
        "Unrecognized lookup format",
    ))
}

async fn route_unstable_actors_lookup(
    params: (String,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (query,) = params;
    log::debug!("lookup {}", query);

    let lookup = parse_lookup(&query)?;

    let uri = match lookup {
        Lookup::Url(uri) => Some(uri),
        Lookup::WebFinger { user, host } => {
            let uri = format!(
                "https://{}/.well-known/webfinger?{}",
                host,
                serde_urlencoded::to_string(FingerRequestQuery {
                    resource: format!("acct:{}@{}", user, host).into(),
                    rel: Some("self".into()),
                })?
            );
            log::debug!("{}", uri);
            let res = ctx
                .http_client
                .request(hyper::Request::get(uri).body(Default::default())?)
                .await?;

            if res.status() == hyper::StatusCode::NOT_FOUND {
                log::debug!("not found");
                None
            } else {
                let res = crate::res_to_error(res).await?;

                let res = hyper::body::to_bytes(res.into_body()).await?;
                let res: FingerResponse = serde_json::from_slice(&res)?;

                let mut found_uri = None;
                for entry in res.links {
                    if entry.rel == "self"
                        && entry.type_.as_deref() == Some(crate::apub_util::ACTIVITY_TYPE)
                    {
                        if let Some(href) = entry.href {
                            found_uri = Some(href.parse()?);
                            break;
                        }
                    }
                }

                found_uri
            }
        }
    };

    let uri = match uri {
        Some(uri) => uri,
        None => {
            return Ok(crate::common_response_builder()
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .body("[]".into())?);
        }
    };

    let actor = crate::apub_util::fetch_actor(&uri, ctx).await?;

    let info = match actor {
        crate::apub_util::ActorLocalInfo::Community { id, .. } => {
            serde_json::json!({"id": id, "type": "community"})
        }
        crate::apub_util::ActorLocalInfo::User { id, .. } => {
            serde_json::json!({"id": id, "type": "user"})
        }
    };

    crate::json_response(&[info])
}

async fn route_unstable_logins_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct LoginsCreateBody<'a> {
        username: Cow<'a, str>,
        password: Cow<'a, str>,
    }

    let body: LoginsCreateBody<'_> = serde_json::from_slice(&body)?;

    let row = db
        .query_opt(
            "SELECT id, username, passhash, is_site_admin, suspended, EXISTS(SELECT 1 FROM notification WHERE to_user = person.id AND created_at > person.last_checked_notifications) FROM person WHERE LOWER(username)=LOWER($1) AND local",
            &[&body.username],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr(&lang::no_such_local_user_by_name()).into_owned(),
            ))
        })?;

    let id = UserLocalID(row.get(0));
    let username: &str = row.get(1);
    let passhash: Option<String> = row.get(2);

    let passhash = passhash.ok_or_else(|| {
        crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr(&lang::no_password()).into_owned(),
        ))
    })?;

    let req_password = body.password.to_owned();

    let correct =
        tokio::task::spawn_blocking(move || bcrypt::verify(req_password.as_ref(), &passhash))
            .await??;

    if correct {
        if row.get(4) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::user_suspended_error()).into_owned(),
            )));
        }

        let token = insert_token(id, &db).await?;

        crate::json_response(
            &serde_json::json!({"token": token.to_string(), "user": RespLoginUserInfo {
                id,
                username,
                is_site_admin: row.get(3),
                has_unread_notifications: row.get(5),
            }}),
        )
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr(&lang::password_incorrect()).into_owned(),
        ))
    }
}

async fn route_unstable_logins_current_get(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let row = db.query_one("SELECT username, is_site_admin, EXISTS(SELECT 1 FROM notification WHERE to_user = person.id AND created_at > person.last_checked_notifications) FROM person WHERE id=$1", &[&user]).await?;

    crate::json_response(&serde_json::json!({
        "user": RespLoginUserInfo {
            id: user,
            username: row.get(0),
            is_site_admin: row.get(1),
            has_unread_notifications: row.get(2),
        },
    }))
}

async fn route_unstable_logins_current_delete(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    if let Some(token) = crate::get_auth_token(&req) {
        let db = ctx.db_pool.get().await?;
        db.execute("DELETE FROM login WHERE token=$1", &[&token])
            .await?;
    }

    Ok(crate::empty_response())
}

async fn route_unstable_nodeinfo_20_get(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let local_posts = {
        let row = db
            .query_one("SELECT COUNT(*) FROM post WHERE local", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let local_comments = {
        let row = db
            .query_one("SELECT COUNT(*) FROM reply WHERE local", &[])
            .await?;
        row.get::<_, i64>(0)
    };
    let local_users = {
        let row = db
            .query_one("SELECT COUNT(*) FROM person WHERE local", &[])
            .await?;
        row.get::<_, i64>(0)
    };

    let open_registrations = {
        let row = db
            .query_one("SELECT signup_allowed FROM site WHERE local", &[])
            .await?;
        row.get::<_, bool>(0)
    };

    let body = serde_json::json!({
        "version": "2.0",
        "software": {
            "name": "lotide",
            "version": env!("CARGO_PKG_VERSION")
        },
        "protocols": ["activitypub"],
        "services": {
            "inbound": [],
            "outbound": []
        },
        "openRegistrations": open_registrations,
        "usage": {
            "users": {
                "total": local_users,
            },
            "localPosts": local_posts,
            "localComments": local_comments
        },
        "metadata": {}
    });

    let body = serde_json::to_vec(&body)?.into();

    Ok(crate::common_response_builder()
        .header(
            hyper::header::CONTENT_TYPE,
            "application/json; profile=http://nodeinfo.diaspora.software/ns/schema/2.0#",
        )
        .body(body)?)
}

async fn route_unstable_instance_get(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let row = db
        .query_one("SELECT description, description_markdown, description_html, signup_allowed FROM site WHERE local = TRUE", &[])
        .await?;
    let description_text: Option<&str> = row.get(0);
    let description_markdown: Option<&str> = row.get(1);
    let description_html: Option<&str> = row.get(2);
    let signup_allowed: bool = row.get(3);

    let body = serde_json::json!({
        "web_push_vapid_key": ctx.vapid_public_key_base64,
        "description": crate::types::Content {
            content_text: description_text.map(Cow::Borrowed),
            content_markdown: description_markdown.map(Cow::Borrowed),
            content_html_safe: description_html.map(|x| crate::clean_html(x)),
        },
        "software": {
            "name": "lotide",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "signup_allowed": signup_allowed
    });

    crate::json_response(&body)
}

async fn route_unstable_instance_patch(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    #[derive(Deserialize)]
    struct InstanceEditBody<'a> {
        description_text: Option<Cow<'a, str>>,
        description_markdown: Option<Cow<'a, str>>,
        description_html: Option<Cow<'a, str>>,
        signup_allowed: Option<bool>,
    }

    let lang = crate::get_lang_for_req(&req);

    let (req_parts, body) = req.into_parts();

    let body = hyper::body::to_bytes(body).await?;
    let body: InstanceEditBody = serde_json::from_slice(&body)?;

    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req_parts, &db).await?;

    let is_site_admin = crate::is_site_admin(&db, user).await?;

    if is_site_admin {
        let description_conflict = if body.description_text.is_some() {
            body.description_markdown.is_some() && body.description_html.is_some()
        } else {
            body.description_markdown.is_some() && body.description_html.is_some()
        };

        if description_conflict {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr(&lang::description_content_conflict()).into_owned(),
            )));
        }

        if let Some(description) = body.description_text {
            db.execute(
                "UPDATE site SET description=$1, description_markdown=NULL, description_html=NULL",
                &[&description],
            )
            .await?;
        } else if let Some(description) = body.description_markdown {
            let (html, md) = tokio::task::spawn_blocking(move || {
                (crate::render_markdown(&description), description)
            })
            .await?;

            db.execute(
                "UPDATE site SET description=NULL, description_markdown=$1, description_html=$2",
                &[&md, &html],
            )
            .await?;
        } else if let Some(description) = body.description_html {
            db.execute(
                "UPDATE site SET description=NULL, description_markdown=NULL, description_html=$1",
                &[&description],
            )
            .await?;
        }

        if let Some(signup_allowed) = body.signup_allowed {
            db.execute("UPDATE site SET signup_allowed=$1", &[&signup_allowed])
                .await?;
        }

        Ok(crate::empty_response())
    } else {
        Ok(crate::simple_response(
            hyper::StatusCode::FORBIDDEN,
            lang.tr(&lang::not_admin()).into_owned(),
        ))
    }
}

async fn route_unstable_objects_lookup(
    params: (String,),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (query,) = params;
    log::debug!("lookup {}", query);

    let lookup = parse_lookup(&query)?;

    let uri = match lookup {
        Lookup::Url(uri) => Some(uri),
        Lookup::WebFinger { user, host } => {
            let uri = format!(
                "https://{}/.well-known/webfinger?{}",
                host,
                serde_urlencoded::to_string(FingerRequestQuery {
                    resource: format!("acct:{}@{}", user, host).into(),
                    rel: Some("self".into()),
                })?
            );
            log::debug!("{}", uri);
            let res = ctx
                .http_client
                .request(hyper::Request::get(uri).body(Default::default())?)
                .await?;

            if res.status() == hyper::StatusCode::NOT_FOUND {
                log::debug!("not found");
                None
            } else {
                let res = crate::res_to_error(res).await?;

                let res = hyper::body::to_bytes(res.into_body()).await?;
                let res: FingerResponse = serde_json::from_slice(&res)?;

                let mut found_uri = None;
                for entry in res.links {
                    if entry.rel == "self"
                        && entry.type_.as_deref() == Some(crate::apub_util::ACTIVITY_TYPE)
                    {
                        if let Some(href) = entry.href {
                            found_uri = Some(href.parse()?);
                            break;
                        }
                    }
                }

                found_uri
            }
        }
    };

    let res = match &uri {
        Some(uri) => {
            let obj = crate::apub_util::fetch_ap_object(uri, &ctx.http_client).await?;

            crate::apub_util::ingest::ingest_object(
                obj,
                crate::apub_util::ingest::FoundFrom::Other,
                ctx,
            )
            .await?
        }
        None => None,
    };

    match res {
        None => Ok(crate::common_response_builder()
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body("[]".into())?),
        Some(res) => crate::json_response(&[res.into_ref()]),
    }
}

async fn apply_comments_replies<'a, T>(
    comments: &mut Vec<(T, RespPostCommentInfo<'a>)>,
    include_your_for: Option<UserLocalID>,
    depth: u8,
    limit: u8,
    sort: SortType,
    db: &tokio_postgres::Client,
    ctx: &'a crate::BaseContext,
) -> Result<(), crate::Error> {
    let ids = comments
        .iter()
        .map(|(_, comment)| comment.base.id)
        .collect::<Vec<_>>();
    if depth > 0 {
        let mut replies =
            get_comments_replies_box(&ids, include_your_for, depth - 1, limit, sort, db, ctx)
                .await?;

        for (_, comment) in comments.iter_mut() {
            let list: RespList<RespPostCommentInfo> =
                replies.remove(&comment.base.id).unwrap_or_default().into();
            comment.replies = Some(list);
        }
    } else {
        use futures::stream::TryStreamExt;

        let stream = crate::query_stream(
            db,
            "SELECT DISTINCT parent FROM reply WHERE parent = ANY($1)",
            &[&ids],
        )
        .await?;

        let with_replies: HashSet<CommentLocalID> = stream
            .map_err(crate::Error::from)
            .map_ok(|row| CommentLocalID(row.get(0)))
            .try_collect()
            .await?;

        for (_, comment) in comments.iter_mut() {
            comment.replies = if with_replies.contains(&comment.base.id) {
                None
            } else {
                Some(RespList::empty())
            };
        }
    }

    comments.retain(|(_, comment)| !comment.deleted || comment.has_replies() != Some(false));

    Ok(())
}

type PinBoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Default)]
struct CommentsRepliesInfoInternal<'a> {
    replies: Vec<RespPostCommentInfo<'a>>,
    next_page: Option<String>,
}

impl<'a> From<CommentsRepliesInfoInternal<'a>> for RespList<'a, RespPostCommentInfo<'a>> {
    fn from(src: CommentsRepliesInfoInternal<'a>) -> RespList<RespPostCommentInfo<'a>> {
        RespList {
            items: src.replies.into(),
            next_page: src.next_page.map(Cow::Owned),
        }
    }
}

fn get_comments_replies_box<'a: 'b, 'b>(
    parents: &'b [CommentLocalID],
    include_your_for: Option<UserLocalID>,
    depth: u8,
    limit: u8,
    sort: SortType,
    db: &'b tokio_postgres::Client,
    ctx: &'a crate::BaseContext,
) -> PinBoxFuture<'b, Result<HashMap<CommentLocalID, CommentsRepliesInfoInternal<'a>>, crate::Error>>
{
    Box::pin(get_comments_replies(
        parents,
        include_your_for,
        depth,
        limit,
        sort,
        None,
        db,
        ctx,
    ))
}

// https://github.com/rust-lang/rust-clippy/issues/7271
#[allow(clippy::needless_lifetimes)]
async fn get_comments_replies<'a>(
    parents: &[CommentLocalID],
    include_your_for: Option<UserLocalID>,
    depth: u8,
    limit: u8,
    sort: SortType,
    page: Option<&str>,
    db: &tokio_postgres::Client,
    ctx: &'a crate::BaseContext,
) -> Result<HashMap<CommentLocalID, CommentsRepliesInfoInternal<'a>>, crate::Error> {
    use futures::TryStreamExt;

    let limit_i = i64::from(limit) + 1;

    let sql1 = "SELECT result.* FROM UNNEST($1::BIGINT[]) JOIN LATERAL (SELECT reply.id, reply.author, reply.content_text, reply.created, reply.parent, reply.content_html, person.username, person.local, person.ap_id, reply.deleted, person.avatar, reply.attachment_href, reply.local, (SELECT COUNT(*) FROM reply_like WHERE reply = reply.id), reply.content_markdown, person.is_bot, reply.ap_id, reply.local, reply.sensitive";
    let (sql2, mut values): (_, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) =
        if include_your_for.is_some() {
            (
                ", EXISTS(SELECT 1 FROM reply_like WHERE reply = reply.id AND person = $3)",
                vec![&parents, &limit_i, &include_your_for],
            )
        } else {
            ("", vec![&parents, &limit_i])
        };
    let mut sql3 =
        " FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE parent = unnest"
            .to_owned();
    let mut sql4 = format!(
        " ORDER BY {}) AS result ON TRUE LIMIT $2",
        sort.comment_sort_sql()
    );

    let mut con1 = None;
    let mut con2 = None;
    let (page_part1, page_part2) = sort
        .handle_page(
            page,
            "reply",
            false,
            ValueConsumer {
                targets: vec![&mut con1, &mut con2],
                start_idx: values.len(),
                used: 0,
            },
        )
        .map_err(InvalidPage::into_user_error)?;
    if let Some(value) = &con1 {
        values.push(value.as_ref());
        if let Some(value) = &con2 {
            values.push(value.as_ref());
        }
    }

    if let Some(part) = page_part1 {
        sql3.push_str(&part);
    }
    if let Some(part) = page_part2 {
        sql4.push_str(&part);
    }

    let sql: String = format!("{}{}{}{}", sql1, sql2, sql3, sql4);
    let sql: &str = &sql;

    let stream = crate::query_stream(db, sql, &values).await?;

    let mut comments: Vec<_> = stream
        .map_err(crate::Error::from)
        .and_then(|row| {
            let id = CommentLocalID(row.get(0));
            let content_text: Option<String> = row.get(2);
            let content_html: Option<String> = row.get(5);
            let created: chrono::DateTime<chrono::FixedOffset> = row.get(3);
            let parent = CommentLocalID(row.get(4));
            let ap_id: Option<String> = row.get(16);
            let local: bool = row.get(17);
            let sensitive: bool = row.get(18);

            let remote_url = if local {
                Some(String::from(
                    crate::apub_util::LocalObjectRef::Comment(id).to_local_uri(&ctx.host_url_apub),
                ))
            } else {
                ap_id
            };

            let author_username: Option<String> = row.get(6);
            let author = author_username.map(|author_username| {
                let author_id = UserLocalID(row.get(1));
                let author_local: bool = row.get(7);
                let author_ap_id: Option<&str> = row.get(8);
                let author_avatar: Option<&str> = row.get(10);

                let author_remote_url = if author_local {
                    Some(String::from(
                        crate::apub_util::LocalObjectRef::User(author_id)
                            .to_local_uri(&ctx.host_url_apub),
                    ))
                } else {
                    author_ap_id.map(ToOwned::to_owned)
                };

                RespMinimalAuthorInfo {
                    id: author_id,
                    username: author_username.into(),
                    local: author_local,
                    host: crate::get_actor_host_or_unknown(
                        author_local,
                        author_ap_id,
                        &ctx.local_hostname,
                    ),
                    remote_url: author_remote_url.map(Cow::Owned),
                    is_bot: row.get(15),
                    avatar: author_avatar.map(|url| RespAvatarInfo {
                        url: ctx.process_avatar_href(url, author_id).into_owned().into(),
                    }),
                }
            });

            futures::future::ok((
                parent,
                RespPostCommentInfo {
                    base: RespMinimalCommentInfo {
                        id,
                        remote_url: remote_url.map(Cow::Owned),
                        content_text: content_text.map(From::from),
                        content_html_safe: content_html.map(|html| crate::clean_html(&html)),
                        sensitive,
                    },

                    attachments: match ctx
                        .process_attachments_inner(row.get::<_, Option<_>>(11).map(Cow::Owned), id)
                    {
                        None => vec![],
                        Some(href) => vec![JustURL { url: href }],
                    },
                    author,
                    content_markdown: row.get::<_, Option<String>>(14).map(Cow::Owned),
                    created: created.to_rfc3339(),
                    deleted: row.get(9),
                    local: row.get(12),
                    replies: Some(RespList::empty()),
                    score: row.get(13),
                    your_vote: include_your_for.map(|_| {
                        if row.get(19) {
                            Some(crate::types::Empty {})
                        } else {
                            None
                        }
                    }),
                },
            ))
        })
        .try_collect()
        .await?;

    apply_comments_replies(&mut comments, include_your_for, depth, limit, sort, db, ctx).await?;

    let mut result = HashMap::new();
    for (parent, comment) in comments {
        let entry = result
            .entry(parent)
            .or_insert_with(|| CommentsRepliesInfoInternal {
                replies: Vec::new(),
                next_page: None,
            });
        if entry.replies.len() < limit.into() {
            entry.replies.push(comment);
        } else {
            entry.next_page = Some(sort.get_next_comments_page(comment, limit, page));
        }
    }

    Ok(result)
}

async fn route_unstable_instance_modlog_events_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let db = ctx.db_pool.get().await?;

    fn default_limit() -> u32 {
        30
    }

    #[derive(Deserialize)]
    struct ModlogEventsListQuery<'a> {
        #[serde(default = "default_limit")]
        limit: u32,

        page: Option<Cow<'a, str>>,
    }

    let query: ModlogEventsListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let inner_limit = i64::from(query.limit) + 1;

    let page = query
        .page
        .as_deref()
        .map(parse_number_58)
        .transpose()
        .map_err(|_| InvalidPage.into_user_error())?;

    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&inner_limit];

    let rows = db.query(&format!("SELECT modlog_event.id, modlog_event.time, modlog_event.action, reply_post.id, reply_post.title, reply_post.local, reply_post.ap_id, reply_post.sensitive, person.id, person.username, person.local, person.ap_id, person.avatar, person.is_bot, reply_author.id, reply_author.username, reply_author.local, reply_author.ap_id, reply_author.avatar, reply_author.is_bot, post_community.id, post_community.name, post_community.local, post_community.ap_id, post_community.deleted, post_author.id, post_author.username, post_author.local, post_author.ap_id, post_author.avatar, post_author.is_bot FROM modlog_event LEFT OUTER JOIN reply ON (reply.id = modlog_event.reply) LEFT OUTER JOIN post AS reply_post ON (reply_post.id = reply.post) LEFT OUTER JOIN person ON (person.id = modlog_event.person) LEFT OUTER JOIN person AS reply_author ON (reply_author.id = reply.author) LEFT OUTER JOIN post ON (post.id = modlog_event.post) LEFT OUTER JOIN community AS post_community ON (post_community.id = post.community) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) WHERE modlog_event.by_community IS NULL{} ORDER BY modlog_event.id DESC LIMIT $1", if let Some(page) = &page {
        values.push(page);

        " AND modlog_event.id <= $2"
    } else {
        ""
    }), &values).await?;

    let (rows, next_page) = if rows.len() > query.limit as usize {
        let next_page = format_number_58(rows.last().unwrap().get(0));
        (&rows[..(query.limit as usize)], Some(Cow::Owned(next_page)))
    } else {
        (&rows[..], None)
    };

    let output = RespList {
        items: rows
            .iter()
            .filter_map(|row| {
                let time: chrono::DateTime<chrono::FixedOffset> = row.get(1);
                let action = row.get(2);

                let reply_post = row.get::<_, Option<_>>(3).map(|post_id| {
                    let post_id = PostLocalID(post_id);
                    let post_title = row.get(4);
                    let post_local: bool = row.get(5);
                    let post_ap_id: Option<&str> = row.get(6);
                    let post_sensitive: bool = row.get(7);

                    let post_remote_url = if post_local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::Post(post_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        post_ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalPostInfo {
                        id: post_id,
                        title: post_title,
                        remote_url: post_remote_url,
                        sensitive: post_sensitive,
                    }
                });

                let user = row.get::<_, Option<_>>(8).map(|user_id| {
                    let user_id = UserLocalID(user_id);
                    let local = row.get(10);
                    let ap_id: Option<&str> = row.get(11);
                    let avatar: Option<&str> = row.get(12);

                    let remote_url = if local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::User(user_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalAuthorInfo {
                        id: user_id,
                        username: Cow::Borrowed(row.get(9)),
                        local,
                        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                        avatar: avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, user_id).into_owned().into(),
                        }),
                        is_bot: row.get(13),
                        remote_url,
                    }
                });

                let reply_author = row.get::<_, Option<_>>(14).map(|user_id| {
                    let user_id = UserLocalID(user_id);
                    let local = row.get(16);
                    let ap_id: Option<&str> = row.get(17);
                    let avatar: Option<&str> = row.get(18);

                    let remote_url = if local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::User(user_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalAuthorInfo {
                        id: user_id,
                        username: Cow::Borrowed(row.get(15)),
                        local,
                        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                        avatar: avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, user_id).into_owned().into(),
                        }),
                        is_bot: row.get(19),
                        remote_url,
                    }
                });

                let post_community = row.get::<_, Option<_>>(20).map(|community_id| {
                    let community_id = CommunityLocalID(community_id);
                    let name = Cow::Borrowed(row.get(21));
                    let local = row.get(22);
                    let ap_id: Option<&str> = row.get(23);
                    let deleted = row.get(24);

                    let remote_url = if local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::Community(community_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalCommunityInfo {
                        id: community_id,
                        deleted,
                        local,
                        name,
                        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                        remote_url,
                    }
                });

                let post_author = row.get::<_, Option<_>>(25).map(|user_id| {
                    let user_id = UserLocalID(user_id);
                    let local = row.get(27);
                    let ap_id: Option<&str> = row.get(28);
                    let avatar: Option<&str> = row.get(29);

                    let remote_url = if local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::User(user_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalAuthorInfo {
                        id: user_id,
                        username: Cow::Borrowed(row.get(26)),
                        local,
                        host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                        avatar: avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, user_id).into_owned().into(),
                        }),
                        is_bot: row.get(30),
                        remote_url,
                    }
                });

                let details = match action {
                    "delete_post" => {
                        if let Some(community) = post_community {
                            if let Some(author) = post_author {
                                RespSiteModlogEventDetails::DeletePost { author, community }
                            } else {
                                return None;
                            }
                        } else {
                            return None;
                        }
                    }
                    "delete_reply" => {
                        if let Some(author) = reply_author {
                            if let Some(post) = reply_post {
                                RespSiteModlogEventDetails::DeleteComment { author, post }
                            } else {
                                return None;
                            }
                        } else {
                            return None;
                        }
                    }
                    "suspend_user" => {
                        if let Some(user) = user {
                            RespSiteModlogEventDetails::SuspendUser { user }
                        } else {
                            return None;
                        }
                    }
                    "unsuspend_user" => {
                        if let Some(user) = user {
                            RespSiteModlogEventDetails::UnsuspendUser { user }
                        } else {
                            return None;
                        }
                    }
                    _ => return None,
                };

                Some(RespSiteModlogEvent {
                    time: time.to_rfc3339(),
                    details,
                })
            })
            .collect(),
        next_page,
    };

    crate::json_response(&output)
}

async fn route_unstable_misc_render_markdown(
    _: (),
    _ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let body = hyper::body::to_bytes(req.into_body()).await?;

    #[derive(Deserialize)]
    struct RenderMarkdownBody<'a> {
        content_markdown: Cow<'a, str>,
    }

    let body: RenderMarkdownBody = serde_json::from_slice(&body)?;

    let html =
        tokio::task::spawn_blocking(move || crate::render_markdown(&body.content_markdown)).await?;

    crate::json_response(&serde_json::json!({ "content_html": html }))
}

// https://github.com/rust-lang/rust-clippy/issues/7271
#[allow(clippy::needless_lifetimes)]
pub async fn process_comment_content<'a, 'b>(
    lang: &'b crate::Translator,
    content_text: Option<Cow<'a, str>>,
    content_markdown: Option<String>,
) -> Result<(Option<Cow<'a, str>>, Option<String>, Option<String>), crate::Error> {
    if !(content_markdown.is_some() ^ content_text.is_some()) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr(&lang::comment_content_conflict()).into_owned(),
        )));
    }

    Ok(match content_markdown {
        Some(md) => {
            if md.trim().is_empty() {
                return Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::BAD_REQUEST,
                    lang.tr(&lang::comment_empty()).into_owned(),
                )));
            }

            let (html, md) =
                tokio::task::spawn_blocking(move || (crate::render_markdown(&md), md)).await?;
            (None, Some(md), Some(html))
        }
        None => match content_text {
            Some(text) => {
                if text.trim().is_empty() {
                    return Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr(&lang::comment_empty()).into_owned(),
                    )));
                }

                (Some(text), None, None)
            }
            None => (None, None, None),
        },
    })
}
