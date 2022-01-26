pub use lotide_types as types;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::ops::Deref;
use std::sync::Arc;

mod apub_util;
mod config;
mod lang;
mod migrate;
mod routes;
mod tasks;
mod worker;

use self::config::Config;
use self::types::{
    CommentLocalID, CommunityLocalID, NotificationID, PollOptionLocalID, PostLocalID, UserLocalID,
};

pub use self::lang::Translator;

#[derive(Clone, Serialize, Deserialize)]
#[serde(try_from = "url::Url")]
#[serde(into = "url::Url")]
pub struct BaseURL(url::Url);
impl BaseURL {
    pub fn path_segments_mut(&mut self) -> url::PathSegmentsMut {
        self.0.path_segments_mut().unwrap()
    }
    pub fn set_fragment(&mut self, fragment: Option<&str>) {
        self.0.set_fragment(fragment);
    }
}

impl std::ops::Deref for BaseURL {
    type Target = url::Url;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for BaseURL {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl From<BaseURL> for String {
    fn from(src: BaseURL) -> String {
        src.0.into()
    }
}

#[derive(Debug)]
pub struct CannotBeABase;
impl std::fmt::Display for CannotBeABase {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "That URL cannot be a base")
    }
}

impl std::convert::TryFrom<url::Url> for BaseURL {
    type Error = CannotBeABase;

    fn try_from(src: url::Url) -> Result<BaseURL, Self::Error> {
        if src.cannot_be_a_base() {
            Err(CannotBeABase)
        } else {
            Ok(BaseURL(src))
        }
    }
}

impl std::str::FromStr for BaseURL {
    type Err = crate::Error;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        let url: url::Url = src.parse()?;

        url.try_into()
            .map_err(|_| crate::Error::InternalStrStatic("Parsed URL cannot be a base"))
    }
}

impl From<BaseURL> for url::Url {
    fn from(src: BaseURL) -> url::Url {
        src.0
    }
}

impl From<BaseURL> for activitystreams::base::AnyBase {
    fn from(src: BaseURL) -> activitystreams::base::AnyBase {
        src.0.into()
    }
}

impl From<BaseURL> for activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase> {
    fn from(
        src: BaseURL,
    ) -> activitystreams::primitives::OneOrMany<activitystreams::base::AnyBase> {
        src.0.into()
    }
}

pub type ParamSlice<'a> = &'a [&'a (dyn tokio_postgres::types::ToSql + Sync)];

pub struct Pineapple {
    value: i32,
}

impl Pineapple {
    pub fn generate() -> Self {
        Self {
            value: rand::thread_rng().gen(),
        }
    }

    pub fn as_int(&self) -> i32 {
        self.value
    }
}

// implementing this trait is discouraged in favor of Display, but bs58 doesn't do streaming output
impl std::string::ToString for Pineapple {
    fn to_string(&self) -> String {
        bs58::encode(&self.value.to_be_bytes()).into_string()
    }
}

impl std::str::FromStr for Pineapple {
    type Err = bs58::decode::Error;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        let src = src.trim_matches(|c: char| !c.is_alphanumeric());

        let mut buf = [0; 4];
        bs58::decode(src).into(&mut buf)?;
        Ok(Self {
            value: i32::from_be_bytes(buf),
        })
    }
}

pub type DbPool = deadpool_postgres::Pool;
pub type HttpClient = hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>;

pub struct BaseContext {
    pub db_pool: DbPool,
    pub mailer: Option<lettre::AsyncSmtpTransport<lettre::Tokio1Executor>>,
    pub mail_from: Option<lettre::message::Mailbox>,
    pub host_url_api: String,
    pub host_url_apub: BaseURL,
    pub http_client: HttpClient,
    pub apub_proxy_rewrites: bool,
    pub media_location: Option<std::path::PathBuf>,
    pub api_ratelimit: henry::RatelimitBucket<std::net::IpAddr>,
    pub vapid_public_key_base64: String,
    pub vapid_signature_builder: web_push::PartialVapidSignatureBuilder,

    pub local_hostname: String,

    worker_trigger: tokio::sync::mpsc::Sender<()>,
}

impl BaseContext {
    pub fn process_href<'a>(
        &self,
        href: impl Into<Cow<'a, str>>,
        post_id: PostLocalID,
    ) -> Cow<'a, str> {
        let href = href.into();
        if href.starts_with("local-media://") {
            format!("{}/stable/posts/{}/href", self.host_url_api, post_id).into()
        } else {
            href
        }
    }

    pub fn process_href_opt<'a>(
        &self,
        href: Option<Cow<'a, str>>,
        post_id: PostLocalID,
    ) -> Option<Cow<'a, str>> {
        href.map(|href| self.process_href(href, post_id))
    }

    pub fn process_attachments_inner<'a>(
        &self,
        href: Option<Cow<'a, str>>,
        comment_id: CommentLocalID,
    ) -> Option<Cow<'a, str>> {
        href.map(|href| {
            if href.starts_with("local-media://") {
                format!(
                    "{}/stable/comments/{}/attachments/0/href",
                    self.host_url_api, comment_id
                )
                .into()
            } else {
                href
            }
        })
    }

    pub fn process_avatar_href<'a>(
        &self,
        href: impl Into<Cow<'a, str>>,
        user_id: UserLocalID,
    ) -> Cow<'a, str> {
        let href = href.into();
        if href.starts_with("local-media://") {
            format!("{}/stable/users/{}/avatar/href", self.host_url_api, user_id,).into()
        } else {
            href
        }
    }

    pub async fn enqueue_task<T: crate::tasks::TaskDef>(
        &self,
        task: &T,
    ) -> Result<(), crate::Error> {
        let db = self.db_pool.get().await?;
        db.execute(
            "INSERT INTO task (kind, params, max_attempts, created_at) VALUES ($1, $2, $3, current_timestamp)",
            &[&T::KIND, &tokio_postgres::types::Json(task), &T::MAX_ATTEMPTS],
        ).await?;

        match self.worker_trigger.clone().try_send(()) {
            Ok(_) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(crate::Error::InternalStrStatic("Worker channel closed"))
            }
        }
    }

    pub async fn enqueue_tasks<T: crate::tasks::TaskDef>(
        &self,
        tasks: &[T],
    ) -> Result<(), crate::Error> {
        let db = self.db_pool.get().await?;

        let tasks_param: Vec<_> = tasks.iter().map(tokio_postgres::types::Json).collect();

        db.execute(
            "INSERT INTO task (kind, max_attempts, created_at, params) SELECT $1, $3, current_timestamp, * FROM UNNEST($2::JSON[])",
            &[&T::KIND, &tasks_param, &T::MAX_ATTEMPTS],
        ).await?;

        match self.worker_trigger.clone().try_send(()) {
            Ok(_) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(crate::Error::InternalStrStatic("Worker channel closed"))
            }
        }
    }
}

pub type RouteContext = BaseContext;

pub type RouteNode<P> = trout::Node<
    P,
    hyper::Request<hyper::Body>,
    std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<hyper::Response<hyper::Body>, Error>> + Send>,
    >,
    Arc<RouteContext>,
>;

#[derive(Debug)]
pub enum Error {
    Internal(Box<dyn std::error::Error + Send>),
    InternalStr(String),
    InternalStrStatic(&'static str),
    UserError(hyper::Response<hyper::Body>),
    RoutingError(trout::RoutingFailure),
}

impl<T: 'static + std::error::Error + Send> From<T> for Error {
    fn from(err: T) -> Error {
        Error::Internal(Box::new(err))
    }
}

#[derive(Debug, PartialEq)]
pub enum APIDOrLocal {
    Local,
    APID(url::Url),
}

pub enum TimestampOrLatest {
    Latest,
    Timestamp(chrono::DateTime<chrono::offset::FixedOffset>),
}

impl std::fmt::Display for TimestampOrLatest {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TimestampOrLatest::Latest => write!(f, "latest"),
            TimestampOrLatest::Timestamp(ts) => write!(f, "{}", ts.timestamp()),
        }
    }
}

pub enum TimestampOrLatestParseError {
    Number(std::num::ParseIntError),
    Timestamp,
}

impl std::str::FromStr for TimestampOrLatest {
    type Err = TimestampOrLatestParseError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        if src == "latest" {
            Ok(TimestampOrLatest::Latest)
        } else {
            use chrono::offset::TimeZone;

            let ts = src.parse().map_err(TimestampOrLatestParseError::Number)?;
            let ts = chrono::offset::Utc
                .timestamp_opt(ts, 0)
                .single()
                .ok_or(TimestampOrLatestParseError::Timestamp)?;
            Ok(TimestampOrLatest::Timestamp(ts.into()))
        }
    }
}

#[derive(Debug)]
pub struct PostInfo<'a> {
    id: PostLocalID,
    author: Option<UserLocalID>,
    href: Option<&'a str>,
    content_text: Option<&'a str>,
    #[allow(dead_code)]
    content_markdown: Option<&'a str>,
    content_html: Option<&'a str>,
    title: &'a str,
    created: &'a chrono::DateTime<chrono::FixedOffset>,
    #[allow(dead_code)]
    community: CommunityLocalID,
    poll: Option<Cow<'a, PollInfo<'a>>>,
}

pub struct PostInfoOwned {
    id: PostLocalID,
    author: Option<UserLocalID>,
    href: Option<String>,
    content_text: Option<String>,
    content_markdown: Option<String>,
    content_html: Option<String>,
    title: String,
    created: chrono::DateTime<chrono::FixedOffset>,
    community: CommunityLocalID,
    poll: Option<PollInfoOwned>,
}

impl<'a> From<&'a PostInfoOwned> for PostInfo<'a> {
    fn from(src: &'a PostInfoOwned) -> PostInfo<'a> {
        PostInfo {
            id: src.id,
            author: src.author,
            href: src.href.as_deref(),
            content_text: src.content_text.as_deref(),
            content_markdown: src.content_markdown.as_deref(),
            content_html: src.content_html.as_deref(),
            title: &src.title,
            created: &src.created,
            community: src.community,
            poll: src.poll.as_ref().map(|x| Cow::Owned(x.into())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PollInfo<'a> {
    multiple: bool,
    options: Cow<'a, [PollOption<'a>]>,
}

pub struct PollInfoOwned {
    multiple: bool,
    options: Vec<PollOptionOwned>,
}

impl<'a> From<&'a PollInfoOwned> for PollInfo<'a> {
    fn from(src: &'a PollInfoOwned) -> Self {
        PollInfo {
            multiple: src.multiple,
            options: src.options.iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PollOption<'a> {
    id: PollOptionLocalID,
    name: &'a str,
    votes: u32,
}

#[derive(Clone)]
pub struct PollOptionOwned {
    id: PollOptionLocalID,
    name: String,
    votes: u32,
}

impl<'a> From<&'a PollOptionOwned> for PollOption<'a> {
    fn from(src: &'a PollOptionOwned) -> Self {
        PollOption {
            id: src.id,
            name: &src.name,
            votes: src.votes,
        }
    }
}

#[derive(Debug)]
pub struct CommentInfo<'a> {
    id: CommentLocalID,
    author: Option<UserLocalID>,
    post: PostLocalID,
    parent: Option<CommentLocalID>,
    content_text: Option<Cow<'a, str>>,
    #[allow(dead_code)]
    content_markdown: Option<Cow<'a, str>>,
    content_html: Option<Cow<'a, str>>,
    created: chrono::DateTime<chrono::FixedOffset>,
    ap_id: APIDOrLocal,
    attachment_href: Option<Cow<'a, str>>,
}

pub const KEY_BITS: u32 = 2048;

pub fn get_url_host(url: &url::Url) -> Option<String> {
    url.host_str().map(|host| match url.port() {
        Some(port) => format!("{}:{}", host, port),
        None => host.to_owned(),
    })
}

pub fn get_url_host_from_str(src: &str) -> Option<String> {
    src.parse().ok().as_ref().and_then(get_url_host)
}

pub fn get_actor_host<'a>(
    local: bool,
    ap_id: Option<&str>,
    local_hostname: &'a str,
) -> Option<Cow<'a, str>> {
    if local {
        Some(local_hostname.into())
    } else {
        ap_id.and_then(get_url_host_from_str).map(Cow::from)
    }
}

pub fn get_actor_host_or_unknown<'a>(
    local: bool,
    ap_id: Option<&str>,
    local_hostname: &'a str,
) -> Cow<'a, str> {
    get_actor_host(local, ap_id, local_hostname).unwrap_or(Cow::Borrowed("[unknown]"))
}

pub fn get_path_and_query(url: &url::Url) -> Result<String, url::ParseError> {
    Ok(format!("{}{}", url.path(), url.query().unwrap_or("")))
}

fn slice_iter<'a>(
    s: &'a [&'a (dyn postgres_types::ToSql + Sync)],
) -> impl ExactSizeIterator<Item = &'a dyn postgres_types::ToSql> + 'a {
    s.iter().map(|s| *s as _)
}

pub async fn query_stream(
    db: &tokio_postgres::Client,
    statement: &(impl tokio_postgres::ToStatement + ?Sized),
    params: ParamSlice<'_>,
) -> Result<tokio_postgres::RowStream, tokio_postgres::Error> {
    db.query_raw(statement, slice_iter(params)).await
}

pub fn common_response_builder() -> http::response::Builder {
    hyper::Response::builder().header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
}

pub fn empty_response() -> hyper::Response<hyper::Body> {
    common_response_builder()
        .status(hyper::StatusCode::NO_CONTENT)
        .body(Default::default())
        .unwrap()
}

pub fn simple_response(
    code: hyper::StatusCode,
    text: impl Into<hyper::Body>,
) -> hyper::Response<hyper::Body> {
    common_response_builder()
        .status(code)
        .body(text.into())
        .unwrap()
}

pub fn json_response(body: &impl serde::Serialize) -> Result<hyper::Response<hyper::Body>, Error> {
    let body = serde_json::to_vec(&body)?;
    Ok(common_response_builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body.into())?)
}

pub async fn res_to_error(
    res: hyper::Response<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    if res.status().is_success() {
        Ok(res)
    } else {
        let bytes = hyper::body::to_bytes(res.into_body()).await?;
        Err(crate::Error::InternalStr(format!(
            "Error in remote response: {}",
            String::from_utf8_lossy(&bytes)
        )))
    }
}

pub trait ReqParts {
    fn headers(&self) -> &hyper::HeaderMap<hyper::header::HeaderValue>;
}

impl<T> ReqParts for hyper::Request<T> {
    fn headers(&self) -> &hyper::HeaderMap<hyper::header::HeaderValue> {
        self.headers()
    }
}

impl ReqParts for http::request::Parts {
    fn headers(&self) -> &hyper::HeaderMap<hyper::header::HeaderValue> {
        &self.headers
    }
}

lazy_static::lazy_static! {
    static ref LANG_MAP: HashMap<unic_langid::LanguageIdentifier, fluent::FluentResource> = {
        let mut result = HashMap::new();

        result.insert(unic_langid::langid!("de"), fluent::FluentResource::try_new(include_str!("../res/lang/de.ftl").to_owned()).expect("Failed to parse translation"));
        result.insert(unic_langid::langid!("en"), fluent::FluentResource::try_new(include_str!("../res/lang/en.ftl").to_owned()).expect("Failed to parse translation"));
        result.insert(unic_langid::langid!("eo"), fluent::FluentResource::try_new(include_str!("../res/lang/eo.ftl").to_owned()).expect("Failed to parse translation"));
        result.insert(unic_langid::langid!("fr"), fluent::FluentResource::try_new(include_str!("../res/lang/fr.ftl").to_owned()).expect("Failed to parse translation"));

        result
    };

    static ref LANGS: Vec<unic_langid::LanguageIdentifier> = {
        LANG_MAP.keys().cloned().collect()
    };
}

pub fn get_lang_for_req(req: &impl ReqParts) -> Translator {
    get_lang_for_header(
        req.headers()
            .get(hyper::header::ACCEPT_LANGUAGE)
            .and_then(|x| x.to_str().ok()),
    )
}

pub fn get_lang_for_header(accept_language: Option<&str>) -> Translator {
    let default = unic_langid::langid!("en");
    let languages = match accept_language {
        Some(accept_language) => {
            let requested = fluent_langneg::accepted_languages::parse(accept_language);
            fluent_langneg::negotiate_languages(
                &requested,
                &LANGS,
                Some(&default),
                fluent_langneg::NegotiationStrategy::Filtering,
            )
        }
        None => vec![&default],
    };

    let mut bundle = fluent::concurrent::FluentBundle::new(languages.iter().copied());
    for lang in languages {
        if let Err(errors) = bundle.add_resource(&LANG_MAP[lang]) {
            for err in errors {
                match err {
                    fluent::FluentError::Overriding { .. } => {}
                    _ => {
                        log::error!("Failed to add language resource: {:?}", err);
                        break;
                    }
                }
            }
        }
    }

    Translator::new(bundle)
}

pub fn get_auth_token(req: &impl ReqParts) -> Option<uuid::Uuid> {
    use headers::Header;

    let value = match req.headers().get(hyper::header::AUTHORIZATION) {
        Some(value) => {
            match headers::Authorization::<headers::authorization::Bearer>::decode(
                &mut std::iter::once(value),
            ) {
                Ok(value) => Some(value.0.token().to_owned()),
                Err(_) => None,
            }
        }
        None => None,
    };

    value.and_then(|value| value.parse::<uuid::Uuid>().ok())
}

pub async fn authenticate(
    req: &impl ReqParts,
    db: &tokio_postgres::Client,
) -> Result<Option<UserLocalID>, Error> {
    match get_auth_token(req) {
        None => Ok(None),
        Some(token) => {
            let row = db
                .query_opt("SELECT person FROM login WHERE token=$1", &[&token])
                .await?;

            match row {
                Some(row) => Ok(Some(UserLocalID(row.get(0)))),
                None => Ok(None),
            }
        }
    }
}

pub async fn require_login(
    req: &impl ReqParts,
    db: &tokio_postgres::Client,
) -> Result<UserLocalID, Error> {
    authenticate(req, db).await?.ok_or_else(|| {
        Error::UserError(simple_response(
            hyper::StatusCode::UNAUTHORIZED,
            "Login Required",
        ))
    })
}

pub async fn is_site_admin(db: &tokio_postgres::Client, user: UserLocalID) -> Result<bool, Error> {
    let row = db
        .query_opt("SELECT is_site_admin FROM person WHERE id=$1", &[&user])
        .await?;
    Ok(match row {
        None => false,
        Some(row) => row.get(0),
    })
}

pub async fn is_local_user(db: &tokio_postgres::Client, user: UserLocalID) -> Result<bool, Error> {
    let row = db
        .query_opt("SELECT local FROM person WHERE id=$1", &[&user])
        .await?;
    Ok(match row {
        None => false,
        Some(row) => row.get(0),
    })
}

pub fn spawn_task<F: std::future::Future<Output = Result<(), Error>> + Send + 'static>(task: F) {
    use futures::future::TryFutureExt;
    tokio::spawn(task.map_err(|err| {
        log::error!("Error in task: {:?}", err);
    }));
}

pub fn render_markdown(src: &str) -> String {
    let parser = pulldown_cmark::Parser::new(src);

    let stream = pdcm_linkify::AutoLinker::new(parser);

    let mut output = String::new();
    pulldown_cmark::html::push_html(&mut output, stream);

    output
}

lazy_static::lazy_static! {
    static ref SANITIZER: ammonia::Builder<'static> = {
        let mut builder = ammonia::Builder::default();
        builder.link_rel(Some("ugc noopener noreferrer"));
        builder.rm_tags(&["img"]);

        builder
    };
}

pub fn clean_html(src: &str) -> String {
    SANITIZER.clean(src).to_string()
}

pub fn on_local_community_add_post(
    community: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
    log::debug!("on_community_add_post");
    crate::apub_util::spawn_announce_community_post(community, post_local_id, post_ap_id, ctx);
}

pub fn on_local_community_add_comment(
    community: CommunityLocalID,
    comment_local_id: CommentLocalID,
    comment_ap_id: url::Url,
    ctx: Arc<crate::RouteContext>,
) {
    crate::apub_util::spawn_announce_community_comment(
        community,
        comment_local_id,
        comment_ap_id,
        ctx,
    );
}

pub fn on_post_add_comment(comment: CommentInfo<'static>, ctx: Arc<crate::RouteContext>) {
    use futures::future::TryFutureExt;

    log::debug!("on_post_add_comment");
    spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let res = futures::future::try_join(
            db.query_opt(
                "SELECT community.id, community.local, community.ap_id, community.ap_inbox, post.local, post.ap_id, person.id, person.ap_id, COALESCE(person.ap_shared_inbox, person.ap_inbox) FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.id = $1 AND post.community = community.id",
                &[&comment.post.raw()],
            )
            .map_err(crate::Error::from),
            async {
                match comment.parent {
                    Some(parent) => {
                        let row = db.query_one(
                            "SELECT reply.local, reply.ap_id, person.id, person.ap_id, COALESCE(person.ap_shared_inbox, person.ap_inbox) FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE reply.id=$1",
                            &[&parent],
                        ).await?;

                        let author_local_id = row.get::<_, Option<_>>(2).map(UserLocalID);

                        if row.get(0) {
                            Ok(Some((crate::apub_util::get_local_comment_apub_id(parent, &ctx.host_url_apub), Some(crate::apub_util::get_local_person_apub_id(author_local_id.unwrap(), &ctx.host_url_apub)), true, author_local_id, None)))
                        } else {
                            let author_ap_inbox: Option<url::Url> = row.get::<_, Option<_>>(4).map(|x: &str| std::str::FromStr::from_str(x)).transpose()?;
                            row.get::<_, Option<&str>>(1).map(|x: &str| -> Result<(BaseURL, Option<BaseURL>, bool, Option<UserLocalID>, Option<url::Url>), crate::Error> { Ok((x.parse()?, row.get::<_, Option<&str>>(3).map(std::str::FromStr::from_str).transpose()?, false, author_local_id, author_ap_inbox)) }).transpose()
                        }
                    },
                    None => Ok(None),
                }
            }
        ).await?;

        if let Some(post_row) = res.0 {
            let community_local: bool = post_row.get(1);
            let post_local: bool = post_row.get(4);

            let post_ap_id = if post_local {
                Some(crate::apub_util::get_local_post_apub_id(
                    comment.post,
                    &ctx.host_url_apub,
                ))
            } else {
                post_row
                    .get::<_, Option<&str>>(5)
                    .map(std::str::FromStr::from_str)
                    .transpose()?
            };

            let comment_ap_id = match &comment.ap_id {
                crate::APIDOrLocal::APID(apid) => apid.clone(),
                crate::APIDOrLocal::Local => {
                    crate::apub_util::get_local_comment_apub_id(comment.id, &ctx.host_url_apub)
                        .into()
                }
            };

            let (
                parent_ap_id,
                post_or_parent_author_local_id,
                post_or_parent_author_local,
                post_or_parent_author_ap_id,
                post_or_parent_author_ap_inbox,
            ) = match comment.parent {
                None => {
                    let author_id = UserLocalID(post_row.get(6));
                    if post_local {
                        (
                            None,
                            Some(author_id),
                            Some(true),
                            Some(Cow::Owned(crate::apub_util::get_local_person_apub_id(
                                author_id,
                                &ctx.host_url_apub,
                            ))),
                            None,
                        )
                    } else {
                        (
                            None,
                            Some(author_id),
                            Some(false),
                            post_row
                                .get::<_, Option<_>>(7)
                                .map(std::str::FromStr::from_str)
                                .transpose()?
                                .map(Cow::Owned),
                            post_row
                                .get::<_, Option<_>>(8)
                                .map(std::str::FromStr::from_str)
                                .transpose()?
                                .map(Cow::Owned),
                        )
                    }
                }
                Some(_) => match &res.1 {
                    None => (None, None, None, None, None),
                    Some((
                        parent_ap_id,
                        parent_author_ap_id,
                        parent_local,
                        parent_author_local_id,
                        parent_author_ap_inbox,
                    )) => (
                        Some(parent_ap_id),
                        *parent_author_local_id,
                        Some(*parent_local),
                        parent_author_ap_id.as_ref().map(Cow::Borrowed),
                        parent_author_ap_inbox.as_ref().map(Cow::Borrowed),
                    ),
                },
            };

            // Generate notifications
            match comment.parent {
                Some(parent_id) => {
                    if let Some((_, _, parent_local, parent_author_id, _)) = res.1 {
                        if parent_local && parent_author_id != comment.author {
                            if let Some(parent_author_id) = parent_author_id {
                                let ctx = ctx.clone();
                                let comment_id = comment.id;
                                crate::spawn_task(async move {
                                    let db = ctx.db_pool.get().await?;
                                    let row = db.query_one(
                                        "INSERT INTO notification (kind, created_at, to_user, reply, parent_reply) VALUES ('reply_reply', current_timestamp, $1, $2, $3) RETURNING id",
                                        &[&parent_author_id, &comment_id.raw(), &parent_id.raw()],
                                    ).await?;
                                    ctx.enqueue_task(&tasks::SendNotification {
                                        notification: NotificationID(row.get(0)),
                                    })
                                    .await?;

                                    Ok(())
                                });
                            }
                        }
                    }
                }
                None => {
                    if post_local && post_or_parent_author_local_id != comment.author {
                        if let Some(post_or_parent_author_local_id) = post_or_parent_author_local_id
                        {
                            let ctx = ctx.clone();
                            let comment_id = comment.id;
                            let comment_post = comment.post;
                            crate::spawn_task(async move {
                                let db = ctx.db_pool.get().await?;
                                let row = db.query_one(
                                    "INSERT INTO notification (kind, created_at, to_user, reply, parent_post) VALUES ('post_reply', current_timestamp, $1, $2, $3) RETURNING id",
                                    &[&post_or_parent_author_local_id.raw(), &comment_id.raw(), &comment_post.raw()],
                                ).await?;
                                ctx.enqueue_task(&tasks::SendNotification {
                                    notification: NotificationID(row.get(0)),
                                })
                                .await?;

                                Ok(())
                            });
                        }
                    }
                }
            }

            // should always be Some
            if let Some(post_ap_id) = post_ap_id {
                let community_id = CommunityLocalID(post_row.get(0));
                if community_local {
                    crate::on_local_community_add_comment(
                        community_id,
                        comment.id,
                        comment_ap_id,
                        ctx.clone(),
                    );
                }
                if comment.ap_id == APIDOrLocal::Local {
                    let mut inboxes = HashSet::new();

                    if !community_local {
                        let community_inbox = std::str::FromStr::from_str(post_row.get(3))?;
                        inboxes.insert(community_inbox);
                    }

                    if post_or_parent_author_local == Some(false) {
                        if let Some(post_or_parent_author_ap_inbox) = post_or_parent_author_ap_inbox
                        {
                            inboxes.insert(post_or_parent_author_ap_inbox.into_owned());
                        }
                    }

                    if !inboxes.is_empty() {
                        let community_ap_id = if community_local {
                            apub_util::get_local_community_apub_id(community_id, &ctx.host_url_apub)
                                .into()
                        } else {
                            std::str::FromStr::from_str(post_row.get(2))?
                        };

                        crate::apub_util::spawn_enqueue_send_comment(
                            inboxes,
                            comment,
                            community_ap_id,
                            post_ap_id.into(),
                            parent_ap_id.map(|x| x.deref().clone()),
                            post_or_parent_author_ap_id.map(|x| x.into_owned().into()),
                            ctx,
                        );
                    }
                }
            }
        }

        Ok(())
    });
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let matches = clap::App::new("lotide")
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .value_name("FILE")
                .help("Sets a path to a config file")
                .takes_value(true),
        )
        .subcommand(
            clap::SubCommand::with_name("migrate").arg(
                clap::Arg::with_name("ACTION")
                    .required(true)
                    .default_value("up")
                    .possible_values(&["up", "down", "setup"]),
            ),
        )
        .get_matches();

    let config = Config::load(matches.value_of_os("config")).expect("Failed to load config");

    if let Some(matches) = matches.subcommand_matches("migrate") {
        crate::migrate::run(config, matches);
        Ok(())
    } else {
        run(config)
    }
}

#[tokio::main]
async fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let db_pool = deadpool_postgres::Pool::new(
        deadpool_postgres::Manager::new(
            config.database_url.parse().unwrap(),
            tokio_postgres::NoTls,
        ),
        16,
    );

    let vapid_key: openssl::ec::EcKey<openssl::pkey::Private> = {
        let db = db_pool.get().await?;
        let row = db
            .query_one("SELECT vapid_private_key FROM site WHERE local=TRUE", &[])
            .await?;
        match row.get(0) {
            Some(bytes) => openssl::ec::EcKey::private_key_from_pem(bytes)?,
            None => {
                let key = openssl::ec::EcKey::generate(
                    openssl::ec::EcGroup::from_curve_name(openssl::nid::Nid::X9_62_PRIME256V1)?
                        .as_ref(),
                )?;
                let private_key_bytes = key.private_key_to_pem()?;
                db.execute(
                    "UPDATE site SET vapid_private_key=$1 WHERE local=TRUE",
                    &[&private_key_bytes],
                )
                .await?;

                key
            }
        }
    };

    let vapid_signature_builder = web_push::VapidSignatureBuilder::from_pem_no_sub::<&[u8]>(
        vapid_key.private_key_to_pem()?.as_ref(),
    )?;
    let vapid_public_key_base64 = base64::encode_config(
        vapid_signature_builder.get_public_key(),
        base64::Config::new(base64::CharacterSet::UrlSafe, false),
    );

    let host_url_apub: url::Url = config
        .host_url_activitypub
        .parse()
        .expect("Failed to parse HOST_URL_ACTIVITYPUB");
    let host_url_apub: BaseURL = host_url_apub
        .try_into()
        .expect("HOST_URL_ACTIVITYPUB is not a valid base URL");

    let smtp_url: Option<url::Url> = config
        .smtp_url
        .as_ref()
        .map(|url| url.parse().expect("Failed to parse SMTP_URL"));
    let mailer = match smtp_url {
        None => None,
        Some(url) => {
            let host = url.host_str().expect("Missing host in SMTP_URL");
            let mut builder = match url.scheme() {
                "smtp" => {
                    lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::builder_dangerous(host)
                }
                "smtps" => lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::relay(host)
                    .expect("Failed to initialize SMTP transport"),
                _ => panic!("Unrecognized scheme for SMTP_URL"),
            };

            if url.username() != "" || url.password().is_some() {
                builder =
                    builder.credentials(lettre::transport::smtp::authentication::Credentials::new(
                        url.username().to_owned(),
                        url.password().unwrap_or("").to_owned(),
                    ));
            }

            Some(builder.build())
        }
    };

    let mail_from: Option<lettre::message::Mailbox> = config
        .smtp_from
        .as_ref()
        .map(|value| value.parse().expect("Failed to parse SMTP_FROM"));

    if mailer.is_some() && mail_from.is_none() {
        panic!("SMTP_URL was provided, but SMTP_FROM was not");
    }

    let allow_forwarded = config.allow_forwarded;

    let (worker_trigger, worker_rx) = tokio::sync::mpsc::channel(1);

    let routes = Arc::new(routes::route_root());
    let context = Arc::new(BaseContext {
        local_hostname: get_url_host(&host_url_apub)
            .expect("Couldn't find host in HOST_URL_ACTIVITYPUB"),

        db_pool,
        mailer,
        mail_from,
        media_location: config.media_location.clone(),
        host_url_api: config.host_url_api.clone(),
        host_url_apub,
        http_client: hyper::Client::builder().build(hyper_tls::HttpsConnector::new()),
        apub_proxy_rewrites: config.apub_proxy_rewrites,
        api_ratelimit: henry::RatelimitBucket::new(300),
        vapid_public_key_base64,
        vapid_signature_builder,

        worker_trigger,
    });

    worker::start_worker(context.clone(), worker_rx);

    let server = hyper::Server::bind(&(std::net::Ipv6Addr::UNSPECIFIED, config.port).into()).serve(
        hyper::service::make_service_fn(|sock: &hyper::server::conn::AddrStream| {
            let addr_direct = sock.remote_addr().ip();
            let routes = routes.clone();
            let context = context.clone();
            async move {
                Ok::<_, hyper::Error>(hyper::service::service_fn(move |req| {
                    let routes = routes.clone();
                    let context = context.clone();
                    async move {
                        let ratelimit_addr = if allow_forwarded {
                            if let Some(value) = req
                                .headers()
                                .get(hyper::header::HeaderName::from_static("x-forwarded-for"))
                            {
                                match value
                                    .to_str()
                                    .map_err(|_| ())
                                    .and_then(|value| value.split(", ").next().ok_or(()))
                                    .and_then(|value| value.parse().map_err(|_| ()))
                                {
                                    Err(_) => {
                                        return Ok(simple_response(
                                            hyper::StatusCode::BAD_REQUEST,
                                            "Invalid X-Forwarded-For value",
                                        ));
                                    }
                                    Ok(value) => Some(value),
                                }
                            } else {
                                None
                            }
                        } else {
                            Some(addr_direct)
                        };

                        let ratelimit_ok = match ratelimit_addr {
                            Some(addr) => context.api_ratelimit.try_call(addr).await,
                            None => true,
                        };
                        let result = if !ratelimit_ok {
                            Ok(simple_response(
                                hyper::StatusCode::TOO_MANY_REQUESTS,
                                "Ratelimit exceeded.",
                            ))
                        } else if req.method() == hyper::Method::OPTIONS
                            && req.uri().path().starts_with("/api")
                        {
                            hyper::Response::builder()
                                .status(hyper::StatusCode::NO_CONTENT)
                                .header(hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                                .header(
                                    hyper::header::ACCESS_CONTROL_ALLOW_METHODS,
                                    "GET, POST, PUT, PATCH, DELETE",
                                )
                                .header(
                                    hyper::header::ACCESS_CONTROL_ALLOW_HEADERS,
                                    "Content-Type, Authorization",
                                )
                                .body(Default::default())
                                .map_err(Into::into)
                        } else {
                            match routes.route(req, context) {
                                Ok(fut) => fut.await,
                                Err(err) => Err(Error::RoutingError(err)),
                            }
                        };

                        Ok::<_, hyper::Error>(match result {
                            Ok(val) => val,
                            Err(Error::UserError(res)) => res,
                            Err(Error::RoutingError(err)) => {
                                let code = match err {
                                    trout::RoutingFailure::NotFound => hyper::StatusCode::NOT_FOUND,
                                    trout::RoutingFailure::MethodNotAllowed => {
                                        hyper::StatusCode::METHOD_NOT_ALLOWED
                                    }
                                };

                                simple_response(code, code.canonical_reason().unwrap())
                            }
                            Err(Error::Internal(err)) => {
                                log::error!("Error: {:?}", err);

                                simple_response(
                                    hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                    "Internal Server Error",
                                )
                            }
                            Err(Error::InternalStr(err)) => {
                                log::error!("Error: {}", err);

                                simple_response(
                                    hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                    "Internal Server Error",
                                )
                            }
                            Err(Error::InternalStrStatic(err)) => {
                                log::error!("Error: {}", err);

                                simple_response(
                                    hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                    "Internal Server Error",
                                )
                            }
                        })
                    }
                }))
            }
        }),
    );

    server.await?;

    Ok(())
}
