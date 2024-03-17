use futures::{Stream, StreamExt, TryStreamExt};
pub use lotide_types as types;
use markup5ever_rcdom as rcdom;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

mod apub_util;
mod config;
mod lang;
mod markdown;
mod migrate;
mod routes;
mod tasks;
mod worker;

use self::config::Config;
use self::types::{
    ActorLocalRef, CommentLocalID, CommunityLocalID, ImageHandling, NotificationID,
    PollOptionLocalID, PostLocalID, UserLocalID,
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
    pub user_agent: String,
    pub apub_proxy_rewrites: bool,
    pub media_storage: Option<MediaStorage>,
    pub api_ratelimit: henry::RatelimitBucket<std::net::IpAddr>,
    pub vapid_public_key_base64: String,
    pub vapid_signature_builder: web_push::PartialVapidSignatureBuilder,
    pub break_stuff: bool,
    pub dev_mode: bool,

    pub local_hostname: String,

    worker_trigger: Option<tokio::sync::mpsc::Sender<()>>,
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

        if let Some(worker_trigger) = &self.worker_trigger {
            match worker_trigger.clone().try_send(()) {
                Ok(_) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(crate::Error::InternalStrStatic("Worker channel closed"))
                }
            }
        } else {
            // separate worker, send notification through database

            db.execute("NOTIFY new_task", &[]).await?;
            Ok(())
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

        if let Some(worker_trigger) = &self.worker_trigger {
            match worker_trigger.clone().try_send(()) {
                Ok(_) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(()),
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    Err(crate::Error::InternalStrStatic("Worker channel closed"))
                }
            }
        } else {
            // separate worker, send notification through database

            db.execute("NOTIFY new_task", &[]).await?;
            Ok(())
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

#[derive(Debug, PartialEq, Clone)]
pub enum APIDOrLocal {
    Local,
    APID(url::Url),
}

#[derive(Clone, Copy, Debug)]
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
    #[allow(dead_code)]
    ap_id: &'a APIDOrLocal,
    author: Option<UserLocalID>,
    href: Option<&'a str>,
    content_text: Option<&'a str>,
    #[allow(dead_code)]
    content_markdown: Option<&'a str>,
    content_html: Option<&'a str>,
    title: &'a str,
    created: chrono::DateTime<chrono::FixedOffset>,
    #[allow(dead_code)]
    community: CommunityLocalID,
    poll: Option<Cow<'a, PollInfo<'a>>>,
    sensitive: bool,
    mentions: &'a [MentionInfo],
}

#[derive(Clone)]
pub struct PostInfoOwned {
    id: PostLocalID,
    ap_id: APIDOrLocal,
    author: Option<UserLocalID>,
    author_ap_id: Option<APIDOrLocal>,
    href: Option<String>,
    content_text: Option<String>,
    content_markdown: Option<String>,
    content_html: Option<String>,
    title: String,
    created: chrono::DateTime<chrono::FixedOffset>,
    community: CommunityLocalID,
    poll: Option<PollInfoOwned>,
    sensitive: bool,
    mentions: Vec<MentionInfo>,
}

impl<'a> From<&'a PostInfoOwned> for PostInfo<'a> {
    fn from(src: &'a PostInfoOwned) -> PostInfo<'a> {
        PostInfo {
            id: src.id,
            ap_id: &src.ap_id,
            author: src.author,
            href: src.href.as_deref(),
            content_text: src.content_text.as_deref(),
            content_markdown: src.content_markdown.as_deref(),
            content_html: src.content_html.as_deref(),
            title: &src.title,
            created: src.created,
            community: src.community,
            poll: src.poll.as_ref().map(|x| Cow::Owned(x.into())),
            sensitive: src.sensitive,
            mentions: &src.mentions,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PollInfo<'a> {
    multiple: bool,
    options: Cow<'a, [PollOption<'a>]>,
    closed_at: Option<&'a chrono::DateTime<chrono::FixedOffset>>,
}

#[derive(Clone)]
pub struct PollInfoOwned {
    multiple: bool,
    options: Vec<PollOptionOwned>,
    is_closed: bool,
    closed_at: Option<chrono::DateTime<chrono::FixedOffset>>,
}

impl<'a> From<&'a PollInfoOwned> for PollInfo<'a> {
    fn from(src: &'a PollInfoOwned) -> Self {
        PollInfo {
            multiple: src.multiple,
            options: src.options.iter().map(Into::into).collect(),
            closed_at: src.closed_at.as_ref(),
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

#[derive(Debug, Clone)]
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
    sensitive: bool,
    mentions: Cow<'a, [MentionInfo]>,
}

#[derive(Debug, Clone)]
pub struct MentionInfo {
    text: String,
    person: UserLocalID,
    ap_id: APIDOrLocal,
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
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
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
        result.insert(unic_langid::langid!("fa"), fluent::FluentResource::try_new(include_str!("../res/lang/fa.ftl").to_owned()).expect("Failed to parse translation"));

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

const UGC_LINK_REL: &str = "ugc noopener";

fn create_sanitizer_base() -> ammonia::Builder<'static> {
    let mut builder = ammonia::Builder::default();
    builder.link_rel(Some(UGC_LINK_REL));

    builder
}

macro_rules! html_ns {
    () => {
        html5ever::namespace_url!("")
    };
    ($ns:ident) => {{
        use html5ever::namespace_url;

        html5ever::ns!($ns)
    }};
}

lazy_static::lazy_static! {
    static ref SANITIZER_BASE: ammonia::Builder<'static> = create_sanitizer_base();

    static ref SANITIZER_REMOVE_IMAGES: ammonia::Builder<'static> = {
        let mut builder = create_sanitizer_base();
        builder.rm_tags(&["img"]);

        builder
    };

    static ref HTML_IMG_TAG: html5ever::QualName = html5ever::QualName::new(
        None,
        html_ns!(html),
        html5ever::local_name!("img"),
    );

    static ref HTML_A_TAG: html5ever::QualName = html5ever::QualName::new(
        None,
        html_ns!(html),
        html5ever::local_name!("a"),
    );
}

pub fn clean_html(src: &str, image_handling: ImageHandling) -> String {
    match image_handling {
        ImageHandling::Remove => SANITIZER_REMOVE_IMAGES.clean(src).to_string(),
        ImageHandling::Preserve => SANITIZER_BASE.clean(src).to_string(),
        ImageHandling::ConvertToLinks => {
            use html5ever::tendril::TendrilSink;

            let content = SANITIZER_BASE.clean(src).to_string();

            let dom = html5ever::parse_fragment(
                rcdom::RcDom::default(),
                Default::default(),
                // ???
                html5ever::QualName::new(None, html_ns!(html), html5ever::local_name!("body")),
                vec![],
            )
            .from_utf8()
            .read_from(&mut content.as_bytes())
            .unwrap();

            fn process_node(node: Rc<rcdom::Node>) -> Option<Rc<rcdom::Node>> {
                match &node.data {
                    rcdom::NodeData::Element { name, attrs, .. } => {
                        if name == &*HTML_IMG_TAG {
                            let attrs = attrs.borrow();
                            let alt = attrs
                                .iter()
                                .find(|x| x.name.local == html5ever::local_name!("alt"));
                            let src = attrs
                                .iter()
                                .find(|x| x.name.local == html5ever::local_name!("src"));

                            match src {
                                Some(src) => {
                                    let out_attrs = vec![
                                        html5ever::Attribute {
                                            name: html5ever::QualName::new(
                                                None,
                                                html_ns!(),
                                                html5ever::local_name!("href"),
                                            ),
                                            value: src.value.clone(),
                                        },
                                        html5ever::Attribute {
                                            name: html5ever::QualName::new(
                                                None,
                                                html_ns!(),
                                                html5ever::local_name!("rel"),
                                            ),
                                            value: UGC_LINK_REL.into(),
                                        },
                                    ];

                                    let text = match alt {
                                        Some(alt) if !alt.value.is_empty() => alt.value.clone(),
                                        Some(_) | None => "Image".into(),
                                    };

                                    let children = vec![rcdom::Node::new(rcdom::NodeData::Text {
                                        contents: std::cell::RefCell::new(text),
                                    })];

                                    Some(Rc::new(rcdom::Node {
                                        parent: std::cell::Cell::new(None),
                                        children: std::cell::RefCell::new(children),
                                        data: rcdom::NodeData::Element {
                                            name: HTML_A_TAG.clone(),
                                            attrs: std::cell::RefCell::new(out_attrs),
                                            template_contents: std::cell::RefCell::new(None),
                                            mathml_annotation_xml_integration_point: false,
                                        },
                                    }))
                                }
                                None => None, // may as well? shouldn't happen much anyway
                            }
                        } else {
                            {
                                let mut children = node.children.borrow_mut();

                                let mut i = 0;
                                while i < children.len() {
                                    match process_node(children[i].clone()) {
                                        None => {
                                            children.remove(i);
                                        }
                                        Some(new_child) => {
                                            children[i] = new_child;
                                            i += 1;
                                        }
                                    }
                                }
                            }

                            Some(node)
                        }
                    }
                    _ => Some(node),
                }
            }

            let output_root = process_node(dom.document.children.borrow()[0].clone());

            match output_root {
                None => "".to_owned(),
                Some(output_root) => {
                    let mut output = Vec::new();
                    html5ever::serialize(
                        &mut output,
                        &rcdom::SerializableHandle::from(output_root),
                        Default::default(),
                    )
                    .unwrap();

                    String::from_utf8(output).unwrap()
                }
            }
        }
    }
}

pub fn on_add_post(
    post: crate::PostInfoOwned,
    community_local: bool,
    is_new: bool, // TODO if not, is this really an "add"?
    ctx: Arc<crate::RouteContext>,
) {
    if let APIDOrLocal::Local = post.ap_id {
        apub_util::spawn_enqueue_send_local_post(post.clone(), ctx.clone());
    }

    crate::spawn_task(async move {
        let author = post.author;
        if community_local {
            on_local_community_add_post(
                post.community,
                post.id,
                match post.ap_id {
                    crate::APIDOrLocal::Local => crate::apub_util::LocalObjectRef::Post(post.id)
                        .to_local_uri(&ctx.host_url_apub)
                        .into(),
                    crate::APIDOrLocal::APID(url) => url,
                },
                author,
                match post.author_ap_id {
                    Some(crate::APIDOrLocal::Local) => Some(
                        crate::apub_util::LocalObjectRef::User(post.author.unwrap())
                            .to_local_uri(&ctx.host_url_apub)
                            .into(),
                    ),
                    Some(crate::APIDOrLocal::APID(url)) => Some(url),
                    None => None,
                },
                ctx.clone(),
            );
        }

        if is_new {
            let local_mentions: Vec<_> = post
                .mentions
                .iter()
                .filter(|x| x.ap_id == APIDOrLocal::Local && Some(x.person) != author)
                .map(|x| x.person)
                .collect();
            if !local_mentions.is_empty() {
                // local users should get notifications when mentioned

                let rows = {
                    let db = ctx.db_pool.get().await?;

                    db.query(
                        "INSERT INTO notification (kind, created_at, post, to_user) SELECT 'post_mention', current_timestamp, $1, * FROM UNNEST($2::BIGINT[])",
                        &[&post.id, &local_mentions],
                    ).await?
                };

                let tasks: Vec<_> = rows
                    .iter()
                    .map(|row| tasks::SendNotification {
                        notification: NotificationID(row.get(0)),
                    })
                    .collect();
                ctx.enqueue_tasks(&tasks).await?;
            }
        }

        Ok(())
    });
}

pub fn on_local_community_add_post(
    community: CommunityLocalID,
    post_local_id: PostLocalID,
    post_ap_id: url::Url,
    post_author: Option<UserLocalID>,
    post_author_ap_id: Option<url::Url>,
    ctx: Arc<crate::RouteContext>,
) {
    log::debug!("on_community_add_post");
    crate::apub_util::spawn_announce_community_post(
        community,
        post_local_id,
        post_ap_id,
        post_author,
        post_author_ap_id,
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
                "SELECT community.id, community.local, community.ap_id, person.ap_id, post.local, post.ap_id, person.id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.id = $1 AND post.community = community.id",
                &[&comment.post.raw()],
            )
            .map_err(crate::Error::from),
            async {
                match comment.parent {
                    Some(parent) => {
                        let row = db.query_one(
                            "SELECT reply.local, reply.ap_id, person.id, person.ap_id FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE reply.id=$1",
                            &[&parent],
                        ).await?;

                        let author_local_id = row.get::<_, Option<_>>(2).map(UserLocalID);

                        if row.get(0) {
                            Ok(Some((crate::apub_util::LocalObjectRef::Comment(parent).to_local_uri(&ctx.host_url_apub), Some(crate::apub_util::LocalObjectRef::User(author_local_id.unwrap()).to_local_uri(&ctx.host_url_apub)), true, author_local_id)))
                        } else {
                            row.get::<_, Option<&str>>(1).map(|x: &str| -> Result<(BaseURL, Option<BaseURL>, bool, Option<UserLocalID>), crate::Error> { Ok((x.parse()?, row.get::<_, Option<&str>>(3).map(std::str::FromStr::from_str).transpose()?, false, author_local_id)) }).transpose()
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
                Some(
                    crate::apub_util::LocalObjectRef::Post(comment.post)
                        .to_local_uri(&ctx.host_url_apub),
                )
            } else {
                post_row
                    .get::<_, Option<&str>>(5)
                    .map(std::str::FromStr::from_str)
                    .transpose()?
            };

            let (
                parent_ap_id,
                post_or_parent_author_local_id,
                post_or_parent_author_local,
                post_or_parent_author_ap_id,
            ) = match comment.parent {
                None => {
                    let author_id = UserLocalID(post_row.get(6));
                    if post_local {
                        (
                            None,
                            Some(author_id),
                            Some(true),
                            Some(Cow::Owned(
                                crate::apub_util::LocalObjectRef::User(author_id)
                                    .to_local_uri(&ctx.host_url_apub),
                            )),
                        )
                    } else {
                        (
                            None,
                            Some(author_id),
                            Some(false),
                            post_row
                                .get::<_, Option<_>>(3)
                                .map(std::str::FromStr::from_str)
                                .transpose()?
                                .map(Cow::Owned),
                        )
                    }
                }
                Some(_) => match &res.1 {
                    None => (None, None, None, None),
                    Some((
                        parent_ap_id,
                        parent_author_ap_id,
                        parent_local,
                        parent_author_local_id,
                    )) => (
                        Some(parent_ap_id),
                        *parent_author_local_id,
                        Some(*parent_local),
                        parent_author_ap_id.as_ref().map(Cow::Borrowed),
                    ),
                },
            };

            let mut already_notified = None;

            // Generate notifications
            match comment.parent {
                Some(parent_id) => {
                    if let Some((_, _, parent_local, parent_author_id)) = res.1 {
                        if parent_local && parent_author_id != comment.author {
                            if let Some(parent_author_id) = parent_author_id {
                                let ctx = ctx.clone();
                                let comment_id = comment.id;

                                already_notified = Some(parent_author_id);

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

                            already_notified = Some(post_or_parent_author_local_id);

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
                if comment.ap_id == APIDOrLocal::Local {
                    let mut audiences = HashSet::new();

                    if community_local {
                        crate::apub_util::spawn_enqueue_forward_local_comment_to_community_followers(
                            comment.clone(),
                            community_id,
                            &post_ap_id,
                            parent_ap_id.map(|x| x.deref().clone()),
                            post_or_parent_author_ap_id.as_ref().map(|x| x.deref().clone().into()),
                            ctx.clone(),
                        );
                    } else {
                        audiences.insert(crate::tasks::AudienceItem::Single(
                            ActorLocalRef::Community(community_id),
                        ));
                    }

                    if post_or_parent_author_local == Some(false) {
                        if let Some(user) = post_or_parent_author_local_id {
                            audiences.insert(crate::tasks::AudienceItem::Single(
                                ActorLocalRef::Person(user),
                            ));
                        }
                    }

                    for mention in &comment.mentions[..] {
                        if mention.ap_id != APIDOrLocal::Local {
                            audiences.insert(crate::tasks::AudienceItem::Single(
                                ActorLocalRef::Person(mention.person),
                            ));
                        }
                    }

                    if !audiences.is_empty() {
                        let community_ap_id = if community_local {
                            apub_util::LocalObjectRef::Community(community_id)
                                .to_local_uri(&ctx.host_url_apub)
                                .into()
                        } else {
                            std::str::FromStr::from_str(post_row.get(2))?
                        };

                        crate::apub_util::spawn_enqueue_send_comment(
                            audiences,
                            comment.clone(),
                            community_ap_id,
                            post_ap_id.into(),
                            parent_ap_id.map(|x| x.deref().clone()),
                            post_or_parent_author_ap_id.map(|x| x.into_owned().into()),
                            ctx.clone(),
                        );
                    }
                }
            }

            let local_mentions: Vec<_> = comment
                .mentions
                .iter()
                .filter(|x| {
                    x.ap_id == APIDOrLocal::Local
                        && Some(x.person) != already_notified
                        && Some(x.person) != comment.author
                })
                .map(|x| x.person)
                .collect();
            if !local_mentions.is_empty() {
                // local users should get notifications when mentioned

                let rows = {
                    let db = ctx.db_pool.get().await?;

                    db.query(
                        "INSERT INTO notification (kind, created_at, reply, parent_reply, parent_post, to_user) SELECT 'reply_mention', current_timestamp, $1, $2, $3, * FROM UNNEST($4::BIGINT[])",
                        &[&comment.id, &comment.parent, &comment.post, &local_mentions],
                    ).await?
                };

                let tasks: Vec<_> = rows
                    .iter()
                    .map(|row| tasks::SendNotification {
                        notification: NotificationID(row.get(0)),
                    })
                    .collect();
                ctx.enqueue_tasks(&tasks).await?;
            }
        }

        Ok(())
    });
}

pub enum MediaStorage {
    Local(std::path::PathBuf),
    S3 {
        client: rusoto_s3::S3Client,
        bucket: String,
    },
}

impl MediaStorage {
    pub async fn open(
        &self,
        path: &str,
    ) -> Result<
        std::pin::Pin<Box<dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>>,
        crate::Error,
    > {
        match self {
            MediaStorage::Local(root) => {
                let path = root.join(path);

                let file = tokio::fs::File::open(path).await?;
                Ok(Box::pin(
                    tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new())
                        .map_ok(|x| x.freeze()),
                ))
            }
            MediaStorage::S3 { client, bucket } => {
                use rusoto_s3::S3;

                let mut req: rusoto_s3::GetObjectRequest = Default::default();
                req.bucket = bucket.clone();
                req.key = path.to_owned();

                let res = client.get_object(req).await?;
                let body = res
                    .body
                    .ok_or(crate::Error::InternalStrStatic("Missing body in S3 return"))?;

                Ok(Box::pin(body))
            }
        }
    }

    pub async fn save(
        &self,
        src: impl Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + 'static,
        content_type: &str,
    ) -> Result<String, crate::Error> {
        match self {
            MediaStorage::Local(root) => {
                let filename = uuid::Uuid::new_v4().to_string();
                let path = root.join(&filename);

                {
                    use tokio::io::AsyncWriteExt;
                    let file = tokio::fs::File::create(path).await?;
                    src.try_fold(file, |mut file, chunk| async move {
                        file.write_all(chunk.as_ref()).await.map(|_| file)
                    })
                    .await?;
                }

                Ok(filename)
            }
            MediaStorage::S3 { client, bucket } => {
                use rusoto_s3::S3;

                let key = uuid::Uuid::new_v4().to_string();

                struct MultipartUploadState {
                    upload_id: String,
                    parts: Vec<rusoto_s3::CompletedPart>,
                }

                match src.map_err(crate::Error::from).fold(((None, vec![], 0), None), {
                    |(mut multipart_state, err), chunk| {
                        let key = key.clone();
                        async move {
                        match (err, chunk) {
                            (Some(err), _) => (multipart_state, Some(err)),
                            (None, Err(err)) => (multipart_state, Some(err)),
                            (None, Ok(chunk)) => {
                                multipart_state.2 += chunk.len();
                                multipart_state.1.push(chunk);

                                const MIN_PART_SIZE: usize = 5_000_000; // 5 MB

                                if multipart_state.2 > MIN_PART_SIZE {
                                    let upload_state = match &mut multipart_state.0 {
                                        Some(state) => state,
                                        None => {
                                            let output = match client.create_multipart_upload(rusoto_s3::CreateMultipartUploadRequest {
                                                bucket: bucket.clone(),
                                                key: key.clone(),
                                                content_type: Some(content_type.to_owned()),
                                                ..Default::default()
                                            }).await {
                                                Ok(output) => output,
                                                Err(err) => return (multipart_state, Some(err.into())),
                                            };

                                            let upload_id = match output.upload_id {
                                                Some(x) => x,
                                                None => return (multipart_state, Some(crate::Error::InternalStrStatic("Missing upload_id in S3 response"))),
                                            };

                                            multipart_state.0 = Some(MultipartUploadState {
                                                upload_id,
                                                parts: Vec::new(),
                                            });
                                            multipart_state.0.as_mut().unwrap()
                                        }
                                    };

                                    let part_number = (upload_state.parts.len() + 1) as i64;

                                    let current_part_contents = std::mem::replace(&mut multipart_state.1, Vec::new());
                                    let current_part_size = multipart_state.2;
                                    multipart_state.2 = 0;

                                    match client.upload_part(rusoto_s3::UploadPartRequest {
                                        body: Some(rusoto_core::ByteStream::new_with_size(futures::stream::iter(current_part_contents.into_iter().map(Ok)), current_part_size)),
                                        bucket: bucket.clone(),
                                        key: key.clone(),
                                        part_number,
                                        upload_id: upload_state.upload_id.clone(),
                                        ..Default::default()
                                    }).await {
                                        Err(err) => {
                                            return (multipart_state, Some(err.into()));
                                        }
                                        Ok(result) => {
                                            upload_state.parts.push(rusoto_s3::CompletedPart {
                                                e_tag: result.e_tag,
                                                part_number: Some(part_number),
                                            });
                                        }
                                    }

                                }

                                (multipart_state, None)
                            }
                        }
                    }
                    }
                }).await {
                    (multipart_state, None) => {
                        let current_part_contents = multipart_state.1;
                        let current_part_size = multipart_state.2;
                        if let Some(mut upload_state) = multipart_state.0 {
                            let part_number = (upload_state.parts.len() + 1) as i64;

                            let result = client.upload_part(rusoto_s3::UploadPartRequest {
                                body: Some(rusoto_core::ByteStream::new_with_size(futures::stream::iter(current_part_contents.into_iter().map(Ok)), current_part_size)),
                                bucket: bucket.clone(),
                                key: key.clone(),
                                part_number,
                                upload_id: upload_state.upload_id.clone(),
                                ..Default::default()
                            }).await?;
                            upload_state.parts.push(rusoto_s3::CompletedPart {
                                e_tag: result.e_tag,
                                part_number: Some(part_number),
                            });

                            client.complete_multipart_upload(rusoto_s3::CompleteMultipartUploadRequest {
                                bucket: bucket.clone(),
                                key: key.clone(),
                                upload_id: upload_state.upload_id,
                                ..Default::default()
                            }).await?;
                        } else {
                            client.put_object(rusoto_s3::PutObjectRequest {
                                bucket: bucket.clone(),
                                key: key.clone(),
                                body: Some(rusoto_core::ByteStream::new_with_size(futures::stream::iter(current_part_contents.into_iter().map(Ok)), current_part_size)),
                                ..Default::default()
                            }).await?;
                        }

                        Ok(key)
                    }
                    (multipart_state, Some(err)) => {
                        if let Some(upload_state) = multipart_state.0 {
                            if let Err(err) = client.abort_multipart_upload(rusoto_s3::AbortMultipartUploadRequest {
                                bucket: bucket.clone(),
                                key: key.clone(),
                                upload_id: upload_state.upload_id,
                                ..Default::default()
                            }).await {
                                log::error!("Failed to abort multipart upload: {:?}", err);
                            }
                        }

                        Err(err.into())
                    }
                }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let matches = clap::Command::new("lotide")
        .arg(
            clap::Arg::new("config")
                .short('c')
                .value_name("FILE")
                .help("Sets a path to a config file")
                .allow_invalid_utf8(true)
                .takes_value(true),
        )
        .subcommand(
            clap::Command::new("migrate").arg(
                clap::Arg::new("ACTION")
                    .default_value("up")
                    .possible_values(&["up", "down", "setup"]),
            ),
        )
        .subcommand(clap::Command::new("worker"))
        .get_matches();

    let config = Config::load(matches.value_of_os("config")).expect("Failed to load config");

    if let Some(matches) = matches.subcommand_matches("migrate") {
        crate::migrate::run(config, matches);
        Ok(())
    } else if matches.subcommand_matches("worker").is_some() {
        run(config, RunType::Worker)
    } else {
        run(config, RunType::Main)
    }
}

enum RunType {
    Worker,
    Main,
}

#[tokio::main]
async fn run(config: Config, run_type: RunType) -> Result<(), Box<dyn std::error::Error>> {
    if config.debug_stuck {
        log::debug!("Starting stuck detector");

        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);

        tokio::spawn(async move {
            while rx.recv().await.is_some() {
                // do nothing, we just need to consume the value
            }
        });

        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
                match tx.try_send(()) {
                    Ok(()) => {
                        // ok
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        log::warn!("Loop appears to be stuck");
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        log::warn!("Stuck detector disappeared");
                        break;
                    }
                }
            }
        });
    }

    let pg_tls_connector = postgres_native_tls::MakeTlsConnector::new({
        let mut builder = native_tls::TlsConnector::builder();

        if let Some(path) = config.database_certificate_path {
            builder.add_root_certificate(native_tls::Certificate::from_pem(&std::fs::read(path)?)?);
        }

        builder.build()?
    });

    let pg_config: tokio_postgres::config::Config = config.database_url.parse().unwrap();

    let db_pool = deadpool_postgres::Pool::new(
        deadpool_postgres::Manager::new(pg_config.clone(), pg_tls_connector.clone()),
        16,
    );

    // ensure latest migrations have been applied
    {
        let tag = migrate::MIGRATIONS.last().unwrap().tag;
        let db = db_pool.get().await?;
        let row = db
            .query_opt("SELECT 1 FROM __migrant_migrations WHERE tag=$1", &[&tag])
            .await?;
        if row.is_none() {
            panic!("Unapplied migrations detected, run `lotide migrate`");
        }
    }

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

    let (run_worker, run_server) = match run_type {
        RunType::Worker => {
            if !config.separate_worker {
                panic!("Cannot run worker without SEPARATE_WORKER");
            }

            (true, false)
        }
        RunType::Main => (!config.separate_worker, true),
    };

    let (worker_trigger, worker_rx) = tokio::sync::mpsc::channel(1);

    let routes = Arc::new(routes::route_root());
    let context = Arc::new(BaseContext {
        local_hostname: get_url_host(&host_url_apub)
            .expect("Couldn't find host in HOST_URL_ACTIVITYPUB"),

        break_stuff: config.break_stuff,
        dev_mode: config.dev_mode,
        db_pool,
        mailer,
        mail_from,
        media_storage: match config.media_storage.as_deref() {
            None => match config.media_location {
                None => None,
                Some(path) => Some(MediaStorage::Local(path.into())),
            },
            Some("local") => Some(MediaStorage::Local(
                config
                    .media_location
                    .expect("Missing media_location")
                    .into(),
            )),
            Some("s3") => {
                let region = match config.media_s3_endpoint {
                    None => match config.media_s3_region {
                        None => Default::default(),
                        Some(src) => src.parse().expect("Unknown AWS region"),
                    },
                    Some(endpoint) => rusoto_core::Region::Custom {
                        name: match config.media_s3_region {
                            None => "us-east-1".to_string(),
                            Some(name) => name,
                        },
                        endpoint,
                    },
                };

                let credentials: Box<dyn rusoto_credential::ProvideAwsCredentials + Send + Sync> =
                    match config.media_s3_access_key_id {
                        None => {
                            Box::new(rusoto_credential::DefaultCredentialsProvider::new().unwrap())
                        }
                        Some(key_id) => Box::new(rusoto_credential::StaticProvider::new(
                            key_id,
                            config
                                .media_s3_secret_key
                                .expect("Missing secret key for media S3"),
                            None,
                            None,
                        )),
                    };

                Some(MediaStorage::S3 {
                    client: rusoto_s3::S3Client::new_with(
                        rusoto_core::request::HttpClient::new().unwrap(),
                        credentials,
                        region,
                    ),
                    bucket: config.media_location.expect("Missing media_location"),
                })
            }
            Some(_) => {
                panic!("Unknown media_storage type");
            }
        },
        host_url_api: config.host_url_api.clone(),
        host_url_apub,
        http_client: hyper::Client::builder().build(hyper_tls::HttpsConnector::new()),
        user_agent: format!("lotide/{}", env!("CARGO_PKG_VERSION")),
        apub_proxy_rewrites: config.apub_proxy_rewrites,
        api_ratelimit: henry::RatelimitBucket::new(300),
        vapid_public_key_base64,
        vapid_signature_builder,

        worker_trigger: if run_worker {
            Some(worker_trigger.clone())
        } else {
            None
        },
    });

    tokio::join!(
        {
            let context = context.clone();
            async {
                if run_worker {
                    tokio::spawn(worker::run_worker(context, worker_rx))
                        .await
                        .unwrap()
                        .unwrap();
                }
            }
        },
        {
            let port = config.port;
            async move {
                if run_server {
                    let server = hyper::Server::bind(
                        &(std::net::Ipv6Addr::UNSPECIFIED, port).into(),
                    )
                    .serve(hyper::service::make_service_fn(
                        |sock: &hyper::server::conn::AddrStream| {
                            let addr_direct = sock.remote_addr().ip();
                            let routes = routes.clone();
                            let context = context.clone();
                            async move {
                                Ok::<_, hyper::Error>(hyper::service::service_fn(move |req| {
                                    let routes = routes.clone();
                                    let context = context.clone();
                                    async move {
                                        let ratelimit_addr = if allow_forwarded {
                                            if let Some(value) = req.headers().get(
                                                hyper::header::HeaderName::from_static(
                                                    "x-forwarded-for",
                                                ),
                                            ) {
                                                match value
                                                    .to_str()
                                                    .map_err(|_| ())
                                                    .and_then(|value| {
                                                        value.split(", ").next().ok_or(())
                                                    })
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
                                            Some(addr) => context.api_ratelimit.try_call(addr),
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
                                                .header(
                                                    hyper::header::ACCESS_CONTROL_ALLOW_ORIGIN,
                                                    "*",
                                                )
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
                                                    trout::RoutingFailure::NotFound => {
                                                        hyper::StatusCode::NOT_FOUND
                                                    }
                                                    trout::RoutingFailure::MethodNotAllowed => {
                                                        hyper::StatusCode::METHOD_NOT_ALLOWED
                                                    }
                                                };

                                                simple_response(
                                                    code,
                                                    code.canonical_reason().unwrap(),
                                                )
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
                        },
                    ));

                    server.await.unwrap();
                } else {
                    async move {
                        let (listen_client, mut listen_conn) =
                            pg_config.connect(pg_tls_connector).await?;

                        let handle = tokio::spawn({
                            let stream = futures::stream::poll_fn(move |cx| {
                                listen_conn.poll_message(cx).map_err(crate::Error::from)
                            });
                            stream.try_fold(worker_trigger, |worker_trigger, msg| async move {
                                if let tokio_postgres::AsyncMessage::Notification(_) = msg {
                                    match worker_trigger.clone().try_send(()) {
                                        Ok(_)
                                        | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                            panic!("Worker channel closed");
                                        }
                                    }
                                }

                                Ok(worker_trigger)
                            })
                        });

                        listen_client.execute("LISTEN new_task", &[]).await?;
                        handle.await??;

                        Ok::<(), crate::Error>(())
                    }
                    .await
                    .unwrap();
                }
            }
        }
    );

    Ok(())
}
