use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::sync::Arc;
use trout::hyper::RoutingFailureExtHyper;

mod apub_util;
mod routes;
mod tasks;
mod worker;

pub type DbPool = deadpool_postgres::Pool;
pub type HttpClient = hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>;

pub struct BaseContext {
    pub db_pool: DbPool,
    pub host_url_api: String,
    pub host_url_apub: String,
    pub http_client: HttpClient,
    pub apub_proxy_rewrites: bool,
}

pub struct RouteContext {
    base: Arc<BaseContext>,
    worker_trigger: tokio::sync::mpsc::Sender<()>,
}

impl RouteContext {
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
}

impl std::ops::Deref for RouteContext {
    type Target = BaseContext;

    fn deref(&self) -> &BaseContext {
        &self.base
    }
}

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

#[derive(Debug)]
pub enum APIDOrLocal {
    Local,
    APID(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorLocalRef {
    Person(i64),
    Community(i64),
}

pub enum ThingLocalRef {
    Post(i64),
    Comment(i64),
}

#[derive(Debug)]
pub struct PostInfo<'a> {
    id: i64,
    author: Option<i64>,
    href: Option<&'a str>,
    content_text: Option<&'a str>,
    #[allow(dead_code)]
    content_markdown: Option<&'a str>,
    content_html: Option<&'a str>,
    title: &'a str,
    created: &'a chrono::DateTime<chrono::FixedOffset>,
    #[allow(dead_code)]
    community: i64,
}

pub struct PostInfoOwned {
    id: i64,
    author: Option<i64>,
    href: Option<String>,
    content_text: Option<String>,
    content_markdown: Option<String>,
    content_html: Option<String>,
    title: String,
    created: chrono::DateTime<chrono::FixedOffset>,
    community: i64,
}

impl<'a> Into<PostInfo<'a>> for &'a PostInfoOwned {
    fn into(self) -> PostInfo<'a> {
        PostInfo {
            id: self.id,
            author: self.author,
            href: self.href.as_deref(),
            content_text: self.content_text.as_deref(),
            content_markdown: self.content_markdown.as_deref(),
            content_html: self.content_html.as_deref(),
            title: &self.title,
            created: &self.created,
            community: self.community,
        }
    }
}

#[derive(Debug)]
pub struct CommentInfo {
    id: i64,
    author: Option<i64>,
    post: i64,
    parent: Option<i64>,
    content_text: Option<String>,
    #[allow(dead_code)]
    content_markdown: Option<String>,
    content_html: Option<String>,
    created: chrono::DateTime<chrono::FixedOffset>,
    ap_id: APIDOrLocal,
}

pub const KEY_BITS: u32 = 2048;

pub fn get_url_host(url: &str) -> Option<String> {
    url::Url::parse(url).ok().and_then(|url| {
        url.host_str().map(|host| match url.port() {
            Some(port) => format!("{}:{}", host, port),
            None => host.to_owned(),
        })
    })
}

pub fn get_actor_host<'a>(
    local: bool,
    ap_id: Option<&str>,
    local_hostname: &'a str,
) -> Option<Cow<'a, str>> {
    if local {
        Some(local_hostname.into())
    } else {
        ap_id.and_then(get_url_host).map(Cow::from)
    }
}

pub fn get_actor_host_or_unknown<'a>(
    local: bool,
    ap_id: Option<&str>,
    local_hostname: &'a str,
) -> Cow<'a, str> {
    get_actor_host(local, ap_id, local_hostname).unwrap_or(Cow::Borrowed("[unknown]"))
}

pub fn get_path_and_query(url: &str) -> Result<String, url::ParseError> {
    let url = url::Url::parse(&url)?;
    Ok(format!("{}{}", url.path(), url.query().unwrap_or("")))
}

pub async fn query_stream(
    db: &tokio_postgres::Client,
    statement: &(impl tokio_postgres::ToStatement + ?Sized),
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<tokio_postgres::RowStream, tokio_postgres::Error> {
    let params = params.iter().map(|s| *s as _);

    db.query_raw(statement, params).await
}

pub fn empty_response() -> hyper::Response<hyper::Body> {
    let mut res = hyper::Response::new((&[][..]).into());
    *res.status_mut() = hyper::StatusCode::NO_CONTENT;
    res
}

pub fn simple_response(
    code: hyper::StatusCode,
    text: impl Into<hyper::Body>,
) -> hyper::Response<hyper::Body> {
    let mut res = hyper::Response::new(text.into());
    *res.status_mut() = code;
    res
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

pub async fn authenticate(
    req: &hyper::Request<hyper::Body>,
    db: &tokio_postgres::Client,
) -> Result<Option<i64>, Error> {
    use headers::Header;

    let value = match req.headers().get(hyper::header::AUTHORIZATION) {
        Some(value) => {
            match headers::Authorization::<headers::authorization::Bearer>::decode(
                &mut std::iter::once(value),
            ) {
                Ok(value) => value.0.token().to_owned(),
                Err(_) => return Ok(None),
            }
        }
        None => return Ok(None),
    };

    let token = match value.parse::<uuid::Uuid>() {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };

    let row = db
        .query_opt("SELECT person FROM login WHERE token=$1", &[&token])
        .await?;

    match row {
        Some(row) => Ok(Some(row.get(0))),
        None => Ok(None),
    }
}

pub async fn require_login(
    req: &hyper::Request<hyper::Body>,
    db: &tokio_postgres::Client,
) -> Result<i64, Error> {
    authenticate(req, db).await?.ok_or_else(|| {
        Error::UserError(simple_response(
            hyper::StatusCode::UNAUTHORIZED,
            "Login Required",
        ))
    })
}

pub fn spawn_task<F: std::future::Future<Output = Result<(), Error>> + Send + 'static>(task: F) {
    use futures::future::TryFutureExt;
    tokio::spawn(task.map_err(|err| {
        eprintln!("Error in task: {:?}", err);
    }));
}

pub fn render_markdown(src: &str) -> String {
    let parser = pulldown_cmark::Parser::new(src);
    let mut output = String::new();
    pulldown_cmark::html::push_html(&mut output, parser);

    output
}

pub fn on_community_add_post<'a>(
    community: i64,
    post_local_id: i64,
    post_ap_id: &str,
    ctx: Arc<crate::RouteContext>,
) {
    println!("on_community_add_post");
    crate::apub_util::spawn_announce_community_post(community, post_local_id, post_ap_id, ctx);
}

pub fn on_community_add_comment(
    community: i64,
    comment_local_id: i64,
    comment_ap_id: &str,
    ctx: Arc<crate::RouteContext>,
) {
    crate::apub_util::spawn_announce_community_comment(
        community,
        comment_local_id,
        comment_ap_id,
        ctx,
    );
}

pub fn on_post_add_comment(comment: CommentInfo, ctx: Arc<crate::RouteContext>) {
    println!("on_post_add_comment");
    spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let res = futures::future::try_join(
            db.query_opt(
                "SELECT community.id, community.local, community.ap_id, community.ap_inbox, post.local, post.ap_id, person.id, person.ap_id FROM community, post LEFT OUTER JOIN person ON (person.id = post.author) WHERE post.id = $1 AND post.community = community.id",
                &[&comment.post],
            ),
            async {
                match comment.parent {
                    Some(parent) => {
                        let row = db.query_one(
                            "SELECT reply.local, reply.ap_id, person.id, person.ap_id FROM reply LEFT OUTER JOIN person ON (person.id = reply.author) WHERE reply.id=$1",
                            &[&parent],
                        ).await?;

                        if row.get(0) {
                            Ok(Some((crate::apub_util::get_local_comment_apub_id(parent, &ctx.host_url_apub), Some(crate::apub_util::get_local_person_apub_id(row.get(2), &ctx.host_url_apub)))))
                        } else {
                            Ok(row.get::<_, Option<String>>(1).map(|x| (x, row.get(3))))
                        }
                    },
                    None => Ok(None),
                }
            }
        ).await?;

        if let Some(row) = res.0 {
            let community_local: bool = row.get(1);
            let post_local: bool = row.get(4);

            let post_ap_id = if post_local {
                Some(crate::apub_util::get_local_post_apub_id(
                    comment.post,
                    &ctx.host_url_apub,
                ))
            } else {
                row.get(5)
            };

            let comment_ap_id = match &comment.ap_id {
                crate::APIDOrLocal::APID(apid) => Cow::Borrowed(apid),
                crate::APIDOrLocal::Local => Cow::Owned(
                    crate::apub_util::get_local_comment_apub_id(comment.id, &ctx.host_url_apub),
                ),
            };

            let (parent_ap_id, post_or_parent_author_ap_id) = match comment.parent {
                None => (
                    None,
                    if post_local {
                        Some(crate::apub_util::get_local_person_apub_id(
                            row.get(6),
                            &ctx.host_url_apub,
                        ))
                    } else {
                        row.get(7)
                    },
                ),
                Some(_) => match res.1 {
                    None => (None, None),
                    Some((parent_ap_id, parent_author_ap_id)) => {
                        (Some(parent_ap_id), parent_author_ap_id)
                    }
                },
            };

            if let Some(post_ap_id) = post_ap_id {
                if community_local {
                    let community = row.get(0);
                    crate::on_community_add_comment(community, comment.id, &comment_ap_id, ctx);
                } else {
                    crate::apub_util::spawn_enqueue_send_comment_to_community(
                        comment,
                        row.get(2),
                        row.get(3),
                        post_ap_id,
                        parent_ap_id,
                        post_or_parent_author_ap_id.as_deref(),
                        ctx,
                    );
                }
            }
        }

        Ok(())
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_url_apub =
        std::env::var("HOST_URL_ACTIVITYPUB").expect("Missing HOST_URL_ACTIVITYPUB");

    let host_url_api = std::env::var("HOST_URL_API").expect("Missing HOST_URL_API");

    let apub_proxy_rewrites = match std::env::var("APUB_PROXY_REWRITES") {
        Ok(value) => value.parse().expect("Failed to parse APUB_PROXY_REWRITES"),
        Err(std::env::VarError::NotPresent) => false,
        Err(other) => Err(other).expect("Failed to parse APUB_PROXY_REWRITES"),
    };

    let db_pool = deadpool_postgres::Pool::new(
        deadpool_postgres::Manager::new(
            std::env::var("DATABASE_URL")
                .expect("Missing DATABASE_URL")
                .parse()
                .unwrap(),
            tokio_postgres::NoTls,
        ),
        16,
    );

    let port = match std::env::var("PORT") {
        Ok(port_str) => port_str.parse().expect("Failed to parse port"),
        _ => 3333,
    };

    let routes = Arc::new(routes::route_root());
    let base_context = Arc::new(BaseContext {
        db_pool,
        host_url_api,
        host_url_apub,
        http_client: hyper::Client::builder().build(hyper_tls::HttpsConnector::new()),
        apub_proxy_rewrites,
    });

    let worker_trigger = worker::start_worker(base_context.clone());

    let context = Arc::new(RouteContext {
        base: base_context,
        worker_trigger,
    });

    let server = hyper::Server::bind(&(std::net::Ipv6Addr::UNSPECIFIED, port).into()).serve(
        hyper::service::make_service_fn(|_| {
            let routes = routes.clone();
            let context = context.clone();
            async {
                Ok::<_, hyper::Error>(hyper::service::service_fn(move |req| {
                    let routes = routes.clone();
                    let context = context.clone();
                    async move {
                        let result = match routes.route(req, context) {
                            Ok(fut) => fut.await,
                            Err(err) => Err(Error::RoutingError(err)),
                        };
                        Ok::<_, hyper::Error>(match result {
                            Ok(val) => val,
                            Err(Error::UserError(res)) => res,
                            Err(Error::RoutingError(err)) => err.to_simple_response(),
                            Err(Error::Internal(err)) => {
                                eprintln!("Error: {:?}", err);

                                simple_response(
                                    hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                    "Internal Server Error",
                                )
                            }
                            Err(Error::InternalStr(err)) => {
                                eprintln!("Error: {}", err);

                                simple_response(
                                    hyper::StatusCode::INTERNAL_SERVER_ERROR,
                                    "Internal Server Error",
                                )
                            }
                            Err(Error::InternalStrStatic(err)) => {
                                eprintln!("Error: {}", err);

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
