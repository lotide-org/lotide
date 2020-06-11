use std::borrow::Cow;
use std::sync::Arc;
use trout::hyper::RoutingFailureExtHyper;

mod apub_util;
mod routes;

pub type DbPool = deadpool_postgres::Pool;
pub type HttpClient = hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>;

pub struct RouteContext {
    db_pool: DbPool,
    host_url_apub: String,
    http_client: HttpClient,
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
    UserError(hyper::Response<hyper::Body>),
    RoutingError(trout::RoutingFailure),
}

impl<T: 'static + std::error::Error + Send> From<T> for Error {
    fn from(err: T) -> Error {
        Error::Internal(Box::new(err))
    }
}

pub struct PostInfo<'a> {
    id: i64,
    author: Option<i64>,
    href: Option<&'a str>,
    content_text: Option<&'a str>,
    title: &'a str,
    created: &'a chrono::DateTime<chrono::FixedOffset>,
    community: i64,
}

pub struct CommentInfo {
    id: i64,
    author: Option<i64>,
    post: i64,
    parent: Option<i64>,
    content_text: String,
    created: chrono::DateTime<chrono::FixedOffset>,
}

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

pub async fn query_stream(
    db: &tokio_postgres::Client,
    statement: &(impl tokio_postgres::ToStatement + ?Sized),
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<tokio_postgres::RowStream, tokio_postgres::Error> {
    let params = params.iter().map(|s| *s as _);

    db.query_raw(statement, params).await
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

pub fn on_community_add_post<'a>(post: &'a PostInfo<'a>, ctx: Arc<crate::RouteContext>) {
    println!("on_community_add_post");
    crate::apub_util::spawn_announce_community_post(post, ctx);
}

pub fn on_community_add_comment(
    comment: CommentInfo,
    post_ap_id: String,
    parent_ap_id: Option<String>,
    community: i64,
    ctx: Arc<crate::RouteContext>,
) {
    crate::spawn_task(crate::apub_util::announce_community_comment(
        comment,
        post_ap_id,
        parent_ap_id,
        community,
        ctx,
    ));
}

pub fn on_post_add_comment(comment: CommentInfo, ctx: Arc<crate::RouteContext>) {
    println!("on_post_add_comment");
    spawn_task(async move {
        let db = ctx.db_pool.get().await?;

        let (post_community_row, parent_ap_id) = futures::future::try_join(
            db.query_opt(
                "SELECT community.id, community.local, community.ap_id, community.ap_inbox, post.local, post.ap_id FROM post, community WHERE post.id = $1 AND post.community = community.id",
                &[&comment.post],
            ),
            async {
                match comment.parent {
                    Some(parent) => {
                        let row = db.query_one(
                            "SELECT local, ap_id FROM reply WHERE id=$1",
                            &[&parent],
                        ).await?;

                        if row.get(0) {
                            Ok(Some(crate::apub_util::get_local_comment_apub_id(parent, &ctx.host_url_apub)))
                        } else {
                            Ok(row.get(1))
                        }
                    },
                    None => Ok(None),
                }
            }
        ).await?;

        if let Some(row) = post_community_row {
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

            if let Some(post_ap_id) = post_ap_id {
                if community_local {
                    let community = row.get(0);
                    crate::on_community_add_comment(
                        comment,
                        post_ap_id,
                        parent_ap_id,
                        community,
                        ctx,
                    );
                } else {
                    crate::apub_util::send_comment_to_community(
                        comment,
                        row.get(2),
                        row.get(3),
                        post_ap_id,
                        parent_ap_id,
                        ctx,
                    )
                    .await?;
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
    let context = Arc::new(RouteContext {
        db_pool,
        host_url_apub,
        http_client: hyper::Client::builder().build(hyper_tls::HttpsConnector::new()),
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
                        })
                    }
                }))
            }
        }),
    );

    server.await?;

    Ok(())
}
