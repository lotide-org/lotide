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

pub fn simple_response(code: hyper::StatusCode, text: impl Into<hyper::Body>) -> hyper::Response<hyper::Body> {
    let mut res = hyper::Response::new(text.into());
    *res.status_mut() = code;
    res
}

pub async fn res_to_error(res: hyper::Response<hyper::Body>) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    if res.status().is_success() {
        Ok(res)
    } else {
        let bytes = hyper::body::to_bytes(res.into_body()).await?;
        Err(crate::Error::InternalStr(format!("Error in remote response: {}", String::from_utf8_lossy(&bytes))))
    }
}

pub async fn authenticate(req: &hyper::Request<hyper::Body>, db: &tokio_postgres::Client) -> Result<Option<i64>, Error> {
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

    let row = db.query_opt("SELECT person FROM login WHERE token=$1", &[&token]).await?;

    match row {
        Some(row) => Ok(Some(row.get(0))),
        None => Ok(None),
    }
}

pub async fn require_login(req: &hyper::Request<hyper::Body>, db: &tokio_postgres::Client) -> Result<i64, Error> {
    authenticate(req, db).await?
        .ok_or_else(|| Error::UserError(simple_response(hyper::StatusCode::UNAUTHORIZED, "Login Required")))
}

pub fn spawn_task<F: std::future::Future<Output = Result<(), Error>> + Send + 'static>(task: F) {
    use futures::future::TryFutureExt;
    tokio::spawn(
        task
            .map_err(|err| {
                eprintln!("Error in task: {:?}", err);
            })
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_url_apub = std::env::var("HOST_URL_ACTIVITYPUB").expect("Missing HOST_URL_ACTIVITYPUB");

    let db_pool = deadpool_postgres::Pool::new(
        deadpool_postgres::Manager::new(
            std::env::var("DATABASE_URL").expect("Missing DATABASE_URL").parse().unwrap(),
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

                                simple_response(hyper::StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
                            },
                            Err(Error::InternalStr(err)) => {
                                eprintln!("Error: {}", err);

                                simple_response(hyper::StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
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
