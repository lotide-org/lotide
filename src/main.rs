use std::sync::Arc;
use trout::hyper::RoutingFailureExtHyper;

mod routes;

type DbPool = deadpool_postgres::Pool;

pub struct RouteContext {
    db_pool: DbPool,
    host_url_apub: String,
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
                                let mut res = hyper::Response::new("Internal Server Error".into());
                                *res.status_mut() = hyper::StatusCode::INTERNAL_SERVER_ERROR;
                                res
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
