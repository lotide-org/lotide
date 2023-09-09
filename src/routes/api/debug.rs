use std::sync::Arc;

async fn route_unstable_debug_db(
    _: (),
    ctx: Arc<crate::RouteContext>,
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let status = ctx.db_pool.status();

    let (idle, waiting) = if status.available < 0 {
        (0, -status.available)
    } else {
        (status.available, 0)
    };

    crate::json_response(&serde_json::json!({
        "pool": {
            "max": status.max_size,
            "size": status.size,
            "idle": idle,
            "waiting": waiting,
        },
    }))
}

pub fn route_debug() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_child(
        "db",
        crate::RouteNode::new().with_handler_async(hyper::Method::GET, route_unstable_debug_db),
    )
}
