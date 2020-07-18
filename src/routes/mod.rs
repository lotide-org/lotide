mod api;
mod apub;
mod well_known;

pub fn route_root() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_handler_async("GET", |_, _, _| {
            futures::future::err(crate::Error::UserError(
                crate::simple_response(
                    hyper::StatusCode::METHOD_NOT_ALLOWED,
                    "lotide is running. Note that lotide itself does not include a frontend, and you'll need to install one separately."
                )
            ))
        })
        .with_child("apub", apub::route_apub())
        .with_child("api", api::route_api())
        // temporary Lemmy compat. To be removed.
        .with_child("inbox", apub::route_inbox())
        .with_child(".well-known", well_known::route_well_known())
}
