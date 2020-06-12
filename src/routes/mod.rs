mod api;
mod apub;

pub fn route_root() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child("apub", apub::route_apub())
        .with_child("api", api::route_api())
        // temporary Lemmy compat. To be removed.
        .with_child("inbox", apub::route_inbox())
}
