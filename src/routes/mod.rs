mod api;
mod apub;
mod well_known;

pub fn route_root() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child("apub", apub::route_apub())
        .with_child("api", api::route_api())
        // temporary Lemmy compat. To be removed.
        .with_child("inbox", apub::route_inbox())
        .with_child(".well-known", well_known::route_well_known())
}
