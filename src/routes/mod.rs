mod apub;

pub fn route_root() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_child("apub", apub::route_apub())
}
