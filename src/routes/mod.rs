use crate::lang;

mod api;
mod apub;
mod well_known;

pub fn route_root() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_handler_async(hyper::Method::GET, |_, _, req| {
            let lang = crate::get_lang_for_req(&req);
            futures::future::err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::METHOD_NOT_ALLOWED,
                lang.tr(&lang::root()).into_owned(),
            )))
        })
        .with_child("apub", apub::route_apub())
        .with_child("api", api::route_api())
        .with_child(".well-known", well_known::route_well_known())
}
