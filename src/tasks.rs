use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[async_trait]
pub trait TaskDef: Serialize {
    fn kind() -> &'static str;
    async fn perform(self, ctx: &crate::BaseContext) -> Result<(), crate::Error>;
}

#[derive(Deserialize, Serialize)]
pub struct DeliverToInbox<'a> {
    pub inbox: Cow<'a, str>,
    pub sign_as: Option<crate::ActorLocalRef>,
    pub object: activitystreams::object::ObjectBox,
}

#[async_trait]
impl<'a> TaskDef for DeliverToInbox<'a> {
    fn kind() -> &'static str {
        "deliver_to_inbox"
    }

    async fn perform(self, ctx: &crate::BaseContext) -> Result<(), crate::Error> {
        Err(crate::Error::InternalStrStatic("unimplemented"))
    }
}
