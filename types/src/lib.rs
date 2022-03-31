use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;

macro_rules! id_wrapper {
    ($ty:ident) => {
        #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
        #[serde(transparent)]
        pub struct $ty(pub i64);
        impl $ty {
            pub fn raw(&self) -> i64 {
                self.0
            }
        }
        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        impl std::str::FromStr for $ty {
            type Err = std::num::ParseIntError;
            fn from_str(src: &str) -> Result<Self, Self::Err> {
                Ok(Self(src.parse()?))
            }
        }
        impl postgres_types::ToSql for $ty {
            fn to_sql(
                &self,
                ty: &postgres_types::Type,
                out: &mut bytes::BytesMut,
            ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                self.0.to_sql(ty, out)
            }
            fn accepts(ty: &postgres_types::Type) -> bool {
                i64::accepts(ty)
            }
            fn to_sql_checked(
                &self,
                ty: &postgres_types::Type,
                out: &mut bytes::BytesMut,
            ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                self.0.to_sql_checked(ty, out)
            }
        }
    };
}

id_wrapper!(CommentLocalID);
id_wrapper!(CommunityLocalID);
id_wrapper!(PollLocalID);
id_wrapper!(PollOptionLocalID);
id_wrapper!(PostLocalID);
id_wrapper!(UserLocalID);
id_wrapper!(NotificationID);
id_wrapper!(NotificationSubscriptionID);
id_wrapper!(FlagLocalID);

#[derive(Serialize, Default, Clone, Copy)]
pub struct Empty {}

#[derive(Deserialize, Serialize, Debug)]
pub struct FingerRequestQuery<'a> {
    pub resource: Cow<'a, str>,
    pub rel: Option<Cow<'a, str>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FingerLink<'a> {
    pub rel: Cow<'a, str>,
    #[serde(rename = "type")]
    pub type_: Option<Cow<'a, str>>,
    pub href: Option<Cow<'a, str>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FingerResponse<'a> {
    pub subject: Cow<'a, str>,
    #[serde(default)]
    pub aliases: Vec<Cow<'a, str>>,
    #[serde(default)]
    pub links: Vec<FingerLink<'a>>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct JustContentText<'a> {
    pub content_text: Cow<'a, str>,
}

#[derive(Serialize, Clone)]
pub struct Content<'a> {
    pub content_text: Option<Cow<'a, str>>,
    pub content_markdown: Option<Cow<'a, str>>,
    #[serde(rename = "content_html")]
    pub content_html_safe: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct RespUserInfo<'a> {
    #[serde(flatten)]
    pub base: RespMinimalAuthorInfo<'a>,

    pub description: Content<'a>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspended: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub your_note: Option<Option<JustContentText<'a>>>,
}

#[derive(Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum RespNotificationInfo<'a> {
    PostReply {
        reply: RespPostCommentInfo<'a>,
        post: RespPostListPost<'a>,
    },
    CommentReply {
        reply: RespPostCommentInfo<'a>,
        comment: RespPostCommentInfo<'a>,
        post: RespPostListPost<'a>,
    },
}

#[derive(Serialize, Clone)]
pub struct RespNotification<'a> {
    #[serde(flatten)]
    pub info: RespNotificationInfo<'a>,

    pub unseen: bool,
}

#[derive(Serialize)]
pub struct JustID<T: serde::Serialize> {
    pub id: T,
}

#[derive(Deserialize)]
pub struct MaybeIncludeYour {
    #[serde(default)]
    pub include_your: bool,
}

#[derive(Serialize, Clone)]
pub struct RespList<'a, T: serde::Serialize + ToOwned + Clone> {
    pub items: Cow<'a, [T]>,
    pub next_page: Option<Cow<'a, str>>,
}

impl<'a, T: serde::Serialize + ToOwned + Clone> RespList<'a, T> {
    pub fn empty() -> Self {
        Self {
            items: Cow::Borrowed(&[]),
            next_page: None,
        }
    }
}

#[derive(Serialize, Clone)]
pub struct RespAvatarInfo<'a> {
    pub url: Cow<'a, str>,
}

#[derive(Serialize, Clone)]
pub struct RespMinimalAuthorInfo<'a> {
    pub id: UserLocalID,
    pub username: Cow<'a, str>,
    pub local: bool,
    pub host: Cow<'a, str>,
    pub remote_url: Option<Cow<'a, str>>,
    pub is_bot: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<RespAvatarInfo<'a>>,
}

#[derive(Serialize)]
pub struct RespLoginUserInfo<'a> {
    pub id: UserLocalID,
    pub username: &'a str,
    pub is_site_admin: bool,
    pub has_unread_notifications: bool,
}

#[derive(Serialize, Clone)]
pub struct JustUser<'a> {
    pub user: RespMinimalAuthorInfo<'a>,
}

#[derive(Serialize, Clone)]
pub struct RespMinimalCommunityInfo<'a> {
    pub id: CommunityLocalID,
    pub name: Cow<'a, str>,
    pub local: bool,
    pub host: Cow<'a, str>,
    pub remote_url: Option<Cow<'a, str>>,
    pub deleted: bool,
}

#[derive(Serialize, Clone)]
pub struct RespMinimalPostInfo<'a> {
    pub id: PostLocalID,
    pub title: &'a str,
    pub remote_url: Option<Cow<'a, str>>,
    pub sensitive: bool,
}

#[derive(Serialize, Clone)]
pub struct RespPostListPost<'a> {
    pub id: PostLocalID,
    pub title: Cow<'a, str>,
    pub remote_url: Option<Cow<'a, str>>,
    pub href: Option<Cow<'a, str>>,
    pub content_text: Option<Cow<'a, str>>,
    pub content_markdown: Option<Cow<'a, str>>,
    #[serde(rename = "content_html")]
    pub content_html_safe: Option<String>,
    pub author: Option<Cow<'a, RespMinimalAuthorInfo<'a>>>,
    pub created: Cow<'a, str>,
    pub community: Cow<'a, RespMinimalCommunityInfo<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replies_count_total: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevance: Option<f32>,
    pub score: i64,
    pub sticky: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub your_vote: Option<Option<Empty>>,
    pub sensitive: bool,
}

#[derive(Serialize, Clone)]
pub struct RespMinimalCommentInfo<'a> {
    pub id: CommentLocalID,
    pub remote_url: Option<Cow<'a, str>>,
    pub sensitive: bool,
    pub content_text: Option<Cow<'a, str>>,
    #[serde(rename = "content_html")]
    pub content_html_safe: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct JustURL<'a> {
    pub url: Cow<'a, str>,
}

#[derive(Serialize, Clone)]
pub struct RespPostCommentInfo<'a> {
    #[serde(flatten)]
    pub base: RespMinimalCommentInfo<'a>,

    pub attachments: Vec<JustURL<'a>>,
    pub author: Option<RespMinimalAuthorInfo<'a>>,
    pub content_markdown: Option<Cow<'a, str>>,
    pub created: String,
    pub deleted: bool,
    pub local: bool,
    pub replies: Option<RespList<'a, RespPostCommentInfo<'a>>>,
    pub score: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub your_vote: Option<Option<Empty>>,
}

impl<'a> RespPostCommentInfo<'a> {
    pub fn has_replies(&self) -> Option<bool> {
        self.replies.as_ref().map(|list| !list.items.is_empty())
    }
}

#[derive(Serialize, Clone)]
#[serde(tag = "type")]
pub enum RespThingInfo<'a> {
    #[serde(rename = "post")]
    Post(RespPostListPost<'a>),
    #[serde(rename = "comment")]
    Comment {
        #[serde(flatten)]
        base: RespMinimalCommentInfo<'a>,
        created: String,
        post: RespMinimalPostInfo<'a>,
    },
}

#[derive(Serialize)]
pub struct RespPostInfo<'a> {
    #[serde(flatten)]
    pub post: &'a RespPostListPost<'a>,
    pub approved: bool,
    pub rejected: bool,
    pub local: bool,
    pub poll: Option<RespPollInfo<'a>>,
}

#[derive(Serialize)]
pub struct RespPollInfo<'a> {
    pub multiple: bool,
    pub options: Vec<RespPollOption<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub your_vote: Option<Option<RespPollYourVote>>,
    pub closed_at: Option<String>,
    pub is_closed: bool,
}

#[derive(Serialize)]
pub struct RespPollOption<'a> {
    pub id: PollOptionLocalID,
    pub name: &'a str,
    pub votes: u32,
}

#[derive(Serialize, Clone)]
pub struct RespCommunityFeedsType {
    pub new: String,
}

#[derive(Serialize, Clone)]
pub struct RespCommunityFeeds {
    pub atom: RespCommunityFeedsType,
}

#[derive(Serialize, Clone)]
pub struct RespCommunityInfo<'a> {
    #[serde(flatten)]
    pub base: RespMinimalCommunityInfo<'a>,

    pub description: Content<'a>,
    pub feeds: RespCommunityFeeds,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub you_are_moderator: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub your_follow: Option<Option<RespYourFollowInfo>>,
}

#[derive(Serialize, Clone)]
pub struct RespYourFollowInfo {
    pub accepted: bool,
}

#[derive(Serialize)]
pub struct RespModeratorInfo<'a> {
    #[serde(flatten)]
    pub base: RespMinimalAuthorInfo<'a>,
    pub moderator_since: Option<String>,
}

#[derive(Serialize)]
pub struct RespCommentInfo<'a> {
    #[serde(flatten)]
    pub base: RespPostCommentInfo<'a>,
    pub parent: Option<JustID<CommentLocalID>>,
    pub post: Option<RespMinimalPostInfo<'a>>,
}

#[derive(Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum RespFlagDetails<'a> {
    Post { post: RespPostListPost<'a> },
}

#[derive(Serialize, Clone)]
pub struct RespFlagInfo<'a> {
    pub id: FlagLocalID,

    pub flagger: RespMinimalAuthorInfo<'a>,

    pub created_local: String,

    pub content: Option<JustContentText<'a>>,

    #[serde(flatten)]
    pub details: RespFlagDetails<'a>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorLocalRef {
    Person(UserLocalID),
    Community(CommunityLocalID),
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "type", content = "id")]
#[serde(rename_all = "snake_case")]
pub enum ThingLocalRef {
    Post(PostLocalID),
    Comment(CommentLocalID),
    User(UserLocalID),
    Community(CommunityLocalID),
}

#[derive(Deserialize)]
pub struct NotificationSubscriptionCreateQuery<'a> {
    #[serde(rename = "type")]
    pub type_: Cow<'a, str>,
    pub endpoint: Cow<'a, str>,
    pub p256dh_key: Cow<'a, str>,
    pub auth_key: Cow<'a, str>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum PollVoteBody {
    Multiple { options: Vec<PollOptionLocalID> },
    Single { option: PollOptionLocalID },
}

#[derive(Serialize)]
pub struct RespPollYourVote {
    pub options: Vec<JustID<PollOptionLocalID>>,
}

#[derive(Serialize, Clone)]
pub struct RespCommunityModlogEvent<'a> {
    pub time: String,
    #[serde(flatten)]
    pub details: RespCommunityModlogEventDetails<'a>,
}

#[derive(Serialize, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum RespCommunityModlogEventDetails<'a> {
    RejectPost { post: RespMinimalPostInfo<'a> },
    ApprovePost { post: RespMinimalPostInfo<'a> },
}
