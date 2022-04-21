use super::try_strip_host;
use crate::types::{
    CommentLocalID, CommunityLocalID, PollLocalID, PollOptionLocalID, PostLocalID, UserLocalID,
};
use crate::BaseURL;

type RefRouteNode<P> = trout::Node<P, String, LocalObjectRef, ()>;

lazy_static::lazy_static! {
    static ref LOCAL_REF_ROUTES: RefRouteNode<()> = {
        RefRouteNode::new()
            .with_child(
                "comments",
                RefRouteNode::new()
                    .with_child_parse::<CommentLocalID, _>(
                        RefRouteNode::new().with_handler((), |(comment,), _, _| LocalObjectRef::Comment(comment))
                            .with_child("likes", RefRouteNode::new().with_child_parse::<UserLocalID, _>(RefRouteNode::new().with_handler((), |(comment, user), _, _| LocalObjectRef::CommentLike(comment, user))))
                    )
            )
            .with_child(
                "communities",
                RefRouteNode::new()
                    .with_child_parse::<CommunityLocalID, _>(
                        RefRouteNode::new()
                            .with_handler((), |(community,), _, _| LocalObjectRef::Community(community))
                            .with_child(
                                "featured",
                                RefRouteNode::new()
                                    .with_handler((), |(community,), _, _| LocalObjectRef::CommunityFeatured(community))
                            )
                            .with_child(
                                "followers",
                                RefRouteNode::new()
                                    .with_handler((), |(community,), _, _| LocalObjectRef::CommunityFollowers(community))
                                    .with_child_parse::<UserLocalID, _>(
                                        RefRouteNode::new()
                                            .with_handler((), |(community, follower), _, _| LocalObjectRef::CommunityFollow(community, follower))
                                            .with_child(
                                                "join",
                                                RefRouteNode::new()
                                                    .with_handler((), |(community, follower), _, _| LocalObjectRef::CommunityFollowJoin(community, follower))
                                            )
                                    )
                            )
                            .with_child(
                                "outbox",
                                RefRouteNode::new()
                                    .with_handler((), |(community,), _, _| LocalObjectRef::CommunityOutbox(community))
                                    .with_child("page", RefRouteNode::new().with_child_parse::<crate::TimestampOrLatest, _>(RefRouteNode::new().with_handler((), |(community, page), _, _| LocalObjectRef::CommunityOutboxPage(community, page))))
                            )
                    )
            )
            .with_child("inbox", RefRouteNode::new().with_handler((), |_, _, _| LocalObjectRef::SharedInbox))
            .with_child("polls", RefRouteNode::new().with_child_parse::<PollLocalID, _>(
                    RefRouteNode::new().with_child(
                        "voters",
                        RefRouteNode::new().with_child_parse::<UserLocalID, _>(
                            RefRouteNode::new().with_child(
                                "votes",
                                RefRouteNode::new().with_child_parse::<PollOptionLocalID, _>(
                                    RefRouteNode::new().with_handler((), |(poll, user, option), _, _| LocalObjectRef::PollVote(poll, user, option))))))))
            .with_child(
                "posts",
                RefRouteNode::new()
                    .with_child_parse::<PostLocalID, _>(
                        RefRouteNode::new()
                            .with_handler((), |(post,), _, _| LocalObjectRef::Post(post))
                            .with_child("likes", RefRouteNode::new().with_child_parse::<UserLocalID, _>(RefRouteNode::new().with_handler((), |(post, user), _, _| LocalObjectRef::PostLike(post, user))))
                    )
            )
            .with_child(
                "users",
                RefRouteNode::new()
                    .with_child_parse::<UserLocalID, _>(
                        RefRouteNode::new()
                            .with_handler((), |(user,), _, _| LocalObjectRef::User(user))
                            .with_child("outbox", RefRouteNode::new().with_handler((), |(user,), _, _| LocalObjectRef::UserOutbox(user)).with_child("page", RefRouteNode::new().with_child_parse::<crate::TimestampOrLatest, _>(RefRouteNode::new().with_handler((), |(user, page), _, _| LocalObjectRef::UserOutboxPage(user, page)))))
                    )
            )
    };
}

#[derive(Debug, Clone, Copy)]
pub enum LocalObjectRef {
    Comment(CommentLocalID),
    CommentLike(CommentLocalID, UserLocalID),
    Community(CommunityLocalID),
    CommunityFeatured(CommunityLocalID),
    CommunityFollowers(CommunityLocalID),
    CommunityFollow(CommunityLocalID, UserLocalID),
    CommunityFollowJoin(CommunityLocalID, UserLocalID),
    CommunityOutbox(CommunityLocalID),
    CommunityOutboxPage(CommunityLocalID, crate::TimestampOrLatest),
    PollVote(PollLocalID, UserLocalID, PollOptionLocalID),
    Post(PostLocalID),
    PostLike(PostLocalID, UserLocalID),
    SharedInbox,
    User(UserLocalID),
    UserOutbox(UserLocalID),
    UserOutboxPage(UserLocalID, crate::TimestampOrLatest),
}

impl LocalObjectRef {
    pub fn try_from_path(path: &str) -> Option<LocalObjectRef> {
        if !path.starts_with('/') {
            return None;
        }

        let path = path[1..].to_owned();
        log::debug!("checking local object {}", path);
        let res = LOCAL_REF_ROUTES.route(path, ());
        log::debug!("found {:?}", res);
        res.ok()
    }

    pub fn try_from_uri(uri: &url::Url, host_url_apub: &BaseURL) -> Option<LocalObjectRef> {
        if let Some(remaining) = try_strip_host(uri, host_url_apub) {
            LocalObjectRef::try_from_path(remaining)
        } else {
            None
        }
    }

    pub fn to_local_uri(self, host_url_apub: &BaseURL) -> BaseURL {
        match self {
            LocalObjectRef::Comment(comment) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut()
                    .extend(&["comments", &comment.to_string()]);
                res
            }
            LocalObjectRef::CommentLike(comment, user) => {
                let mut res = LocalObjectRef::Comment(comment).to_local_uri(host_url_apub);
                res.path_segments_mut()
                    .extend(&["likes", &user.to_string()]);
                res
            }
            LocalObjectRef::Community(community) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut()
                    .extend(&["communities", &community.to_string()]);
                res
            }
            LocalObjectRef::CommunityFeatured(community) => {
                let mut res = LocalObjectRef::Community(community).to_local_uri(host_url_apub);
                res.path_segments_mut().push("featured");
                res
            }
            LocalObjectRef::CommunityFollowers(community) => {
                let mut res = LocalObjectRef::Community(community).to_local_uri(host_url_apub);
                res.path_segments_mut().push("followers");
                res
            }
            LocalObjectRef::CommunityFollow(community, follower) => {
                let mut res =
                    LocalObjectRef::CommunityFollowers(community).to_local_uri(host_url_apub);
                res.path_segments_mut().push(&follower.to_string());
                res
            }
            LocalObjectRef::CommunityFollowJoin(community, follower) => {
                let mut res = LocalObjectRef::CommunityFollow(community, follower)
                    .to_local_uri(host_url_apub);
                res.path_segments_mut().push("join");
                res
            }
            LocalObjectRef::CommunityOutbox(community) => {
                let mut res = LocalObjectRef::Community(community).to_local_uri(host_url_apub);
                res.path_segments_mut().push("outbox");
                res
            }
            LocalObjectRef::CommunityOutboxPage(community, page) => {
                let mut res =
                    LocalObjectRef::CommunityOutbox(community).to_local_uri(host_url_apub);
                res.path_segments_mut().extend(&["page", &page.to_string()]);
                res
            }
            LocalObjectRef::PollVote(poll, user, option) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut().extend(&[
                    "polls",
                    &poll.to_string(),
                    "voters",
                    &user.to_string(),
                    "votes",
                    &option.to_string(),
                ]);
                res
            }
            LocalObjectRef::Post(post) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut()
                    .extend(&["posts", &post.to_string()]);
                res
            }
            LocalObjectRef::PostLike(post, user) => {
                let mut res =
                    crate::apub_util::LocalObjectRef::Post(post).to_local_uri(host_url_apub);
                res.path_segments_mut()
                    .extend(&["likes", &user.to_string()]);
                res
            }
            LocalObjectRef::SharedInbox => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut().push("inbox");
                res
            }
            LocalObjectRef::User(user) => {
                let mut res = host_url_apub.clone();
                res.path_segments_mut()
                    .extend(&["users", &user.to_string()]);
                res
            }
            LocalObjectRef::UserOutbox(user) => {
                let mut res = LocalObjectRef::User(user).to_local_uri(host_url_apub);
                res.path_segments_mut().push("outbox");
                res
            }
            LocalObjectRef::UserOutboxPage(user, page) => {
                let mut res = LocalObjectRef::UserOutbox(user).to_local_uri(host_url_apub);
                res.path_segments_mut().extend(&["page", &page.to_string()]);
                res
            }
        }
    }
}
