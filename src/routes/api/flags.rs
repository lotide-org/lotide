use crate::types::{
    CommunityLocalID, FlagLocalID, JustContentText, PostLocalID, RespAvatarInfo, RespFlagDetails,
    RespFlagInfo, RespList, RespMinimalAuthorInfo, RespMinimalCommunityInfo, RespPostListPost,
    UserLocalID,
};
use serde::Deserialize;
use std::borrow::Cow;
use std::sync::Arc;

async fn route_unstable_flags_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use std::fmt::Write;

    #[derive(Deserialize)]
    struct FlagsListQuery {
        to_this_site_admin: Option<bool>,
        to_community: Option<CommunityLocalID>,
    }

    let query: FlagsListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    match query.to_community {
        Some(community_id) => {
            let row = db
                .query_opt(
                    "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                    &[&community_id, &user],
                )
                .await?;
            match row {
                None => {
                    if crate::is_site_admin(&db, user).await? {
                        Ok(())
                    } else {
                        Err(crate::Error::UserError(crate::simple_response(
                            hyper::StatusCode::FORBIDDEN,
                            lang.tr("must_be_moderator", None).into_owned(),
                        )))
                    }
                }
                Some(_) => Ok(()),
            }
        }
        None => {
            if crate::is_site_admin(&db, user).await? {
                Ok(())
            } else {
                Err(crate::Error::UserError(crate::simple_response(
                    hyper::StatusCode::FORBIDDEN,
                    lang.tr("not_admin", None).into_owned(),
                )))
            }
        }
    }?;

    let mut sql = "SELECT flag.kind, flag.id, flag.content_text, flag.created_local, flagger.id, flagger.local, flagger.username, flagger.ap_id, flagger.avatar, flagger.is_bot, post.id, post.href, post.content_text, post.title, post.created, post.content_markdown, post.content_html, post_author.id, post_author.username, post_author.local, post_author.ap_id, post_author.avatar, (SELECT COUNT(*) FROM post_like WHERE post_like.post = post.id), (SELECT COUNT(*) FROM reply WHERE reply.post = post.id), post.sticky, post_author.is_bot, post.ap_id, post.local, post.approved, community.id, community.name, community.local, community.ap_id, community.deleted FROM flag INNER JOIN person AS flagger ON (flagger.id = flag.person) LEFT OUTER JOIN post ON (post.id = flag.post) LEFT OUTER JOIN person AS post_author ON (post_author.id = post.author) LEFT OUTER JOIN community ON (community.id = post.community) WHERE TRUE".to_owned();
    let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![];

    if let Some(to_community) = &query.to_community {
        values.push(to_community);
        write!(
            sql,
            " AND flag.to_community=TRUE AND post.community=${}",
            values.len()
        )
        .unwrap();
    }
    if let Some(value) = query.to_this_site_admin {
        write!(sql, "AND {}((flag.to_site_admin AND flag.local) OR (flag.to_remote_site_admin AND NOT flag.local))", if value { "NOT " } else { "" }).unwrap();
    }

    sql.push_str(" ORDER BY flag.id DESC LIMIT 30");

    let sql: &str = &sql;
    let rows = db.query(sql, &values).await?;

    let items: Vec<_> = rows
        .iter()
        .filter_map(|row| {
            let details = match row.get(0) {
                "post" => {
                    if let Some(post_id) = row.get(10) {
                        let post_id = PostLocalID(post_id);
                        let post_created: chrono::DateTime<chrono::FixedOffset> = row.get(14);
                        let post_ap_id: Option<&str> = row.get(26);
                        let post_local = row.get(27);

                        let author_id = UserLocalID(row.get(17));
                        let author_local = row.get(19);
                        let author_ap_id: Option<&str> = row.get(20);
                        let author_avatar: Option<&str> = row.get(21);

                        let author = RespMinimalAuthorInfo {
                            id: author_id,
                            username: Cow::Borrowed(row.get(18)),
                            local: author_local,
                            host: crate::get_actor_host_or_unknown(
                                author_local,
                                author_ap_id,
                                &ctx.local_hostname,
                            ),
                            remote_url: if author_local {
                                Some(Cow::Owned(String::from(
                                    crate::apub_util::get_local_person_apub_id(
                                        author_id,
                                        &ctx.host_url_apub,
                                    ),
                                )))
                            } else {
                                author_ap_id.map(Cow::Borrowed)
                            },
                            avatar: author_avatar.map(|url| RespAvatarInfo {
                                url: ctx.process_avatar_href(url, author_id),
                            }),
                            is_bot: row.get(25),
                        };

                        let community_id = CommunityLocalID(row.get(29));
                        let community_local = row.get(31);
                        let community_ap_id: Option<&str> = row.get(32);

                        let community = RespMinimalCommunityInfo {
                            id: community_id,
                            name: Cow::Borrowed(row.get(30)),
                            local: community_local,
                            host: crate::get_actor_host_or_unknown(
                                community_local,
                                community_ap_id,
                                &ctx.local_hostname,
                            ),
                            remote_url: if community_local {
                                Some(Cow::Owned(String::from(
                                    crate::apub_util::get_local_community_apub_id(
                                        community_id,
                                        &ctx.host_url_apub,
                                    ),
                                )))
                            } else {
                                community_ap_id.map(Cow::Borrowed)
                            },
                            deleted: row.get(33),
                        };

                        let content_text: Option<&str> = row.get(12);

                        let post = RespPostListPost {
                            id: post_id,
                            href: row.get::<_, Option<&str>>(11).map(Cow::Borrowed),
                            content_text: content_text.map(Cow::Borrowed),
                            content_markdown: row.get::<_, Option<&str>>(15).map(Cow::Borrowed),
                            content_html_safe: row
                                .get::<_, Option<&str>>(16)
                                .map(|html| crate::clean_html(&html)),
                            title: Cow::Borrowed(row.get(13)),
                            created: post_created.to_rfc3339().into(),
                            score: row.get(22),
                            replies_count_total: Some(row.get(23)),
                            sticky: row.get(24),
                            author: Some(Cow::Owned(author)),
                            remote_url: if post_local {
                                Some(Cow::Owned(String::from(
                                    crate::apub_util::get_local_post_apub_id(
                                        post_id,
                                        &ctx.host_url_apub,
                                    ),
                                )))
                            } else {
                                post_ap_id.map(Cow::Borrowed)
                            },
                            your_vote: None,
                            relevance: None,
                            community: Cow::Owned(community),
                        };

                        Some(RespFlagDetails::Post { post })
                    } else {
                        None
                    }
                }
                _ => None,
            };
            match details {
                None => None,
                Some(details) => {
                    let created_local: chrono::DateTime<chrono::FixedOffset> = row.get(3);

                    let flagger_id = UserLocalID(row.get(4));
                    let flagger_local = row.get(5);
                    let flagger_ap_id: Option<&str> = row.get(7);
                    let flagger_avatar: Option<&str> = row.get(8);

                    let flagger = RespMinimalAuthorInfo {
                        id: flagger_id,
                        username: Cow::Borrowed(row.get(6)),
                        local: flagger_local,
                        host: crate::get_actor_host_or_unknown(
                            flagger_local,
                            flagger_ap_id.as_deref(),
                            &ctx.local_hostname,
                        ),
                        remote_url: if flagger_local {
                            Some(Cow::Owned(String::from(
                                crate::apub_util::get_local_person_apub_id(
                                    flagger_id,
                                    &ctx.host_url_apub,
                                ),
                            )))
                        } else {
                            flagger_ap_id.map(Cow::Borrowed)
                        },
                        avatar: flagger_avatar.map(|url| RespAvatarInfo {
                            url: ctx.process_avatar_href(url, flagger_id).into_owned().into(),
                        }),
                        is_bot: row.get(9),
                    };

                    Some(RespFlagInfo {
                        details,
                        id: FlagLocalID(row.get(1)),
                        content: row.get::<_, Option<&str>>(2).map(|content_text| {
                            JustContentText {
                                content_text: content_text.into(),
                            }
                        }),
                        created_local: created_local.to_rfc3339(),
                        flagger,
                    })
                }
            }
        })
        .collect();

    let output = RespList {
        next_page: None,
        items: Cow::Owned(items),
    };

    crate::json_response(&output)
}

pub fn route_flags() -> crate::RouteNode<()> {
    crate::RouteNode::new().with_handler_async("GET", route_unstable_flags_list)
}
