use super::{format_number_58, parse_number_58, CommunitiesSortType, InvalidPage, ValueConsumer};
use crate::lang;
use crate::types::{
    CommunityLocalID, MaybeIncludeYour, PostLocalID, RespAvatarInfo, RespCommunityFeeds,
    RespCommunityFeedsType, RespCommunityInfo, RespCommunityModlogEvent,
    RespCommunityModlogEventDetails, RespList, RespMinimalAuthorInfo, RespMinimalCommunityInfo,
    RespMinimalPostInfo, RespModeratorInfo, RespYourFollowInfo, UserLocalID,
};
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Deref;
use std::sync::Arc;

async fn require_community_exists(
    community_id: CommunityLocalID,
    db: &tokio_postgres::Client,
    lang: &crate::Translator,
) -> Result<(), crate::Error> {
    let row = db
        .query_opt(
            "SELECT deleted FROM community WHERE id=$1",
            &[&community_id],
        )
        .await?;
    let exists = match row {
        None => false,
        Some(row) => !row.get::<_, bool>(0),
    };

    if exists {
        Ok(())
    } else {
        Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr(&lang::no_such_community()).into_owned(),
        )))
    }
}

fn get_community_description_content<'a>(
    description_text: Option<&'a str>,
    description_markdown: Option<&'a str>,
    description_html: Option<&'a str>,
) -> crate::types::Content<'a> {
    crate::types::Content {
        content_text: if description_text.is_none()
            && description_markdown.is_none()
            && description_html.is_none()
        {
            Some(Cow::Borrowed(""))
        } else {
            description_text.map(Cow::Borrowed)
        },
        content_markdown: description_markdown.map(Cow::Borrowed),
        content_html_safe: description_html.map(|x| crate::clean_html(x)),
    }
}

async fn route_unstable_communities_list(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use std::fmt::Write;

    fn default_limit() -> i64 {
        30
    }

    fn default_sort() -> CommunitiesSortType {
        CommunitiesSortType::OldLocal
    }

    #[derive(Deserialize)]
    struct CommunitiesListQuery<'a> {
        search: Option<Cow<'a, str>>,

        local: Option<bool>,

        #[serde(rename = "your_follow.accepted")]
        your_follow_accepted: Option<bool>,

        you_are_moderator: Option<bool>,

        #[serde(default)]
        include_your: bool,

        #[serde(default = "default_limit")]
        limit: i64,

        page: Option<Cow<'a, str>>,

        #[serde(default = "default_sort")]
        sort: CommunitiesSortType,
    }

    let query: CommunitiesListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let mut sql = String::from(
        "SELECT id, name, local, ap_id, description, description_html, description_markdown",
    );
    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

    let db = ctx.db_pool.get().await?;

    let login_user_maybe = if query.include_your
        || query.your_follow_accepted.is_some()
        || query.you_are_moderator.is_some()
    {
        Some(crate::require_login(&req, &db).await?)
    } else {
        None
    };

    let include_your_for = if query.include_your {
        Some(login_user_maybe.unwrap())
    } else {
        None
    };

    if let Some(user) = &include_your_for {
        values.push(user);
        sql.push_str(", (SELECT accepted FROM community_follow WHERE community=community.id AND follower=$1), EXISTS(SELECT 1 FROM community_moderator WHERE community=community.id AND person=$1)");
    }

    sql.push_str(" FROM community WHERE NOT deleted");

    if let Some(search) = &query.search {
        values.push(search);
        write!(
            sql,
            " AND community_fts(community) @@ plainto_tsquery('english', ${0})",
            values.len()
        )
        .unwrap();
    }
    if let Some(req_your_follow_accepted) = &query.your_follow_accepted {
        values.push(login_user_maybe.as_ref().unwrap());
        write!(
            sql,
            " AND community.id IN (SELECT community FROM community_follow WHERE follower=${}",
            values.len()
        )
        .unwrap();
        values.push(req_your_follow_accepted);
        write!(sql, " AND accepted=${})", values.len()).unwrap();
    }
    if let Some(req_you_are_moderator) = &query.you_are_moderator {
        write!(sql, " AND community.id ").unwrap();

        if !req_you_are_moderator {
            write!(sql, "NOT ").unwrap();
        }

        values.push(login_user_maybe.as_ref().unwrap());
        write!(
            sql,
            "IN (SELECT community FROM community_moderator WHERE person=${})",
            values.len()
        )
        .unwrap();
    }
    if let Some(req_local) = &query.local {
        values.push(req_local);
        write!(sql, " AND community.local=${}", values.len()).unwrap();
    }

    let mut con1 = None;
    let mut con2 = None;
    let (page_part1, page_part2) = query
        .sort
        .handle_page(
            query.page.as_deref(),
            ValueConsumer {
                targets: vec![&mut con1, &mut con2],
                start_idx: values.len(),
                used: 0,
            },
        )
        .map_err(super::InvalidPage::into_user_error)?;
    if let Some(value) = &con1 {
        values.push(value.as_ref());
        if let Some(value) = &con2 {
            values.push(value.as_ref());
        }
    }

    if let Some(part) = page_part1 {
        sql.push_str(&part);
    }

    write!(sql, " ORDER BY {}", query.sort.sort_sql()).unwrap();

    let limit_plus_1 = query.limit + 1;

    values.push(&limit_plus_1);
    write!(sql, " LIMIT ${}", values.len()).unwrap();

    if let Some(part) = page_part2 {
        sql.push_str(&part);
    }

    log::debug!("sql = {:?}", sql);

    let sql: &str = &sql;
    let mut rows = db.query(sql, &values).await?;

    let next_page = if rows.len() > query.limit.try_into().unwrap() {
        let row = rows.pop().unwrap();

        let id = CommunityLocalID(row.get(0));
        let name = Cow::Borrowed(row.get(1));
        let local = row.get(2);
        let ap_id: Option<&str> = row.get(3);

        Some(query.sort.get_next_page(
            &RespMinimalCommunityInfo {
                host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                id,
                name,
                local,
                remote_url: ap_id.map(Cow::Borrowed),
                deleted: false,
            },
            query.page.as_deref(),
        ))
    } else {
        None
    };

    let rows = rows;

    let pending_moderation_actions_map = if query.include_your {
        let moderated_communities: Vec<_> = rows
            .iter()
            .filter_map(|row| {
                if row.get(8) {
                    Some(CommunityLocalID(row.get(0)))
                } else {
                    None
                }
            })
            .collect();

        if moderated_communities.is_empty() {
            None
        } else {
            let rows = db.query("SELECT COUNT(*), post.community FROM flag INNER JOIN post ON (post.id = post) WHERE flag.to_community AND NOT flag.to_community_dismissed AND post.approved AND post.community=ANY($1::BIGINT[]) GROUP BY post.community", &[&moderated_communities]).await?;
            Some(
                rows.into_iter()
                    .map(|row| (CommunityLocalID(row.get(1)), row.get(0)))
                    .collect::<HashMap<CommunityLocalID, i64>>(),
            )
        }
    } else {
        None
    };

    let output = RespList {
        items: rows
            .iter()
            .map(|row| {
                let id = CommunityLocalID(row.get(0));
                let name: &str = row.get(1);
                let local = row.get(2);
                let ap_id = row.get(3);

                let host = crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname);

                let remote_url = if local {
                    Some(Cow::Owned(String::from(
                        crate::apub_util::LocalObjectRef::Community(id)
                            .to_local_uri(&ctx.host_url_apub),
                    )))
                } else {
                    ap_id.map(Cow::Borrowed)
                };

                let you_are_moderator = if query.include_your {
                    Some(row.get(8))
                } else {
                    None
                };

                RespCommunityInfo {
                    base: RespMinimalCommunityInfo {
                        id,
                        name: Cow::Borrowed(name),
                        local,
                        host,
                        remote_url,
                        deleted: false,
                    },

                    description: get_community_description_content(
                        row.get(4),
                        row.get(6),
                        row.get(5),
                    ),

                    feeds: RespCommunityFeeds {
                        atom: RespCommunityFeedsType {
                            new: format!("{}/stable/communities/{}/feed", ctx.host_url_api, id),
                        },
                    },

                    you_are_moderator,
                    your_follow: if query.include_your {
                        Some(
                            row.get::<_, Option<bool>>(7)
                                .map(|accepted| RespYourFollowInfo { accepted }),
                        )
                    } else {
                        None
                    },

                    pending_moderation_actions: if you_are_moderator == Some(true) {
                        Some(
                            pending_moderation_actions_map
                                .as_ref()
                                .unwrap()
                                .get(&id)
                                .copied()
                                .unwrap_or(0) as u32,
                        )
                    } else {
                        None
                    },
                }
            })
            .collect::<Vec<_>>()
            .into(),
        next_page: next_page.map(Cow::Owned),
    };

    crate::json_response(&output)
}

async fn route_unstable_communities_create(
    _: (),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let lang = crate::get_lang_for_req(&req);

    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunitiesCreateBody<'a> {
        name: &'a str,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommunitiesCreateBody<'_> = serde_json::from_slice(&body)?;

    for ch in body.name.chars() {
        if !super::USERNAME_ALLOWED_CHARS.contains(&ch) {
            return Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::BAD_REQUEST,
                lang.tr(&lang::community_name_disallowed_chars())
                    .into_owned(),
            )));
        }
    }

    {
        let row = db
            .query_one(
                "SELECT community_creation_requirement FROM site WHERE local=TRUE",
                &[],
            )
            .await?;
        let requirement: Option<&str> = row.get(0);
        match requirement {
            None => Ok(()),
            Some(_) => {
                if crate::is_site_admin(&db, user).await? {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr(&lang::permission_missing_create_community())
                            .into_owned(),
                    )))
                }
            }
        }
    }?;

    let rsa = openssl::rsa::Rsa::generate(crate::KEY_BITS)?;
    let private_key = rsa.private_key_to_pem()?;
    let public_key = rsa.public_key_to_pem()?;

    let community_id = {
        let trans = db.transaction().await?;

        trans
            .execute(
                "INSERT INTO local_actor_name (name) VALUES ($1)",
                &[&body.name],
            )
            .await
            .map_err(|err| {
                if err.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                    crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr(&lang::name_in_use()).into_owned(),
                    ))
                } else {
                    err.into()
                }
            })?;

        let row = trans
            .query_one(
                "INSERT INTO community (name, local, private_key, public_key, created_by, created_local) VALUES ($1, TRUE, $2, $3, $4, current_timestamp) RETURNING id",
                &[&body.name, &private_key, &public_key, &user.raw()],
            )
            .await?;

        let community_id = CommunityLocalID(row.get(0));

        trans
            .execute(
                "INSERT INTO community_moderator (community, person, created_local) VALUES ($1, $2, current_timestamp)",
                &[&community_id, &user],
            )
            .await?;

        trans.commit().await?;

        community_id
    };

    crate::json_response(&serde_json::json!({"community": {"id": community_id}}))
}

async fn route_unstable_communities_delete(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let res = {
        let row = db
            .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
            .await?;

        match row {
            None => {
                return Ok(crate::empty_response()); // already gone
            }
            Some(row) => {
                if row.get(0) {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::BAD_REQUEST,
                        lang.tr(&lang::community_not_local()).into_owned(),
                    )))
                }
            }
        }
    };

    res?;

    ({
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
                        lang.tr(&lang::community_edit_denied()).into_owned(),
                    )))
                }
            }
            Some(_) => Ok(()),
        }
    })?;

    let row_count = db.execute("UPDATE community SET deleted=TRUE, old_name=name, name='[deleted]', description=NULL, description_html=NULL, description_markdown=NULL WHERE id=$1 AND NOT deleted", &[&community_id]).await?;

    if row_count > 0 {
        // successfully deleted, inform followers

        let delete_ap =
            crate::apub_util::local_community_delete_to_ap(community_id, &ctx.host_url_apub);
        crate::spawn_task(crate::apub_util::enqueue_forward_to_community_followers(
            community_id,
            serde_json::to_string(&delete_ap)?,
            ctx,
        ));
    }

    Ok(crate::empty_response())
}

async fn route_unstable_communities_get(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let query: MaybeIncludeYour = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let row = {
        (if query.include_your {
            let user = crate::require_login(&req, &db).await?;
            db.query_opt(
                "SELECT name, local, ap_id, description, description_html, description_markdown, (SELECT accepted FROM community_follow WHERE community=community.id AND follower=$2), EXISTS(SELECT 1 FROM community_moderator WHERE community=community.id AND person=$2) FROM community WHERE id=$1 AND NOT deleted",
                &[&community_id.raw(), &user.raw()],
            ).await?
        } else {
            db.query_opt(
                "SELECT name, local, ap_id, description, description_html, description_markdown FROM community WHERE id=$1 AND NOT deleted",
                &[&community_id.raw()],
            ).await?
        })
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr(&lang::no_such_community()).into_owned(),
            ))
        })?
    };

    let community_local = row.get(1);
    let community_ap_id: Option<&str> = row.get(2);

    let community_remote_url = if community_local {
        Some(Cow::Owned(String::from(
            crate::apub_util::LocalObjectRef::Community(community_id)
                .to_local_uri(&ctx.host_url_apub),
        )))
    } else {
        community_ap_id.map(Cow::Borrowed)
    };

    let you_are_moderator = if query.include_your {
        Some(row.get(7))
    } else {
        None
    };

    let pending_moderation_actions = if you_are_moderator == Some(true) {
        let row = db.query_one("SELECT COUNT(*) FROM flag INNER JOIN post ON (post.id = post) WHERE flag.to_community AND post.approved AND post.community=$1", &[&community_id]).await?;
        Some(row.get::<_, i64>(0) as u32)
    } else {
        None
    };

    let info = RespCommunityInfo {
        base: RespMinimalCommunityInfo {
            id: community_id,
            name: Cow::Borrowed(row.get(0)),
            local: community_local,
            host: if community_local {
                (&ctx.local_hostname).into()
            } else {
                match community_ap_id.and_then(crate::get_url_host_from_str) {
                    Some(host) => host.into(),
                    None => "[unknown]".into(),
                }
            },
            remote_url: community_remote_url,
            deleted: false, // already should have failed if deleted
        },
        description: get_community_description_content(row.get(3), row.get(5), row.get(4)),
        feeds: RespCommunityFeeds {
            atom: RespCommunityFeedsType {
                new: format!(
                    "{}/stable/communities/{}/feed",
                    ctx.host_url_api, community_id
                ),
            },
        },
        you_are_moderator,
        your_follow: if query.include_your {
            Some(
                row.get::<_, Option<bool>>(6)
                    .map(|accepted| RespYourFollowInfo { accepted }),
            )
        } else {
            None
        },

        pending_moderation_actions,
    };

    crate::json_response(&info)
}

async fn route_unstable_communities_patch(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    require_community_exists(community_id, &db, &lang).await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunitiesEditBody<'a> {
        description_text: Option<Cow<'a, str>>,
        description_markdown: Option<Cow<'a, str>>,
        description_html: Option<Cow<'a, str>>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommunitiesEditBody = serde_json::from_slice(&body)?;

    let too_many_description_updates = if body.description_text.is_some() {
        body.description_markdown.is_some() || body.description_html.is_some()
    } else {
        body.description_markdown.is_some() && body.description_html.is_some()
    };

    if too_many_description_updates {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::BAD_REQUEST,
            lang.tr(&lang::description_content_conflict()).into_owned(),
        )));
    }

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::community_edit_denied()).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    if let Some(description) = body.description_text {
        db.execute(
            "UPDATE community SET description=$1, description_markdown=NULL, description_html=NULL WHERE id=$2",
            &[&description, &community_id],
        )
        .await?;

        crate::apub_util::spawn_enqueue_send_new_community_update(community_id, ctx);
    } else if let Some(description) = body.description_markdown {
        let html = super::render_markdown_with_mentions(&description, &ctx).await?;

        db.execute(
            "UPDATE community SET description=NULL, description_markdown=$1, description_html=$3 WHERE id=$2",
            &[&description, &community_id, &html],
        )
        .await?;

        crate::apub_util::spawn_enqueue_send_new_community_update(community_id, ctx);
    } else if let Some(description) = body.description_html {
        db.execute(
            "UPDATE community SET description=NULL, description_markdown=NULL, description_html=$1 WHERE id=$2",
            &[&description, &community_id],
        )
        .await?;

        crate::apub_util::spawn_enqueue_send_new_community_update(community_id, ctx);
    }

    Ok(crate::empty_response())
}

async fn route_unstable_communities_follow(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community,) = params;

    let lang = crate::get_lang_for_req(&req);
    let db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunitiesFollowBody {
        #[serde(default)]
        try_wait_for_accept: bool,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommunitiesFollowBody = serde_json::from_slice(&body)?;

    let row = db
        .query_opt(
            "SELECT local, deleted FROM community WHERE id=$1",
            &[&community],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr(&lang::no_such_community()).into_owned(),
            ))
        })?;

    let community_local: bool = row.get(0);

    if row.get(1) {
        // deleted

        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr(&lang::no_such_community()).into_owned(),
        )));
    }

    let row_count = db.execute("INSERT INTO community_follow (community, follower, local, accepted) VALUES ($1, $2, TRUE, $3) ON CONFLICT DO NOTHING", &[&community, &user.raw(), &community_local]).await?;

    let output = if community_local {
        RespYourFollowInfo { accepted: true }
    } else if row_count > 0 {
        crate::apub_util::spawn_enqueue_send_community_follow(community, user, ctx);

        if body.try_wait_for_accept {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            let row = db
                .query_one(
                    "SELECT accepted FROM community_follow WHERE community=$1 AND follower=$2",
                    &[&community, &user.raw()],
                )
                .await?;

            RespYourFollowInfo {
                accepted: row.get(0),
            }
        } else {
            RespYourFollowInfo { accepted: false }
        }
    } else {
        let row = db
            .query_one(
                "SELECT accepted FROM community_follow WHERE community=$1 AND follower=$2",
                &[&community, &user.raw()],
            )
            .await?;

        RespYourFollowInfo {
            accepted: row.get(0),
        }
    };

    crate::json_response(&output)
}

async fn route_unstable_communities_moderators_list(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id,) = params;

    let lang = crate::get_lang_for_req(&req);

    let db = ctx.db_pool.get().await?;

    ({
        let row = db
            .query_opt("SELECT local FROM community WHERE id=$1", &[&community_id])
            .await?;

        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr(&lang::no_such_community()).into_owned(),
            ))),
            Some(row) => {
                if row.get(0) {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::NOT_FOUND,
                        lang.tr(&lang::community_moderators_not_local())
                            .into_owned(),
                    )))
                }
            }
        }
    })?;

    let rows = db.query(
        "SELECT person.id, person.username, person.local, person.ap_id, person.avatar, community_moderator.created_local, person.is_bot FROM person, community_moderator WHERE person.id = community_moderator.person AND community_moderator.community = $1 ORDER BY community_moderator.created_local ASC NULLS FIRST",
        &[&community_id],
    ).await?;

    let output: Vec<_> = rows
        .iter()
        .map(|row| {
            let id = UserLocalID(row.get(0));
            let local = row.get(2);
            let ap_id: Option<_> = row.get(3);

            let moderator_since: Option<chrono::DateTime<chrono::offset::Utc>> = row.get(5);

            let remote_url = if local {
                Some(Cow::Owned(String::from(
                    crate::apub_util::LocalObjectRef::User(id).to_local_uri(&ctx.host_url_apub),
                )))
            } else {
                ap_id.map(Cow::Borrowed)
            };

            RespModeratorInfo {
                base: RespMinimalAuthorInfo {
                    id,
                    username: Cow::Borrowed(row.get(1)),
                    local,
                    host: crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname),
                    remote_url,
                    is_bot: row.get(6),
                    avatar: row.get::<_, Option<&str>>(4).map(|url| RespAvatarInfo {
                        url: ctx.process_avatar_href(url, id),
                    }),
                },

                moderator_since: moderator_since.map(|time| time.to_rfc3339()),
            }
        })
        .collect();

    crate::json_response(&output)
}

async fn route_unstable_communities_moderators_add(
    params: (CommunityLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, user_id) = params;

    let db = ctx.db_pool.get().await?;

    let lang = crate::get_lang_for_req(&req);
    let login_user = crate::require_login(&req, &db).await?;

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &login_user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::must_be_moderator()).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    ({
        let row = db
            .query_opt("SELECT local FROM person WHERE id=$1", &[&user_id])
            .await?;

        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::no_such_user()).into_owned(),
            ))),
            Some(row) => {
                let local: bool = row.get(0);

                if local {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        lang.tr(&lang::moderators_only_local()).into_owned(),
                    )))
                }
            }
        }
    })?;

    db.execute(
        "INSERT INTO community_moderator (community, person, created_local) VALUES ($1, $2, current_timestamp)",
        &[&community_id, &user_id],
    )
    .await?;

    Ok(crate::empty_response())
}

async fn route_unstable_communities_moderators_remove(
    params: (CommunityLocalID, UserLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community_id, user_id) = params;

    let mut db = ctx.db_pool.get().await?;

    let lang = crate::get_lang_for_req(&req);
    let login_user = crate::require_login(&req, &db).await?;

    let self_moderator_row = db
        .query_opt(
            "SELECT created_local FROM community_moderator WHERE community=$1 AND person=$2",
            &[&community_id, &login_user],
        )
        .await?;

    let self_moderator_since: Option<chrono::DateTime<chrono::offset::Utc>> = ({
        match self_moderator_row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::must_be_moderator()).into_owned(),
            ))),
            Some(row) => Ok(row.get(0)),
        }
    })?;

    {
        let trans = db.transaction().await?;
        let row = trans.query_opt(
            "DELETE FROM community_moderator WHERE community=$1 AND person=$2 RETURNING (created_local >= $3)",
            &[&community_id, &user_id, &self_moderator_since],
        )
        .await?;

        let is_allowed = match self_moderator_since {
            None => true, // self was moderator before timestamps existed, can remove anyone
            Some(_) => {
                match row {
                    None => true, // was already removed, ok
                    Some(row) => {
                        let res: Option<bool> = row.get(0);
                        res.unwrap_or(false) // other has no timestamp, must be older
                    }
                }
            }
        };

        if is_allowed {
            trans.commit().await?;
            Ok(crate::empty_response())
        } else {
            trans.rollback().await?;

            Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::community_moderators_remove_must_be_older())
                    .into_owned(),
            )))
        }
    }
}

async fn route_unstable_communities_modlog_events_list(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community,) = params;
    let db = ctx.db_pool.get().await?;

    fn default_limit() -> u32 {
        30
    }

    #[derive(Deserialize)]
    struct ModlogEventsListQuery<'a> {
        #[serde(default = "default_limit")]
        limit: u32,

        page: Option<Cow<'a, str>>,
    }

    let query: ModlogEventsListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let inner_limit = i64::from(query.limit) + 1;

    let page = query
        .page
        .as_deref()
        .map(parse_number_58)
        .transpose()
        .map_err(|_| InvalidPage.into_user_error())?;

    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        vec![&community, &inner_limit];

    let rows = db.query(&format!("SELECT modlog_event.id, modlog_event.time, modlog_event.action, post.id, post.title, post.ap_id, post.local, post.sensitive FROM modlog_event LEFT OUTER JOIN post ON (post.id = modlog_event.post) WHERE modlog_event.by_community=$1{}ORDER BY modlog_event.id DESC LIMIT $2", if let Some(page) = &page {
        values.push(page);

        " AND modlog_event.id <= $3"
    } else {
        ""
    }), &values).await?;

    let (rows, next_page) = if rows.len() > query.limit as usize {
        let next_page = format_number_58(rows.last().unwrap().get(0));
        (&rows[..(query.limit as usize)], Some(Cow::Owned(next_page)))
    } else {
        (&rows[..], None)
    };

    let output = RespList {
        items: rows
            .iter()
            .filter_map(|row| {
                let time: chrono::DateTime<chrono::FixedOffset> = row.get(1);
                let action = row.get(2);

                let post = row.get::<_, Option<_>>(3).map(|post_id| {
                    let post_id = PostLocalID(post_id);
                    let post_title = row.get(4);
                    let post_ap_id: Option<&str> = row.get(5);
                    let post_local: bool = row.get(6);
                    let post_sensitive: bool = row.get(7);

                    let post_remote_url = if post_local {
                        Some(Cow::Owned(String::from(
                            crate::apub_util::LocalObjectRef::Post(post_id)
                                .to_local_uri(&ctx.host_url_apub),
                        )))
                    } else {
                        post_ap_id.map(Cow::Borrowed)
                    };

                    RespMinimalPostInfo {
                        id: post_id,
                        title: post_title,
                        remote_url: post_remote_url,
                        sensitive: post_sensitive,
                    }
                });

                let details = match action {
                    "approve_post" => RespCommunityModlogEventDetails::ApprovePost { post: post? },
                    "reject_post" => RespCommunityModlogEventDetails::RejectPost { post: post? },
                    _ => return None,
                };

                Some(RespCommunityModlogEvent {
                    time: time.to_rfc3339(),
                    details,
                })
            })
            .collect(),
        next_page,
    };

    crate::json_response(&output)
}

async fn route_unstable_communities_unfollow(
    params: (CommunityLocalID,),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    let (community,) = params;
    let mut db = ctx.db_pool.get().await?;

    let user = crate::require_login(&req, &db).await?;

    let new_undo = {
        let trans = db.transaction().await?;

        let row_count = trans
            .execute(
                "DELETE FROM community_follow WHERE community=$1 AND follower=$2",
                &[&community, &user.raw()],
            )
            .await?;

        if row_count > 0 {
            let id = uuid::Uuid::new_v4();
            trans.execute(
                "INSERT INTO local_community_follow_undo (id, community, follower) VALUES ($1, $2, $3)",
                &[&id, &community, &user.raw()],
            ).await?;

            trans.commit().await?;

            Some(id)
        } else {
            None
        }
    };

    if let Some(new_undo) = new_undo {
        crate::apub_util::spawn_enqueue_send_community_follow_undo(new_undo, community, user, ctx);
    }

    Ok(crate::simple_response(hyper::StatusCode::ACCEPTED, ""))
}

async fn route_unstable_communities_posts_patch(
    params: (CommunityLocalID, PostLocalID),
    ctx: Arc<crate::RouteContext>,
    req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, crate::Error> {
    use std::fmt::Write;

    let (community_id, post_id) = params;

    let lang = crate::get_lang_for_req(&req);
    let mut db = ctx.db_pool.get().await?;

    require_community_exists(community_id, &db, &lang).await?;

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunityPostEditBody {
        approved: Option<bool>,
        sticky: Option<bool>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommunityPostEditBody = serde_json::from_slice(&body)?;

    ({
        let row = db
            .query_opt(
                "SELECT 1 FROM community_moderator WHERE community=$1 AND person=$2",
                &[&community_id, &user],
            )
            .await?;
        match row {
            None => Err(crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::FORBIDDEN,
                lang.tr(&lang::community_edit_denied()).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    let old_row = db
        .query_opt(
            "SELECT community, approved, local, ap_id, sticky FROM post WHERE id=$1",
            &[&post_id],
        )
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr(&lang::no_such_post()).into_owned(),
            ))
        })?;

    if community_id != CommunityLocalID(old_row.get(0)) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr(&lang::post_not_in_community()).into_owned(),
        )));
    }

    let old_approved: bool = old_row.get(1);
    let old_sticky: bool = old_row.get(4);

    let post_ap_id = if old_row.get(2) {
        crate::apub_util::LocalObjectRef::Post(post_id)
            .to_local_uri(&ctx.host_url_apub)
            .into()
    } else {
        std::str::FromStr::from_str(old_row.get(3))?
    };

    let mut sql = "UPDATE post SET ".to_owned();
    let mut values: Vec<&(dyn postgres_types::ToSql + Sync)> = vec![&post_id];
    let mut any_changes = false;

    if let Some(approved) = &body.approved {
        if !any_changes {
            any_changes = true;
        } else {
            sql.push(',');
        }
        values.push(approved);
        write!(sql, "approved=${0}, rejected=(NOT ${0})", values.len()).unwrap();
    }
    if let Some(sticky) = &body.sticky {
        if !any_changes {
            any_changes = true;
        } else {
            sql.push(',');
        }
        values.push(sticky);
        write!(sql, "sticky=${}", values.len()).unwrap();
    }

    if any_changes {
        sql.push_str(" WHERE id=$1");

        {
            let trans = db.transaction().await?;
            trans.execute(sql.deref(), &values).await?;

            if let Some(approved) = body.approved {
                if approved != old_approved {
                    let action = if approved {
                        "approve_post"
                    } else {
                        "reject_post"
                    };

                    trans.execute("INSERT INTO modlog_event (time, by_community, by_person, action, post) VALUES (current_timestamp, $1, $2, $3, $4)", &[&community_id, &user, &action, &post_id]).await?;
                }
            }

            trans.commit().await?;
        }

        if let Some(approved) = body.approved {
            if approved != old_approved {
                if approved {
                    crate::apub_util::spawn_announce_community_post(
                        community_id,
                        post_id,
                        post_ap_id,
                        ctx.clone(),
                    );
                } else {
                    crate::apub_util::spawn_enqueue_send_community_post_announce_undo(
                        community_id,
                        post_id,
                        post_ap_id,
                        ctx.clone(),
                    );
                }
            }
        }

        if let Some(sticky) = body.sticky {
            if sticky != old_sticky {
                crate::apub_util::spawn_enqueue_send_new_community_update(community_id, ctx);
            }
        }
    }

    Ok(crate::empty_response())
}

pub fn route_communities() -> crate::RouteNode<()> {
    crate::RouteNode::new()
        .with_handler_async(hyper::Method::GET, route_unstable_communities_list)
        .with_handler_async(hyper::Method::POST, route_unstable_communities_create)
        .with_child_parse::<CommunityLocalID, _>(
            crate::RouteNode::new()
                .with_handler_async(hyper::Method::DELETE, route_unstable_communities_delete)
                .with_handler_async(hyper::Method::GET, route_unstable_communities_get)
                .with_handler_async(hyper::Method::PATCH, route_unstable_communities_patch)
                .with_child(
                    "follow",
                    crate::RouteNode::new()
                        .with_handler_async(hyper::Method::POST, route_unstable_communities_follow),
                )
                .with_child(
                    "moderators",
                    crate::RouteNode::new()
                        .with_handler_async(
                            hyper::Method::GET,
                            route_unstable_communities_moderators_list,
                        )
                        .with_child_parse::<UserLocalID, _>(
                            crate::RouteNode::new()
                                .with_handler_async(
                                    hyper::Method::PUT,
                                    route_unstable_communities_moderators_add,
                                )
                                .with_handler_async(
                                    hyper::Method::DELETE,
                                    route_unstable_communities_moderators_remove,
                                ),
                        ),
                )
                .with_child(
                    "modlog",
                    crate::RouteNode::new().with_child(
                        "events",
                        crate::RouteNode::new().with_handler_async(
                            hyper::Method::GET,
                            route_unstable_communities_modlog_events_list,
                        ),
                    ),
                )
                .with_child(
                    "unfollow",
                    crate::RouteNode::new().with_handler_async(
                        hyper::Method::POST,
                        route_unstable_communities_unfollow,
                    ),
                )
                .with_child(
                    "posts",
                    crate::RouteNode::new().with_child_parse::<PostLocalID, _>(
                        crate::RouteNode::new().with_handler_async(
                            hyper::Method::PATCH,
                            route_unstable_communities_posts_patch,
                        ),
                    ),
                ),
        )
}
