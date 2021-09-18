use crate::types::{
    CommunityLocalID, MaybeIncludeYour, PostLocalID, RespAvatarInfo, RespCommunityFeeds,
    RespCommunityFeedsType, RespCommunityInfo, RespList, RespMinimalAuthorInfo,
    RespMinimalCommunityInfo, RespModeratorInfo, RespYourFollowInfo, UserLocalID,
};
use serde_derive::Deserialize;
use std::borrow::Cow;
use std::convert::TryInto;
use std::ops::Deref;
use std::sync::Arc;

fn get_community_description_fields<'a>(
    description_text: &'a str,
    description_html: Option<&'a str>,
) -> (&'a str, Option<&'a str>, Option<String>) {
    match description_html {
        Some(description_html) => (
            description_html,
            None,
            Some(crate::clean_html(description_html)),
        ),
        None => (description_text, Some(description_text), None),
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

    #[derive(Deserialize)]
    struct CommunitiesListQuery<'a> {
        search: Option<Cow<'a, str>>,

        local: Option<bool>,

        #[serde(rename = "your_follow.accepted")]
        your_follow_accepted: Option<bool>,

        #[serde(default)]
        include_your: bool,

        #[serde(default = "default_limit")]
        limit: i64,

        page: Option<Cow<'a, str>>,
    }

    let query: CommunitiesListQuery = serde_urlencoded::from_str(req.uri().query().unwrap_or(""))?;

    let mut sql = String::from("SELECT id, name, local, ap_id, description, description_html");
    let mut values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

    let db = ctx.db_pool.get().await?;

    let login_user_maybe = if query.include_your || query.your_follow_accepted.is_some() {
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

    sql.push_str(" FROM community");

    let mut did_where = false;

    if let Some(search) = &query.search {
        values.push(search);
        write!(sql, " WHERE community_fts(community) @@ plainto_tsquery('english', ${0}) ORDER BY ts_rank_cd(community_fts(community), plainto_tsquery('english', ${0})) DESC", values.len()).unwrap();
        did_where = true;
    }
    if let Some(req_your_follow_accepted) = &query.your_follow_accepted {
        values.push(login_user_maybe.as_ref().unwrap());
        write!(
            sql,
            " {} community.id IN (SELECT community FROM community_follow WHERE follower=${}",
            if did_where { "AND" } else { "WHERE" },
            values.len()
        )
        .unwrap();
        did_where = true;
        values.push(req_your_follow_accepted);
        write!(sql, " AND accepted=${})", values.len()).unwrap();
    }
    if let Some(req_local) = &query.local {
        values.push(req_local);
        write!(
            sql,
            " {} community.local=${}",
            if did_where { "AND" } else { "WHERE" },
            values.len()
        )
        .unwrap();
        did_where = true;
    }

    let page = query
        .page
        .as_deref()
        .map(super::parse_number_58)
        .transpose()
        .map_err(|_| super::InvalidPage.into_user_error())?;

    if let Some(page) = &page {
        values.push(page);
        write!(
            sql,
            " {} id >= ${}",
            if did_where { "AND" } else { "WHERE" },
            values.len()
        )
        .unwrap();
    }

    write!(sql, " ORDER BY id").unwrap();

    let limit_plus_1 = query.limit + 1;

    values.push(&limit_plus_1);
    write!(sql, " LIMIT ${}", values.len()).unwrap();

    let sql: &str = &sql;
    let mut rows = db.query(sql, &values).await?;

    let next_page = if rows.len() > query.limit.try_into().unwrap() {
        let row = rows.pop().unwrap();

        let id: i64 = row.get(0);

        Some(super::format_number_58(id))
    } else {
        None
    };

    let rows = rows;

    let output = RespList {
        items: rows
            .iter()
            .map(|row| {
                let id = CommunityLocalID(row.get(0));
                let name: &str = row.get(1);
                let local = row.get(2);
                let ap_id = row.get(3);

                let (description, description_text, description_html) =
                    get_community_description_fields(row.get(4), row.get(5));

                let host = crate::get_actor_host_or_unknown(local, ap_id, &ctx.local_hostname);

                let remote_url = if local {
                    Some(Cow::Owned(String::from(
                        crate::apub_util::get_local_community_apub_id(id, &ctx.host_url_apub),
                    )))
                } else {
                    ap_id.map(Cow::Borrowed)
                };

                RespCommunityInfo {
                    base: RespMinimalCommunityInfo {
                        id,
                        name: Cow::Borrowed(name),
                        local,
                        host,
                        remote_url,
                    },

                    description,
                    description_html,
                    description_text,

                    feeds: RespCommunityFeeds {
                        atom: RespCommunityFeedsType {
                            new: format!("{}/stable/communities/{}/feed", ctx.host_url_api, id),
                        },
                    },

                    you_are_moderator: if query.include_your {
                        Some(row.get(7))
                    } else {
                        None
                    },
                    your_follow: if query.include_your {
                        Some(
                            row.get::<_, Option<bool>>(6)
                                .map(|accepted| RespYourFollowInfo { accepted }),
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
                lang.tr("community_name_disallowed_chars", None)
                    .into_owned(),
            )));
        }
    }

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
                        lang.tr("name_in_use", None).into_owned(),
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
                "SELECT name, local, ap_id, description, description_html, (SELECT accepted FROM community_follow WHERE community=community.id AND follower=$2), EXISTS(SELECT 1 FROM community_moderator WHERE community=community.id AND person=$2) FROM community WHERE id=$1",
                &[&community_id.raw(), &user.raw()],
            ).await?
        } else {
            db.query_opt(
                "SELECT name, local, ap_id, description, description_html FROM community WHERE id=$1",
                &[&community_id.raw()],
            ).await?
        })
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr("no_such_community", None).into_owned(),
            ))
        })?
    };

    let community_local = row.get(1);
    let community_ap_id: Option<&str> = row.get(2);

    let community_remote_url = if community_local {
        Some(Cow::Owned(String::from(
            crate::apub_util::get_local_community_apub_id(community_id, &ctx.host_url_apub),
        )))
    } else {
        community_ap_id.map(Cow::Borrowed)
    };

    let (description, description_text, description_html) =
        get_community_description_fields(row.get(3), row.get(4));

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
        },
        description,
        description_html,
        description_text,
        feeds: RespCommunityFeeds {
            atom: RespCommunityFeedsType {
                new: format!(
                    "{}/stable/communities/{}/feed",
                    ctx.host_url_api, community_id
                ),
            },
        },
        you_are_moderator: if query.include_your {
            Some(row.get(6))
        } else {
            None
        },
        your_follow: if query.include_your {
            Some(
                row.get::<_, Option<bool>>(5)
                    .map(|accepted| RespYourFollowInfo { accepted }),
            )
        } else {
            None
        },
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

    let user = crate::require_login(&req, &db).await?;

    #[derive(Deserialize)]
    struct CommunitiesEditBody<'a> {
        description: Option<Cow<'a, str>>,
    }

    let body = hyper::body::to_bytes(req.into_body()).await?;
    let body: CommunitiesEditBody = serde_json::from_slice(&body)?;

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
                lang.tr("community_edit_denied", None).into_owned(),
            ))),
            Some(_) => Ok(()),
        }
    })?;

    if let Some(description) = body.description {
        db.execute(
            "UPDATE community SET description=$1 WHERE id=$2",
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
        .query_opt("SELECT local FROM community WHERE id=$1", &[&community])
        .await?
        .ok_or_else(|| {
            crate::Error::UserError(crate::simple_response(
                hyper::StatusCode::NOT_FOUND,
                lang.tr("no_such_community", None).into_owned(),
            ))
        })?;

    let community_local: bool = row.get(0);

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
                lang.tr("no_such_community", None).into_owned(),
            ))),
            Some(row) => {
                if row.get(0) {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::NOT_FOUND,
                        lang.tr("community_moderators_not_local", None).into_owned(),
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
                    crate::apub_util::get_local_person_apub_id(id, &ctx.host_url_apub),
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
                lang.tr("must_be_moderator", None).into_owned(),
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
                lang.tr("no_such_user", None).into_owned(),
            ))),
            Some(row) => {
                let local: bool = row.get(0);

                if local {
                    Ok(())
                } else {
                    Err(crate::Error::UserError(crate::simple_response(
                        hyper::StatusCode::FORBIDDEN,
                        lang.tr("moderators_only_local", None).into_owned(),
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
                lang.tr("must_be_moderator", None).into_owned(),
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
                lang.tr("community_moderators_remove_must_be_older", None)
                    .into_owned(),
            )))
        }
    }
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
    let db = ctx.db_pool.get().await?;

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
                lang.tr("community_edit_denied", None).into_owned(),
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
                lang.tr("no_such_post", None).into_owned(),
            ))
        })?;

    if community_id != CommunityLocalID(old_row.get(0)) {
        return Err(crate::Error::UserError(crate::simple_response(
            hyper::StatusCode::NOT_FOUND,
            lang.tr("post_not_in_community", None).into_owned(),
        )));
    }

    let old_approved: bool = old_row.get(1);
    let old_sticky: bool = old_row.get(4);

    let post_ap_id = if old_row.get(2) {
        crate::apub_util::get_local_post_apub_id(post_id, &ctx.host_url_apub).into()
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
        write!(sql, "approved=${}", values.len()).unwrap();
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

        log::debug!("sql = {}", sql);

        db.execute(sql.deref(), &values).await?;

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
        .with_handler_async("GET", route_unstable_communities_list)
        .with_handler_async("POST", route_unstable_communities_create)
        .with_child_parse::<CommunityLocalID, _>(
            crate::RouteNode::new()
                .with_handler_async("GET", route_unstable_communities_get)
                .with_handler_async("PATCH", route_unstable_communities_patch)
                .with_child(
                    "follow",
                    crate::RouteNode::new()
                        .with_handler_async("POST", route_unstable_communities_follow),
                )
                .with_child(
                    "moderators",
                    crate::RouteNode::new()
                        .with_handler_async("GET", route_unstable_communities_moderators_list)
                        .with_child_parse::<UserLocalID, _>(
                            crate::RouteNode::new()
                                .with_handler_async(
                                    "PUT",
                                    route_unstable_communities_moderators_add,
                                )
                                .with_handler_async(
                                    "DELETE",
                                    route_unstable_communities_moderators_remove,
                                ),
                        ),
                )
                .with_child(
                    "unfollow",
                    crate::RouteNode::new()
                        .with_handler_async("POST", route_unstable_communities_unfollow),
                )
                .with_child(
                    "posts",
                    crate::RouteNode::new().with_child_parse::<PostLocalID, _>(
                        crate::RouteNode::new()
                            .with_handler_async("PATCH", route_unstable_communities_posts_patch),
                    ),
                ),
        )
}
