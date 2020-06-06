use std::sync::Arc;

pub const ACTIVITY_TYPE: &'static str = "application/activity+json";

pub fn get_local_person_apub_id(person: i64, host_url_apub: &str) -> String {
    format!("{}/users/{}", host_url_apub, person)
}

pub async fn send_community_follow(community: i64, local_follower: i64, ctx: Arc<crate::RouteContext>) -> Result<(), crate::Error> {
    let (community_ap_id, community_inbox): (String, String) = {
        let db = ctx.db_pool.get().await?;

        let row = db.query_one("SELECT local, ap_id, ap_inbox FROM community WHERE id=$1", &[&community]).await?;
        let local = row.get(0);
        if local {
            // no need to send follows to ourself
            return Ok(());
        } else {
            let ap_id = row.get(1);
            let ap_inbox = row.get(2);

            (if let Some(ap_id) = ap_id {
                if let Some(ap_inbox) = ap_inbox {
                    Some((ap_id, ap_inbox))
                } else {
                    None
                }
            } else {
                None
            })
                .ok_or_else(|| crate::Error::InternalStr(format!("Missing apub info for community {}", community)))?
        }
    };

    let mut follow = activitystreams::activity::Follow::new();

    let person_ap_id = get_local_person_apub_id(local_follower, &ctx.host_url_apub);

    follow.follow_props.set_actor_xsd_any_uri(person_ap_id)?;

    follow.follow_props.set_object_xsd_any_uri(community_ap_id.as_ref())?;
    follow.object_props.set_to_xsd_any_uri(community_ap_id)?;

    println!("{:?}", follow);

    let body = serde_json::to_vec(&follow)?.into();

    let req = hyper::Request::post(community_inbox)
        .header(hyper::header::CONTENT_TYPE, ACTIVITY_TYPE)
        .body(body)?;

    let res = crate::res_to_error(ctx.http_client.request(req).await?).await?;

    println!("{:?}", res);

    Ok(())
}
