use futures::FutureExt;
use std::sync::Arc;

const TASK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

pub async fn run_worker(
    ctx: Arc<crate::BaseContext>,
    recv: tokio::sync::mpsc::Receiver<()>,
) -> Result<(), crate::Error> {
    futures::try_join!(run_task_runner(ctx.clone(), recv), run_schedule(ctx),)?;

    Ok(())
}

async fn run_task_runner(
    ctx: Arc<crate::BaseContext>,
    mut recv: tokio::sync::mpsc::Receiver<()>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    // TODO allow disabling this so multiple workers can run
    db.execute(
        "UPDATE task SET state='pending', attempts = attempts + 1 WHERE state='running'",
        &[],
    )
    .await?;

    // TODO consider running tasks in parallel
    loop {
        let row = db
            .query_opt(
                "UPDATE task SET state='running' WHERE id=(\
                    SELECT id FROM task \
                        WHERE state='pending' \
                        AND (attempted_at IS NULL OR attempted_at + (EXP(attempts) * INTERVAL '20 SECONDS') < current_timestamp) \
                        FOR UPDATE SKIP LOCKED LIMIT 1\
                    ) RETURNING id, kind, params",
                &[],
            )
            .await?;

        if let Some(row) = row {
            let task_id: i64 = row.get(0);
            let kind: &str = row.get(1);
            let params: serde_json::Value = row.get(2);

            let result =
                tokio::time::timeout(TASK_TIMEOUT, perform_task(ctx.clone(), kind, params)).await;
            let result = match result {
                Err(_) => Err(crate::Error::InternalStrStatic("Timeout")),
                Ok(res) => res,
            };

            if let Err(err) = result {
                let err = format!("{:?}", err);
                db.execute(
                    "UPDATE task \
                        SET state=(CASE WHEN attempts + 1 < max_attempts THEN 'pending'::lt_task_state ELSE 'failed'::lt_task_state END), attempts = attempts + 1, latest_error=$2, attempted_at=current_timestamp \
                        WHERE id=$1",
                    &[&task_id, &err],
                ).await?;
            } else {
                db.execute("UPDATE task SET state='completed', completed_at=current_timestamp, attempts = attempts + 1 WHERE id=$1", &[&task_id]).await?;
            }
        } else {
            match tokio::time::timeout(std::time::Duration::from_secs(60), recv.recv()).await {
                Err(tokio::time::error::Elapsed { .. }) => {}
                Ok(recv_res) => recv_res.ok_or(crate::Error::InternalStrStatic(
                    "Worker trigger senders lost",
                ))?,
            }
        }
    }
}

async fn run_schedule(ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
    let scheduler = tokio_cron_scheduler::JobScheduler::new().await?;

    scheduler.add(
        tokio_cron_scheduler::Job::new_async("22 23 22 * * *", {
            let ctx = ctx.clone();
            move |_, _| {
                let ctx = ctx.clone();
                Box::pin(async move {
                    let db = ctx.db_pool.get().await?;

                    let res = db.execute(
                        "DELETE FROM task WHERE (state='completed' AND completed_at < current_timestamp - INTERVAL '1 MONTH') OR (state='failed' AND attempted_at < current_timestamp - INTERVAL '3 MONTHS')",
                        &[],
                    ).await?;

                    if res > 0 {
                        log::debug!("Cleaned up {} old tasks", res);
                    }

                    Result::<_, crate::Error>::Ok(())
                }.map(|res| {
                    if let Err(err) = res {
                        log::error!("Failed to clean up old tasks: {:?}", err);
                    }
                }))
            }
        })?,
    ).await?;

    Ok(scheduler.start().await?)
}

async fn perform_task(
    ctx: Arc<crate::BaseContext>,
    kind: &str,
    params: serde_json::Value,
) -> Result<(), crate::Error> {
    use crate::tasks::TaskDef;

    match kind {
        crate::tasks::DeliverToInbox::KIND => {
            let def: crate::tasks::DeliverToInbox = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        #[allow(deprecated)]
        crate::tasks::DeliverToFollowers::KIND => {
            let def: crate::tasks::DeliverToFollowers = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::DeliverToAudience::KIND => {
            let def: crate::tasks::DeliverToAudience = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::FetchActor::KIND => {
            let def: crate::tasks::FetchActor = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::FetchCommunityFeatured::KIND => {
            let def: crate::tasks::FetchCommunityFeatured = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::SendNotification::KIND => {
            let def: crate::tasks::SendNotification = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::SendNotificationForSubscription::KIND => {
            let def: crate::tasks::SendNotificationForSubscription =
                serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::IngestObjectFromInbox::KIND => {
            let def: crate::tasks::IngestObjectFromInbox = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        _ => {
            return Err(crate::Error::InternalStr(format!(
                "Unrecognized task type: {}",
                kind
            )))
        }
    }

    Ok(())
}
