use std::sync::Arc;

pub fn start_worker(ctx: Arc<crate::BaseContext>) -> tokio::sync::mpsc::Sender<()> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    crate::spawn_task(run_worker(ctx, rx));

    tx
}

async fn run_worker(
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
                SELECT id FROM task WHERE state='pending' FOR UPDATE SKIP LOCKED LIMIT 1\
            ) RETURNING id, kind, params",
                &[],
            )
            .await?;

        if let Some(row) = row {
            let task_id: i64 = row.get(0);
            let kind: &str = row.get(1);
            let params: serde_json::Value = row.get(2);

            let result = perform_task(&ctx, kind, params).await;
            if let Err(err) = result {
                let err = format!("{:?}", err);
                db.execute("UPDATE task SET state=(CASE WHEN attempts + 1 < max_attempts THEN 'pending'::lt_task_state ELSE 'failed'::lt_task_state END), attempts = attempts + 1, latest_error=$2 WHERE id=$1", &[&task_id, &err]).await?;
            } else {
                db.execute("UPDATE task SET state='completed', completed_at=current_timestamp, attempts = attempts + 1 WHERE id=$1", &[&task_id]).await?;
            }
        } else {
            recv.recv()
                .await
                .expect("All task triggers have been dropped");
        }
    }
}

async fn perform_task(
    ctx: &crate::BaseContext,
    kind: &str,
    params: serde_json::Value,
) -> Result<(), crate::Error> {
    use crate::tasks::TaskDef;

    match kind {
        crate::tasks::DeliverToInbox::KIND => {
            let def: crate::tasks::DeliverToInbox = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::DeliverToFollowers::KIND => {
            let def: crate::tasks::DeliverToFollowers = serde_json::from_value(params)?;
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
