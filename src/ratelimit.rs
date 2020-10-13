use std::sync::atomic::AtomicU16;

pub struct RatelimitBucket<K> {
    cap: u16,
    inner: tokio::sync::RwLock<Inner<K>>,
}

impl<K: Eq + std::hash::Hash + std::fmt::Debug> RatelimitBucket<K> {
    pub fn new(cap: u16) -> Self {
        Self {
            cap,
            inner: tokio::sync::RwLock::new(Inner {
                divider_time: std::time::Instant::now(),
                last_minute: None,
                current_minute: dashmap::DashMap::new(),
            }),
        }
    }

    pub async fn try_call(&self, key: K) -> bool {
        let now = std::time::Instant::now();
        let inner = self.inner.read().await;
        let seconds_into = now.duration_since(inner.divider_time).as_secs();
        if seconds_into >= 60 {
            std::mem::drop(inner);
            let mut inner = self.inner.write().await;

            let seconds_into_new = now.duration_since(inner.divider_time).as_secs();

            // check again
            if seconds_into_new >= 120 {
                // more than two minutes elapsed, reset
                inner.last_minute = None;
                inner.current_minute = dashmap::DashMap::new();
                inner.divider_time = now;

                self.try_for_current(0, &inner, key).await
            } else if seconds_into_new >= 60 {
                let mut tmp = dashmap::DashMap::new();
                std::mem::swap(&mut tmp, &mut inner.current_minute);
                inner.last_minute = Some(tmp.into_read_only());
                inner.divider_time += std::time::Duration::new(60, 0);

                self.try_for_current(seconds_into_new - 60, &inner, key)
                    .await
            } else {
                self.try_for_current(seconds_into_new, &inner, key).await
            }
        } else {
            self.try_for_current(seconds_into, &inner, key).await
        }
    }

    async fn try_for_current(&self, seconds_into: u64, inner: &Inner<K>, key: K) -> bool {
        let prev_count = if let Some(last_minute) = &inner.last_minute {
            if let Some(prev_count) = last_minute.get(&key) {
                (u64::from(prev_count.load(std::sync::atomic::Ordering::Relaxed))
                    * (60 - seconds_into)
                    / 60) as u16
            } else {
                0
            }
        } else {
            0
        };

        let count = prev_count
            + inner
                .current_minute
                .entry(key)
                .or_default()
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        count < self.cap
    }
}

struct Inner<K> {
    divider_time: std::time::Instant,
    last_minute: Option<dashmap::ReadOnlyView<K, AtomicU16>>,
    current_minute: dashmap::DashMap<K, AtomicU16>,
}
