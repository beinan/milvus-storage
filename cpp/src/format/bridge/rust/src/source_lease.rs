// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Runtime source leases. This cache is separate from the legacy broker policy.
//! Each instance belongs to exactly one context, authorized root and operation.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};

use crate::source_read_context::{OwnedSourceContext, SourceFailure, SourceOperation};

// No Debug implementation: a lease contains a credential.
pub(crate) struct SourceLease {
    pub(crate) token: String,
    pub(crate) expires_at: i64,
    pub(crate) generation: u64,
    refresh_at: i64,
}

#[async_trait]
trait Fetcher: Send + Sync {
    fn check(&self) -> Result<(), SourceFailure>;
    fn now(&self) -> i64;
    async fn fetch(&self) -> Result<(String, i64), SourceFailure>;
}

struct NativeFetcher {
    context: OwnedSourceContext,
    root: String,
    operation: SourceOperation,
}

#[async_trait]
impl Fetcher for NativeFetcher {
    fn check(&self) -> Result<(), SourceFailure> {
        self.context.check_state()
    }
    fn now(&self) -> i64 {
        self.context.now_unix_ms()
    }
    async fn fetch(&self) -> Result<(String, i64), SourceFailure> {
        let lease = self.context.fetch(&self.root, self.operation).await?;
        Ok((lease.sas_token, lease.expires_at_unix_ms))
    }
}

type Attempt = Shared<BoxFuture<'static, Result<Arc<SourceLease>, SourceFailure>>>;
#[derive(Default)]
struct State {
    lease: Option<Arc<SourceLease>>,
    refresh: Option<(u64, Attempt)>,
    generation: u64,
}

pub(crate) struct SourceLeaseCache {
    fetcher: Arc<dyn Fetcher>,
    state: Mutex<State>,
}

impl SourceLeaseCache {
    pub(crate) fn new(
        context: OwnedSourceContext,
        resource_uri: &str,
        operation: SourceOperation,
    ) -> Result<Self, SourceFailure> {
        let root = context.resolve(resource_uri)?.scope_root_uri;
        Ok(Self {
            fetcher: Arc::new(NativeFetcher {
                context,
                root,
                operation,
            }),
            state: Mutex::new(State::default()),
        })
    }

    pub(crate) async fn acquire(&self) -> Result<Arc<SourceLease>, SourceFailure> {
        self.fetcher.check()?;
        let (generation, attempt) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| SourceFailure::InvalidResponse)?;
            let now = self.fetcher.now();
            if let Some(lease) = &state.lease {
                if now < lease.refresh_at || (state.refresh.is_some() && now < lease.expires_at) {
                    self.fetcher.check()?;
                    return Ok(lease.clone());
                }
            }
            if let Some(attempt) = &state.refresh {
                attempt.clone()
            } else {
                state.generation = state
                    .generation
                    .checked_add(1)
                    .ok_or(SourceFailure::InvalidResponse)?;
                let generation = state.generation;
                let fallback = state.lease.clone();
                let fetcher = self.fetcher.clone();
                // The shared future owns the fetcher, not this cache: no cycle.
                // Dropping one caller leaves the same attempt available to the
                // remaining callers; an abandoned leader cannot strand waiters.
                let attempt = async move {
                    for retry in 0..4 {
                        fetcher.check()?;
                        let result = fetcher.fetch().await;
                        fetcher.check()?;
                        match result {
                            Ok((token, expires_at)) => {
                                let now = fetcher.now();
                                let remaining = expires_at
                                    .checked_sub(now)
                                    .ok_or(SourceFailure::InvalidResponse)?;
                                if remaining <= 0 {
                                    return Err(SourceFailure::Expired);
                                }
                                if token.is_empty() {
                                    return Err(SourceFailure::InvalidResponse);
                                }
                                return Ok(Arc::new(SourceLease {
                                    token,
                                    expires_at,
                                    generation,
                                    refresh_at: expires_at - (remaining / 2).min(60_000),
                                }));
                            }
                            Err(SourceFailure::Transient) => {
                                if let Some(lease) = &fallback {
                                    if lease.expires_at > fetcher.now() {
                                        return Ok(lease.clone());
                                    }
                                }
                                if retry == 3 {
                                    return Err(SourceFailure::Transient);
                                }
                                tokio::time::sleep(Duration::from_millis(100 << retry)).await;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    unreachable!()
                }
                .boxed()
                .shared();
                state.refresh = Some((generation, attempt.clone()));
                (generation, attempt)
            }
        };
        // Waiting for another caller's blocking native callback must remain
        // cancellable, including during retry backoff. Native callback ownership
        // remains retained by spawn_blocking until its own deadline is honored.
        let result = loop {
            self.fetcher.check()?;
            tokio::select! {
                result = attempt.clone() => break result,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        };
        self.fetcher.check()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| SourceFailure::InvalidResponse)?;
        let now = self.fetcher.now();
        if state
            .refresh
            .as_ref()
            .is_some_and(|(id, _)| *id == generation)
        {
            match &result {
                Ok(lease) if lease.expires_at > now => state.lease = Some(lease.clone()),
                Err(SourceFailure::Transient) => {}
                _ => state.lease = None,
            }
            state.refresh = None;
        }
        self.fetcher.check()?;
        match result {
            Ok(_) => {
                if let Some(lease) = &state.lease {
                    if lease.expires_at > now {
                        return Ok(lease.clone());
                    }
                }
                Err(SourceFailure::Expired)
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    struct MockFetcher {
        now: AtomicI64,
        state: AtomicUsize,
        calls: AtomicUsize,
        responses: Mutex<VecDeque<Result<(String, i64), SourceFailure>>>,
        gate: Semaphore,
    }
    #[async_trait]
    impl Fetcher for MockFetcher {
        fn check(&self) -> Result<(), SourceFailure> {
            SourceFailure::from_status(self.state.load(Ordering::SeqCst) as u32)
        }
        fn now(&self) -> i64 {
            self.now.load(Ordering::SeqCst)
        }
        async fn fetch(&self) -> Result<(String, i64), SourceFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.gate.acquire().await.unwrap().forget();
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra acquisition")
        }
    }
    fn setup(
        responses: Vec<Result<(String, i64), SourceFailure>>,
        permits: usize,
    ) -> (Arc<MockFetcher>, Arc<SourceLeaseCache>) {
        let fetcher = Arc::new(MockFetcher {
            now: AtomicI64::new(0),
            state: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            responses: Mutex::new(responses.into()),
            gate: Semaphore::new(permits),
        });
        let cache = Arc::new(SourceLeaseCache {
            fetcher: fetcher.clone(),
            state: Mutex::new(State::default()),
        });
        (fetcher, cache)
    }
    fn lease(token: &str, expires: i64) -> Result<(String, i64), SourceFailure> {
        Ok((token.to_string(), expires))
    }
    async fn started(fetcher: &MockFetcher, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while fetcher.calls.load(Ordering::SeqCst) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn short_leases_rotate_without_refetching_every_read() {
        let (fetcher, cache) = setup(vec![lease("one", 120), lease("two", 240)], 2);
        assert_eq!(cache.acquire().await.unwrap().token, "one");
        fetcher.now.store(59, Ordering::SeqCst);
        assert_eq!(cache.acquire().await.unwrap().token, "one");
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
        fetcher.now.store(60, Ordering::SeqCst);
        assert_eq!(cache.acquire().await.unwrap().token, "two");
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn transient_outage_uses_only_valid_lease_then_recovers() {
        let (fetcher, cache) = setup(
            vec![
                lease("one", 120),
                Err(SourceFailure::Transient),
                Err(SourceFailure::Transient),
                Err(SourceFailure::Transient),
                Err(SourceFailure::Transient),
                Err(SourceFailure::Transient),
                lease("two", 300),
            ],
            7,
        );
        cache.acquire().await.unwrap();
        fetcher.now.store(60, Ordering::SeqCst);
        assert_eq!(cache.acquire().await.unwrap().token, "one");
        fetcher.now.store(120, Ordering::SeqCst);
        assert_eq!(cache.acquire().await.err(), Some(SourceFailure::Transient));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 6);
        assert_eq!(cache.acquire().await.unwrap().token, "two");
    }

    #[tokio::test]
    async fn concurrent_callers_share_one_failed_batch_and_later_recover() {
        for expired in [false, true] {
            let mut replies = if expired {
                vec![lease("old", 120)]
            } else {
                vec![]
            };
            replies.extend(vec![Err(SourceFailure::Transient); 4]);
            replies.push(lease("new", 400));
            let (fetcher, cache) = setup(replies, usize::from(expired));
            if expired {
                cache.acquire().await.unwrap();
                fetcher.now.store(120, Ordering::SeqCst);
            }
            let calls_before = fetcher.calls.load(Ordering::SeqCst);
            // Poll every future before allowing the first acquisition to finish.
            let futures = (0..8).map(|_| cache.acquire()).collect::<Vec<_>>();
            let worker = tokio::spawn(async move {
                started(&fetcher, calls_before + 1).await;
                fetcher.gate.add_permits(4);
                fetcher
            });
            let results = futures::future::join_all(futures).await;
            let fetcher = worker.await.unwrap();
            assert!(
                results
                    .into_iter()
                    .all(|r| r.err() == Some(SourceFailure::Transient))
            );
            assert_eq!(fetcher.calls.load(Ordering::SeqCst), calls_before + 4);
            fetcher.gate.add_permits(1);
            assert_eq!(cache.acquire().await.unwrap().token, "new");
        }
    }

    #[tokio::test]
    async fn dropped_leader_does_not_strand_shared_refresh() {
        let (fetcher, cache) = setup(vec![lease("one", 120)], 0);
        let task = tokio::spawn({
            let cache = cache.clone();
            async move { cache.acquire().await }
        });
        started(&fetcher, 1).await;
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        fetcher.gate.add_permits(1);
        let lease = tokio::time::timeout(Duration::from_secs(1), cache.acquire())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.token, "one");
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancellation_releases_waiters_without_waiting_for_provider() {
        let (fetcher, cache) = setup(vec![lease("one", 120)], 0);
        let task = tokio::spawn({
            let cache = cache.clone();
            async move { cache.acquire().await }
        });
        started(&fetcher, 1).await;
        fetcher.state.store(4, Ordering::SeqCst);
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(200), task)
                .await
                .unwrap()
                .unwrap()
                .err(),
            Some(SourceFailure::Cancelled)
        );
        assert_eq!(cache.acquire().await.err(), Some(SourceFailure::Cancelled));
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn denied_and_invalid_refreshes_clear_old_lease() {
        for failure in [
            SourceFailure::Denied,
            SourceFailure::InvalidResponse,
            SourceFailure::Expired,
        ] {
            let (fetcher, cache) = setup(
                vec![
                    lease("one", 120),
                    Err(failure),
                    Err(SourceFailure::Transient),
                    Err(SourceFailure::Transient),
                    Err(SourceFailure::Transient),
                    Err(SourceFailure::Transient),
                ],
                6,
            );
            cache.acquire().await.unwrap();
            fetcher.now.store(60, Ordering::SeqCst);
            assert_eq!(cache.acquire().await.err(), Some(failure));
            assert_eq!(cache.acquire().await.err(), Some(SourceFailure::Transient));
        }
    }

    #[tokio::test]
    async fn valid_cached_read_does_not_wait_for_refresh() {
        let (fetcher, cache) = setup(vec![lease("one", 120), lease("two", 240)], 1);
        cache.acquire().await.unwrap();
        fetcher.now.store(60, Ordering::SeqCst);
        let task = tokio::spawn({
            let cache = cache.clone();
            async move { cache.acquire().await }
        });
        started(&fetcher, 2).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(50), cache.acquire())
                .await
                .unwrap()
                .unwrap()
                .token,
            "one"
        );
        fetcher.gate.add_permits(1);
        assert_eq!(task.await.unwrap().unwrap().token, "two");
    }
}
