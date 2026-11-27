use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

const CANCEL_POLL: Duration = Duration::from_millis(25);
use std::thread;

struct State<T, E> {
    next_job: usize,
    next_output: usize,
    ready: BTreeMap<usize, T>,
    failure: Option<E>,
    aborted: bool,
}

struct Shared<T, E> {
    state: Mutex<State<T, E>>,
    changed: Condvar,
}

impl<T, E> Shared<T, E> {
    fn lock(&self) -> MutexGuard<'_, State<T, E>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn abort(&self, failure: Option<E>) {
        let mut state = self.lock();
        if state.failure.is_none() {
            state.failure = failure;
        }
        state.aborted = true;
        self.changed.notify_all();
    }
}

struct AbortOnPanic<'a, T, E>(&'a Shared<T, E>);

impl<T, E> Drop for AbortOnPanic<'_, T, E> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.0.abort(None);
        }
    }
}

pub(crate) fn ordered<T: Send, E: Send>(
    jobs: usize,
    workers: usize,
    window: usize,
    cancel: &AtomicBool,
    cancelled: impl Fn() -> E,
    produce: impl Fn(usize) -> Result<T, E> + Sync,
    mut consume: impl FnMut(T) -> Result<(), E>,
) -> Result<(), E> {
    let shared = Shared {
        state: Mutex::new(State {
            next_job: 0,
            next_output: 0,
            ready: BTreeMap::new(),
            failure: None,
            aborted: false,
        }),
        changed: Condvar::new(),
    };
    let shared = &shared;
    let produce = &produce;
    thread::scope(|scope| {
        for _ in 0..workers.min(jobs).max(1) {
            scope.spawn(move || {
                let _guard = AbortOnPanic(shared);
                loop {
                    let job = {
                        let mut state = shared.lock();
                        loop {
                            if state.aborted
                                || state.next_job >= jobs
                                || cancel.load(Ordering::Relaxed)
                            {
                                return;
                            }
                            if state.next_job < state.next_output + window {
                                break;
                            }
                            state = shared
                                .changed
                                .wait(state)
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                        }
                        state.next_job += 1;
                        state.next_job - 1
                    };
                    match produce(job) {
                        Ok(value) => {
                            shared.lock().ready.insert(job, value);
                            shared.changed.notify_all();
                        }
                        Err(error) => {
                            shared.abort(Some(error));
                            return;
                        }
                    }
                }
            });
        }
        let _guard = AbortOnPanic(shared);
        for output in 0..jobs {
            let value = {
                let mut state = shared.lock();
                loop {
                    if state.aborted {
                        break None;
                    }
                    if cancel.load(Ordering::Relaxed) {
                        state.failure.get_or_insert_with(&cancelled);
                        state.aborted = true;
                        shared.changed.notify_all();
                        break None;
                    }
                    if let Some(value) = state.ready.remove(&output) {
                        break Some(value);
                    }
                    state = shared
                        .changed
                        .wait_timeout(state, CANCEL_POLL)
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .0;
                }
            };
            let Some(value) = value else { break };
            if let Err(error) = consume(value) {
                shared.abort(Some(error));
                break;
            }
            shared.lock().next_output = output + 1;
            shared.changed.notify_all();
        }
    });
    match shared.lock().failure.take() {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_are_consumed_in_job_order_whatever_the_worker_count() {
        for workers in [1, 3, 16] {
            let mut seen = Vec::new();
            ordered(
                50,
                workers,
                4,
                &AtomicBool::new(false),
                || (),
                |job| {
                    if job % 7 == 0 {
                        thread::sleep(std::time::Duration::from_millis(2));
                    }
                    Ok::<_, ()>(job)
                },
                |value| {
                    seen.push(value);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(seen, (0..50).collect::<Vec<_>>());
        }
    }

    #[test]
    fn the_first_producer_failure_stops_the_run() {
        let result = ordered(
            100,
            4,
            4,
            &AtomicBool::new(false),
            || "cancelled",
            |job| if job == 10 { Err("boom") } else { Ok(job) },
            |_| Ok(()),
        );
        assert_eq!(result, Err("boom"));
    }

    #[test]
    fn a_consumer_failure_stops_the_run() {
        let result = ordered(
            100,
            4,
            4,
            &AtomicBool::new(false),
            || "cancelled",
            Ok::<_, &str>,
            |_| Err("full"),
        );
        assert_eq!(result, Err("full"));
    }

    #[test]
    fn a_set_cancel_flag_stops_the_run() {
        let result = ordered(
            100,
            2,
            2,
            &AtomicBool::new(true),
            || "cancelled",
            Ok::<_, &str>,
            |_| Ok(()),
        );
        assert_eq!(result, Err("cancelled"));
    }

    #[test]
    fn cancellation_is_seen_before_the_next_ready_result_is_consumed() {
        let cancel = AtomicBool::new(false);
        let produced = std::sync::atomic::AtomicUsize::new(0);
        let mut consumed = 0;
        let result = ordered(
            10_000,
            4,
            10_000,
            &cancel,
            || "cancelled",
            |job| {
                produced.fetch_add(1, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(1));
                Ok::<_, &str>(job)
            },
            |_| {
                consumed += 1;
                cancel.store(true, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(50));
                Ok(())
            },
        );
        assert_eq!(result, Err("cancelled"));
        assert_eq!(consumed, 1);
        assert!(produced.load(Ordering::Relaxed) < 10_000);
    }
}
