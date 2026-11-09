//! Best-effort cleanup for multipart futures dropped before completion.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use object_store::{MultipartUpload, PutPayload, PutResult, Result, UploadPart};
use pyo3_async_runtimes::tokio::get_runtime;

use tokio::sync::Notify;

const ABORT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Default)]
struct UploadState {
    active_parts: AtomicUsize,
    parts_drained: Notify,
    transfer_failed: AtomicBool,
}

impl UploadState {
    async fn wait_for_parts(&self) {
        while self.active_parts.load(Ordering::Acquire) != 0 {
            self.parts_drained.notified().await;
        }
    }
}

struct ActivePart {
    part: Option<UploadPart>,
    state: Arc<UploadState>,
}

impl Future for ActivePart {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self
            .part
            .as_mut()
            .expect("part future has not been dropped")
            .as_mut()
            .poll(context);
        if matches!(result, Poll::Ready(Err(_))) {
            self.state.transfer_failed.store(true, Ordering::Release);
        }
        result
    }
}

impl Drop for ActivePart {
    fn drop(&mut self) {
        // Release the request future before declaring this part drained, even
        // when its enclosing task was cancelled before the first poll.
        drop(self.part.take());
        if self.state.active_parts.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.state.parts_drained.notify_one();
        }
    }
}

/// Keep the native upload handle until completion or a successful abort. In
/// particular, cancelling the Python future must not just drop that handle.
#[derive(Debug)]
pub(crate) struct AbortOnDrop {
    upload: Option<Box<dyn MultipartUpload>>,
    timeout: Duration,
    state: Arc<UploadState>,
}

impl AbortOnDrop {
    pub(crate) fn new(upload: Box<dyn MultipartUpload>) -> Self {
        Self {
            upload: Some(upload),
            timeout: ABORT_TIMEOUT,
            state: Arc::new(UploadState::default()),
        }
    }
}

#[async_trait]
impl MultipartUpload for AbortOnDrop {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.state.active_parts.fetch_add(1, Ordering::AcqRel);
        let part = self
            .upload
            .as_mut()
            .expect("multipart upload has not finished")
            .put_part(data);
        Box::pin(ActivePart {
            part: Some(part),
            state: self.state.clone(),
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let result = self
            .upload
            .as_mut()
            .expect("multipart upload has not finished")
            .complete()
            .await;
        match result {
            Ok(result) => {
                self.upload = None;
                Ok(result)
            }
            Err(error) => {
                self.state.transfer_failed.store(true, Ordering::Release);
                Err(error)
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        let result = self
            .upload
            .as_mut()
            .expect("multipart upload has not finished")
            .abort()
            .await;
        if result.is_ok() {
            self.upload = None;
        } else if self.state.transfer_failed.load(Ordering::Acquire) {
            // WriteMultipart::finish propagates abort errors before its primary
            // transfer error. Keep that original error; Drop still retries cleanup.
            return Ok(());
        }
        result
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(mut upload) = self.upload.take() {
            let timeout = self.timeout;
            let state = self.state.clone();
            // A destructor cannot await. Keep cleanup on the native runtime,
            // independent of the cancelled Python future, and bound its lifetime.
            let runtime = tokio::runtime::Handle::try_current()
                .unwrap_or_else(|_| get_runtime().handle().clone());
            runtime.spawn(async move {
                let _ = tokio::time::timeout(timeout, async move {
                    // WriteMultipart drops its upload before its JoinSet. Wait
                    // until those part futures finish or drop before aborting.
                    state.wait_for_parts().await;
                    let _ = upload.abort().await;
                })
                .await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    #[derive(Debug, Default)]
    struct State {
        aborts: AtomicUsize,
        drops: AtomicUsize,
        completing: Notify,
        fail_complete: AtomicBool,
        fail_abort: AtomicBool,
        fail_part: AtomicBool,
        block_part: AtomicBool,
        part_started: Notify,
        part_drops: AtomicUsize,
        part_drops_at_abort: AtomicUsize,
    }

    struct Part(Arc<State>);

    impl Future for Part {
        type Output = Result<()>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            self.0.part_started.notify_one();
            if self.0.fail_part.load(Ordering::Acquire) {
                Poll::Ready(Err(object_store::Error::Generic {
                    store: "test",
                    source: "part failed".into(),
                }))
            } else if self.0.block_part.load(Ordering::Acquire) {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    impl Drop for Part {
        fn drop(&mut self) {
            self.0.part_drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct Upload {
        state: Arc<State>,
        block_complete: bool,
        block_abort: bool,
    }

    impl Drop for Upload {
        fn drop(&mut self) {
            self.state.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl MultipartUpload for Upload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(Part(self.state.clone()))
        }

        async fn complete(&mut self) -> Result<PutResult> {
            self.state.completing.notify_one();
            if self.block_complete {
                pending::<()>().await;
            }
            if self.state.fail_complete.load(Ordering::Acquire) {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: "complete failed".into(),
                });
            }
            Ok(PutResult {
                e_tag: None,
                version: None,
                extensions: Default::default(),
            })
        }

        async fn abort(&mut self) -> Result<()> {
            self.state.part_drops_at_abort.store(
                self.state.part_drops.load(Ordering::SeqCst),
                Ordering::SeqCst,
            );
            self.state.aborts.fetch_add(1, Ordering::SeqCst);
            if self.block_abort {
                pending::<()>().await;
            }
            if self.state.fail_abort.load(Ordering::Acquire) {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: "abort failed".into(),
                });
            }
            Ok(())
        }
    }

    async fn wait_for_drop(state: &State) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while state.drops.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup must release the upload handle");
    }

    fn upload(state: &Arc<State>, block_complete: bool, block_abort: bool) -> AbortOnDrop {
        AbortOnDrop::new(Box::new(Upload {
            state: state.clone(),
            block_complete,
            block_abort,
        }))
    }

    #[tokio::test]
    async fn dropped_upload_is_aborted() {
        let state = Arc::new(State::default());
        drop(upload(&state, false, false));
        wait_for_drop(&state).await;
        assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn completed_upload_is_not_aborted() {
        let state = Arc::new(State::default());
        let mut upload = upload(&state, false, false);
        upload.complete().await.unwrap();
        drop(upload);
        wait_for_drop(&state).await;
        assert_eq!(state.aborts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn explicitly_aborted_upload_is_not_aborted_twice() {
        let state = Arc::new(State::default());
        let mut upload = upload(&state, false, false);
        upload.abort().await.unwrap();
        drop(upload);
        wait_for_drop(&state).await;
        assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelling_completion_still_aborts() {
        let state = Arc::new(State::default());
        let mut writer = object_store::WriteMultipart::new(Box::new(upload(&state, true, false)));
        writer.put(bytes::Bytes::from_static(b"payload"));
        let task = tokio::spawn(writer.finish());
        tokio::time::timeout(Duration::from_secs(5), state.completing.notified())
            .await
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        wait_for_drop(&state).await;
        assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn detached_cleanup_is_bounded() {
        let state = Arc::new(State::default());
        let mut upload = upload(&state, false, true);
        upload.timeout = Duration::from_millis(10);
        drop(upload);
        wait_for_drop(&state).await;
        assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn dropped_upload_waits_for_unpolled_parts_before_aborting() {
        let state = Arc::new(State::default());
        let mut upload = upload(&state, false, false);
        let part = upload.put_part(bytes::Bytes::from_static(b"payload").into());
        drop(upload);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(state.aborts.load(Ordering::SeqCst), 0);
        drop(part);
        wait_for_drop(&state).await;
        assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_abort_does_not_replace_completion_error() {
        let state = Arc::new(State::default());
        state.fail_complete.store(true, Ordering::Release);
        state.fail_abort.store(true, Ordering::Release);
        let writer = object_store::WriteMultipart::new(Box::new(upload(&state, false, false)));
        let error = writer.finish().await.unwrap_err();
        assert!(error.to_string().contains("complete failed"));
        wait_for_drop(&state).await;
    }
    #[tokio::test]
    async fn cancelling_pending_parts_drops_requests_before_aborting() {
        let state = Arc::new(State::default());
        state.block_part.store(true, Ordering::Release);
        let mut writer = object_store::WriteMultipart::new_with_chunk_size(
            Box::new(upload(&state, false, false)),
            4,
        );
        writer.put(bytes::Bytes::from_static(b"data"));
        let task = tokio::spawn(writer.finish());
        tokio::time::timeout(Duration::from_secs(5), state.part_started.notified())
            .await
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        wait_for_drop(&state).await;
        assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(state.part_drops_at_abort.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_abort_does_not_replace_part_error() {
        let state = Arc::new(State::default());
        state.fail_part.store(true, Ordering::Release);
        state.fail_abort.store(true, Ordering::Release);
        let mut writer = object_store::WriteMultipart::new_with_chunk_size(
            Box::new(upload(&state, false, false)),
            4,
        );
        writer.put(bytes::Bytes::from_static(b"data"));
        let error = writer.finish().await.unwrap_err();
        assert!(error.to_string().contains("part failed"));
        wait_for_drop(&state).await;
    }
}
