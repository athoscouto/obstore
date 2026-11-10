//! Private S3 streaming-upload source. A retry reconstructs the producer.

use std::fs::{File, Metadata};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use base64::Engine;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{stream, StreamExt};

use crate::client::crypto::{crypto_provider, CryptoProvider, DigestAlgorithm, DigestContext};
use crate::client::{HttpError, HttpErrorKind};

/// A fixed, caller-owned file. The caller must not modify it during an upload/retry.
#[derive(Debug)]
pub(crate) struct StreamingFile {
    file: Mutex<Option<Arc<File>>>,
    length: u64,
    modified: Option<SystemTime>,
    chunk_size: usize,
    crypto: Option<Arc<dyn CryptoProvider>>,
    encoded_length: usize,
}

pub(crate) struct CancelStreamingFile(Arc<StreamingFile>);

impl Drop for CancelStreamingFile {
    fn drop(&mut self) {
        self.0
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

struct State {
    source: Arc<StreamingFile>,
    offset: u64,
    digest: Box<dyn DigestContext>,
    complete: bool,
}

fn error(e: impl std::error::Error + Send + Sync + 'static) -> HttpError {
    HttpError::new(HttpErrorKind::Unknown, e)
}

fn invalid(message: &'static str) -> HttpError {
    error(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn trailer(digest: &[u8]) -> Bytes {
    Bytes::from(format!(
        "0\r\nx-amz-checksum-sha256:{}\r\n\r\n",
        base64::prelude::BASE64_STANDARD.encode(digest)
    ))
}

fn encoded_length(length: u64, chunk_size: usize) -> io::Result<usize> {
    let encoded = usize::try_from(length).ok().and_then(|length| {
        let full_chunks = length / chunk_size;
        let remaining = length % chunk_size;
        let full_frame = chunk_size + format!("{chunk_size:x}").len() + 4;
        let last_frame = if remaining == 0 {
            0
        } else {
            remaining + format!("{remaining:x}").len() + 4
        };
        full_chunks
            .checked_mul(full_frame)
            .and_then(|total| total.checked_add(last_frame))
            .and_then(|total| total.checked_add(trailer(&[0; 32]).len()))
    });
    encoded.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "encoded upload length exceeds platform size",
        )
    })
}

impl StreamingFile {
    pub(crate) fn new(
        file: File,
        length: u64,
        chunk_size: usize,
        crypto: Option<Arc<dyn CryptoProvider>>,
    ) -> io::Result<Self> {
        if chunk_size == 0 || chunk_size > 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "S3 streaming supports chunks in 1..=1 MiB",
            ));
        }
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "S3 streaming requires a regular file with the declared length",
            ));
        }
        Ok(Self {
            file: Mutex::new(Some(Arc::new(file))),
            length,
            modified: metadata.modified().ok(),
            chunk_size,
            crypto,
            encoded_length: encoded_length(length, chunk_size)?,
        })
    }

    pub(crate) fn length(&self) -> usize {
        self.encoded_length
    }

    /// Own this guard for the whole upload, including every retry. A transport
    /// may retain the stream after the request future is dropped; it must not
    /// retain the source descriptor or start another read after cancellation.
    pub(crate) fn cancel_on_drop(self: &Arc<Self>) -> CancelStreamingFile {
        CancelStreamingFile(self.clone())
    }

    fn file_handle(&self) -> Result<Arc<File>, HttpError> {
        self.file
            .lock()
            .map_err(|_| invalid("streaming file lock poisoned"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                error(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "streaming upload cancelled",
                ))
            })
    }

    fn validate(&self, metadata: &Metadata) -> Result<(), HttpError> {
        if metadata.len() != self.length || metadata.modified().ok() != self.modified {
            return Err(invalid("source file changed during streaming upload"));
        }
        Ok(())
    }

    pub(crate) fn stream(self: &Arc<Self>) -> BoxStream<'static, Result<Bytes, HttpError>> {
        let digest = match crypto_provider(self.crypto.as_deref())
            .and_then(|provider| provider.digest(DigestAlgorithm::Sha256))
        {
            Ok(digest) => digest,
            Err(e) => return stream::once(async { Err(error(e)) }).boxed(),
        };
        let state = State {
            source: self.clone(),
            offset: 0,
            digest,
            complete: false,
        };
        stream::try_unfold(state, |state| async move {
            if state.complete {
                return Ok(None);
            }
            tokio::task::spawn_blocking(move || {
                let mut state = state;
                let source = &state.source;
                // The job owns only its current chunk's handle. Queued jobs
                // acquire no handle if cancellation already closed the source.
                let file = source.file_handle()?;
                if state.offset == 0 || state.offset == source.length {
                    source.validate(&file.metadata().map_err(error)?)?;
                }
                if state.offset == source.length {
                    let chunk = trailer(state.digest.finish().map_err(error)?);
                    state.complete = true;
                    return Ok(Some((chunk, state)));
                }
                let size = (source.length - state.offset).min(source.chunk_size as u64) as usize;
                let prefix = format!("{size:x}\r\n");
                let start = prefix.len();
                let mut chunk = Vec::with_capacity(start + size + 2);
                chunk.extend_from_slice(prefix.as_bytes());
                chunk.resize(start + size, 0);
                let mut read = 0;
                while read < size {
                    let count = read_at(
                        &file,
                        &mut chunk[start + read..],
                        state.offset + read as u64,
                    )
                    .map_err(error)?;
                    if count == 0 {
                        return Err(invalid("source file ended before declared length"));
                    }
                    read += count;
                }
                state.digest.update(&chunk[start..]);
                chunk.extend_from_slice(b"\r\n");
                state.offset += size as u64;
                Ok(Some((Bytes::from(chunk), state)))
            })
            .await
            .map_err(error)?
        })
        .boxed()
    }
}

#[cfg(unix)]
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buffer, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buffer, offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use std::io::Write;

    #[test]
    fn encoded_length_rejects_overflow() {
        assert!(encoded_length(u64::MAX, 1).is_err());
    }

    #[tokio::test]
    async fn files_above_32_mib_keep_bounded_frames() {
        let file = tempfile::tempfile().unwrap();
        let length = 33 * 1024 * 1024;
        file.set_len(length).unwrap();
        let source = Arc::new(StreamingFile::new(file, length, 1024 * 1024, None).unwrap());
        let mut stream = source.stream();
        let mut total = 0;
        let mut frames = 0;
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            assert!(frame.len() <= 1024 * 1024 + 10);
            total += frame.len();
            frames += 1;
        }
        assert_eq!(frames, 34); // 33 data frames and the SHA256 trailer.
        assert_eq!(total, source.length());
    }

    #[tokio::test]
    async fn checksum_framing_and_retry_restart_are_identical() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"abcde").unwrap();
        let source = Arc::new(StreamingFile::new(file, 5, 3, None).unwrap());
        let mut partial = source.stream();
        assert_eq!(partial.next().await.unwrap().unwrap(), "3\r\nabc\r\n");
        drop(partial);
        let chunks = source.stream().try_collect::<Vec<_>>().await.unwrap();
        let wire = chunks.concat();
        assert_eq!(wire.len(), source.length());
        assert_eq!(wire, b"3\r\nabc\r\n2\r\nde\r\n0\r\nx-amz-checksum-sha256:NrvlDtloQdEEQ7y2cNZVTwo0t2G+Z+ycSorSwMRMpCw=\r\n\r\n");
    }

    #[tokio::test]
    async fn empty_file_emits_sha256_trailer_and_replay() {
        let source =
            Arc::new(StreamingFile::new(tempfile::tempfile().unwrap(), 0, 1024, None).unwrap());
        for _ in 0..2 {
            let chunks = source.stream().try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(
                chunks.concat(),
                b"0\r\nx-amz-checksum-sha256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\r\n\r\n"
            );
        }
    }

    #[tokio::test]
    async fn cancellation_releases_file_while_transport_keeps_partial_stream() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"abcde").unwrap();
        let source = Arc::new(StreamingFile::new(file, 5, 3, None).unwrap());
        let weak_file = Arc::downgrade(&source.file_handle().unwrap());
        let guard = source.cancel_on_drop();
        let mut stalled_transport = source.stream();
        assert_eq!(
            stalled_transport.next().await.unwrap().unwrap(),
            "3\r\nabc\r\n"
        );
        assert!(weak_file.upgrade().is_some());

        drop(guard);

        // Keeping the stream/source alive must not keep the descriptor alive.
        assert!(weak_file.upgrade().is_none());
        assert!(stalled_transport.next().await.unwrap().is_err());
        assert!(source.stream().next().await.unwrap().is_err());
    }

    #[tokio::test]
    async fn cancelling_before_poll_closes_file_and_prevents_first_read() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"abc").unwrap();
        let source = Arc::new(StreamingFile::new(file, 3, 2, None).unwrap());
        let weak_file = Arc::downgrade(&source.file_handle().unwrap());
        let stream = source.stream();
        drop(source.cancel_on_drop());

        assert!(weak_file.upgrade().is_none());
        assert!(stream.try_collect::<Vec<_>>().await.is_err());
    }

    #[tokio::test]
    async fn changed_file_is_rejected_on_replay() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"abc").unwrap();
        let writer = file.try_clone().unwrap();
        let source = Arc::new(StreamingFile::new(file, 3, 2, None).unwrap());
        writer.set_len(1).unwrap();
        assert!(source.stream().try_collect::<Vec<_>>().await.is_err());
    }
}
