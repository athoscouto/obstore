# Vendored object_store 0.14.2

This directory contains the checksum-verified crates.io `object_store` 0.14.2 source archive, with private S3 file-streaming and server-side copy patches. The archive SHA-256 is `f1796bc93603f78c5760a69f2d58badc9618d22adade0a95385bb2adbae4eb94`.

The source archive is available from `https://static.crates.io/crates/object_store/object_store-0.14.2.crate`. Cargo resolves the workspace dependency through the root `[patch.crates-io]` entry. The vendored crate is excluded from workspace membership, so it does not add another workspace package. `object_store-0.14.2.patch` is the minimized unified source diff from the pristine crate.

The streaming changes are limited to replayable HTTP request bodies, the S3 SHA256 trailer path, the regular-file producer, and the hidden `AmazonS3::{supports_file_stream, put_file_stream_opts}` entry points. To reproduce the complete source diff against the pristine archive, extract that exact archive to a temporary directory and run:

```sh
diff -ru --exclude=.cargo-ok --exclude=Cargo.lock --exclude=target /tmp/object_store-0.14.2 vendor/object_store-0.14.2
```

Run the dependency's focused tests using its independent lockfile with `cargo test --manifest-path vendor/object_store-0.14.2/Cargo.toml --locked --features aws client::http::`.

The copy changes support replacement custom metadata and a hidden `AmazonS3::copy_from` entry point. Cross-store copies apply each store's prefix independently and use the destination's transport and credentials. They use a single `CopyObject` request (up to 5 GB), preserve the source version when specified, and return the destination ETag and version. Different effective service endpoints (including regional endpoint aliases) and SSE-C are rejected; copying between separately configured services is not emulated by downloading and uploading.

The stream pins the opened file descriptor, checks length and modification time at attempt boundaries, reads bounded chunks on `spawn_blocking`, reconstructs a producer for retries, and releases its file descriptor when the operation is cancelled. These checks do not provide an immutable snapshot; the caller must keep the source unchanged until completion. The capability method is the same guard used by the upload method; callers should decide before issuing a request and use their buffered path when it returns false. A network failure is returned as-is and does not trigger a buffered retry.
