"""Local HTTPS proxy receiver for public obstore upload smoke checks."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import shutil
import socketserver
import ssl
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import io
    import socket

    from obstore import Attributes
    from obstore.store import ClientConfig, S3RequestMetrics


class _Receiver(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cert: Path, key: Path) -> None:
        self.payloads: list[bytes] = []
        self.requests: list[tuple[bytes, dict[bytes, bytes]]] = []
        self.trailers: list[dict[bytes, bytes]] = []
        self.errors: list[Exception] = []
        self.tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.tls.load_cert_chain(str(cert), str(key))
        super().__init__(("127.0.0.1", 0), _Handler)


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        receiver: _Receiver = self.server  # type: ignore[assignment]
        try:
            connection, request = _tunnel(self.request, receiver.tls)
            verb, headers, wire_body = _read_request(request)
            receiver.requests.append((verb, headers))
            body, trailers = (
                _decode_aws_chunked(wire_body)
                if b"aws-chunked" in headers.get(b"content-encoding", b"").lower()
                else (wire_body, {})
            )
            receiver.payloads.append(body)
            receiver.trailers.append(trailers)
            connection.sendall(
                b'HTTP/1.1 200 OK\r\nETag: "0123456789abcdef0123456789abcdef"\r\n'
                b"Content-Length: 0\r\nConnection: close\r\n\r\n",
            )
            connection.close()
        except (AssertionError, OSError, ValueError, ssl.SSLError) as error:
            receiver.errors.append(error)


def _tunnel(
    proxy: socket.socket,
    tls: ssl.SSLContext,
) -> tuple[socket.socket, io.BufferedIOBase]:
    request = proxy.makefile("rb")
    connect_line = request.readline()
    if not connect_line.startswith(b"CONNECT "):
        raise AssertionError("expected an HTTP CONNECT request")
    while request.readline() not in (b"\r\n", b"\n", b""):
        pass
    proxy.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
    connection = tls.wrap_socket(proxy, server_side=True)
    connection.settimeout(15)
    return connection, connection.makefile("rwb")


def _read_request(stream: io.BufferedIOBase) -> tuple[bytes, dict[bytes, bytes], bytes]:
    request_line = stream.readline()
    if not request_line.startswith(b"PUT "):
        raise AssertionError("expected a PUT request")
    headers = _read_headers(stream)
    if headers.get(b"expect", b"").lower() == b"100-continue":
        stream.write(b"HTTP/1.1 100 Continue\r\n\r\n")
        stream.flush()
    body = (
        _read_http_chunks(stream)
        if headers.get(b"transfer-encoding", b"").lower() == b"chunked"
        else _read_length(stream, int(headers.get(b"content-length", b"0")))
    )
    return request_line.strip(), headers, body


def _read_headers(stream: io.BufferedIOBase) -> dict[bytes, bytes]:
    headers: dict[bytes, bytes] = {}
    while True:
        line = stream.readline()
        if line in (b"\r\n", b"\n", b""):
            return headers
        name, value = line.split(b":", 1)
        headers[name.strip().lower()] = value.strip()


def _read_http_chunks(stream: io.BufferedIOBase) -> bytes:
    body = bytearray()
    while True:
        size = int(stream.readline().split(b";", 1)[0].strip(), 16)
        if size == 0:
            while stream.readline() not in (b"\r\n", b"\n", b""):
                pass
            return bytes(body)
        body.extend(_read_length(stream, size))
        stream.read(2)


def _read_length(stream: io.BufferedIOBase, length: int) -> bytes:
    body = bytearray()
    while len(body) < length:
        piece = stream.read(min(65536, length - len(body)))
        if not piece:
            raise AssertionError("request body ended early")
        body.extend(piece)
    return bytes(body)


def _decode_aws_chunked(wire: bytes) -> tuple[bytes, dict[bytes, bytes]]:
    payload = bytearray()
    trailers: dict[bytes, bytes] = {}
    offset = 0
    while True:
        end = wire.index(b"\r\n", offset)
        size = int(wire[offset:end].split(b";", 1)[0], 16)
        offset = end + 2
        if size == 0:
            while wire[offset:] not in (b"\r\n", b""):
                end = wire.index(b"\r\n", offset)
                name, value = wire[offset:end].split(b":", 1)
                trailers[name.strip().lower()] = value.strip()
                offset = end + 2
            return bytes(payload), trailers
        payload.extend(wire[offset : offset + size])
        offset += size + 2


def _create_cert(root: Path) -> tuple[Path, Path, Path]:
    openssl = shutil.which("openssl")
    if openssl is None:
        raise RuntimeError("OpenSSL is required for the local TLS smoke receiver")
    ca_cert, ca_key = root / "ca.pem", root / "ca-key.pem"
    cert, key = root / "cert.pem", root / "key.pem"
    # LibreSSL appends -addext values to ambient x509_extensions, producing
    # duplicate OIDs. Keep the test CA configuration self-contained instead.
    ca_config = root / "ca.cnf"
    ca_config.write_text(
        """[req]
distinguished_name=subject
x509_extensions=ca
prompt=no
[subject]
CN=Obstore smoke test CA
[ca]
basicConstraints=critical,CA:TRUE,pathlen:0
keyUsage=critical,keyCertSign,cRLSign
subjectKeyIdentifier=hash
""",
    )
    subprocess.run(  # noqa: S603 - executable and arguments are fixed test inputs
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-sha256",
            "-keyout",
            str(ca_key),
            "-out",
            str(ca_cert),
            "-days",
            "1",
            "-config",
            str(ca_config),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    csr = root / "server.csr"
    subprocess.run(  # noqa: S603 - executable and arguments are fixed test inputs
        [
            openssl,
            "req",
            "-config",
            str(ca_config),
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(csr),
            "-subj",
            "/CN=s3.us-east-1.amazonaws.com",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    extensions = root / "server-extensions.cnf"
    extensions.write_text(
        """subjectAltName=DNS:s3.us-east-1.amazonaws.com
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
""",
    )
    subprocess.run(  # noqa: S603 - executable and arguments are fixed test inputs
        [
            openssl,
            "x509",
            "-req",
            "-in",
            str(csr),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(cert),
            "-days",
            "1",
            "-sha256",
            "-extfile",
            str(extensions),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return cert, key, ca_cert


def _assert_smoke(
    receiver: _Receiver,
    payload: bytes,
    encodings: tuple[bytes, ...],
) -> None:
    if receiver.errors:
        message = f"HTTPS receiver failed: {receiver.errors[0]}"
        raise AssertionError(message)
    if len(receiver.payloads) != len(encodings):
        raise AssertionError("local receiver did not capture all streaming uploads")
    if any(
        hashlib.sha256(body).digest() != hashlib.sha256(payload).digest()
        for body in receiver.payloads
    ):
        raise AssertionError("public Path upload payload did not match its source")
    if not receiver.requests:
        raise AssertionError("no S3 request reached the local HTTPS receiver")
    for index, expected_encoding in enumerate(encodings):
        request, headers = receiver.requests[index]
        _assert_request(
            request,
            headers,
            receiver.trailers[index],
            (payload, expected_encoding),
            include_metadata=index == 0,
        )


def _assert_request(
    request: bytes,
    headers: dict[bytes, bytes],
    trailers: dict[bytes, bytes],
    expected: tuple[bytes, bytes],
    *,
    include_metadata: bool,
) -> None:
    payload, expected_encoding = expected
    if b"/streaming-smoke-bucket/test-prefix/streamed-" not in request:
        raise AssertionError("prefix was not applied to the S3 request")
    if include_metadata:
        if headers.get(b"content-type") != b"application/octet-stream":
            raise AssertionError("streaming upload lost Content-Type")
        if headers.get(b"x-amz-tagging") != b"kind=streaming-smoke":
            raise AssertionError("streaming upload lost object tags")
    if headers.get(b"x-amz-content-sha256") != b"STREAMING-UNSIGNED-PAYLOAD-TRAILER":
        raise AssertionError("public Path API did not use the S3 file-stream route")
    if headers.get(b"content-encoding") != expected_encoding:
        raise AssertionError("streaming upload content-encoding did not match")
    if b"credential=test-access-key/" not in headers.get(b"authorization", b"").lower():
        raise AssertionError(
            "streaming upload was not signed with configured credentials",
        )
    expected_checksum = base64.b64encode(hashlib.sha256(payload).digest())
    if trailers.get(b"x-amz-checksum-sha256") != expected_checksum:
        raise AssertionError("streaming upload checksum trailer did not match source")


def _assert_buffered(receiver: _Receiver, payload: bytes, *, conditional: bool) -> None:
    if receiver.errors:
        raise AssertionError("buffered request failed in HTTPS receiver")
    if receiver.payloads[-1] != payload:
        raise AssertionError("buffered request changed source bytes")
    _, headers = receiver.requests[-1]
    if (
        headers.get(b"x-amz-content-sha256")
        != hashlib.sha256(payload).hexdigest().encode()
    ):
        raise AssertionError("unsupported input did not retain full-payload signing")
    if conditional and headers.get(b"if-none-match") != b"*":
        raise AssertionError("conditional write lost its precondition")


async def run_smoke(
    *,
    large_size: int | None = None,
    request_metrics: S3RequestMetrics | None = None,
) -> None:
    """Exercise public Path streaming against an ordinary AWS HTTPS endpoint."""
    import obstore
    from obstore.store import S3Store

    with tempfile.TemporaryDirectory(prefix="obstore-stream-smoke-") as temp:
        root = Path(temp)
        cert, key, ca_cert = await asyncio.to_thread(_create_cert, root)
        receiver = _Receiver(cert, key)
        thread = threading.Thread(target=receiver.serve_forever, daemon=True)
        thread.start()
        try:
            payload = bytes(
                (index * 17) % 251 for index in range(2 * 1024 * 1024 + 173)
            )
            source = root / "source.bin"
            source.write_bytes(payload)
            cases: tuple[tuple[str, ClientConfig, Attributes, bytes], ...] = (
                ("default", {}, {}, b"aws-chunked"),
                (
                    "configured",
                    {"default_headers": {"Content-Encoding": "configured"}},
                    {},
                    b"configured,aws-chunked",
                ),
                (
                    "overridden",
                    {"default_headers": {"Content-Encoding": "configured"}},
                    {"Content-Encoding": "explicit"},
                    b"explicit,aws-chunked",
                ),
            )
            store = None
            for name, client_options, attributes, _expected_encoding in cases:
                store = S3Store(
                    config={
                        "bucket": "streaming-smoke-bucket",
                        "region": "us-east-1",
                        "endpoint": "https://s3.us-east-1.amazonaws.com",
                        "access_key_id": "test-access-key",
                        "secret_access_key": "test-secret-key",
                        "virtual_hosted_style_request": False,
                        "conditional_put": "etag",
                    },
                    prefix="test-prefix",
                    request_metrics=request_metrics,
                    client_options={
                        **client_options,
                        "proxy_url": f"http://127.0.0.1:{receiver.server_address[1]}",
                        "root_certificate": ca_cert.read_text(),
                    },
                )
                upload_attributes = dict(attributes)
                if name == "default":
                    upload_attributes["Content-Type"] = "application/octet-stream"
                await asyncio.wait_for(
                    obstore.put_async(
                        store,
                        f"streamed-{name}.bin",
                        source,
                        attributes=upload_attributes,
                        tags={"kind": "streaming-smoke"} if name == "default" else None,
                        use_multipart=False if name == "default" else None,
                    ),
                    timeout=60,
                )
            _assert_smoke(
                receiver,
                payload,
                tuple(case[3] for case in cases),
            )
            # The same eligible S3 endpoint must retain buffered byte uploads and
            # conditional writes rather than applying streaming indiscriminately.
            if store is None:
                raise AssertionError("no S3 store was configured for fallback checks")
            fallback_cases: tuple[
                tuple[str, bytes | Path, Literal["create"] | None],
                ...,
            ] = (
                ("bytes", payload, None),
                ("create", source, "create"),
            )
            for name, body, mode in fallback_cases:
                await asyncio.wait_for(
                    obstore.put_async(store, f"buffered-{name}.bin", body, mode=mode),
                    timeout=60,
                )
                _assert_buffered(receiver, payload, conditional=mode == "create")
            if large_size is not None:
                large_payload = b"x" * large_size
                source.write_bytes(large_payload)
                for index, force_single_part in enumerate((False, None)):
                    await asyncio.wait_for(
                        obstore.put_async(
                            store,
                            f"streamed-large-{index}.bin",
                            source,
                            use_multipart=force_single_part,
                            multipart_threshold=large_size + 1
                            if force_single_part is None
                            else None,
                            attributes={"Content-Encoding": "identity"},
                        ),
                        timeout=120,
                    )
                    if receiver.payloads[-1] != large_payload:
                        raise AssertionError(
                            "large single-part upload changed source bytes",
                        )
                    request, headers = receiver.requests[-1]
                    _assert_request(
                        request,
                        headers,
                        receiver.trailers[-1],
                        (large_payload, b"identity,aws-chunked"),
                        include_metadata=False,
                    )
        finally:
            receiver.shutdown()
            receiver.server_close()
            thread.join(timeout=2)
