from __future__ import annotations

import shutil
import subprocess
from typing import TYPE_CHECKING

import pytest

from tests.streaming_https import _create_cert, run_smoke

if TYPE_CHECKING:
    from pathlib import Path


def test_test_ca_does_not_inherit_ambient_extensions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ambient = tmp_path / "ambient.cnf"
    ambient.write_text(
        """[req]
distinguished_name=subject
x509_extensions=ca
[subject]
[ca]
basicConstraints=critical,CA:TRUE
keyUsage=critical,keyCertSign,cRLSign
subjectKeyIdentifier=hash
""",
    )
    monkeypatch.setenv("OPENSSL_CONF", str(ambient))
    cert, _, ca = _create_cert(tmp_path)
    openssl = shutil.which("openssl")
    assert openssl is not None
    output = subprocess.check_output(  # noqa: S603
        [openssl, "x509", "-in", str(ca), "-text", "-noout"],
        text=True,
    )
    for extension in (
        "X509v3 Basic Constraints",
        "X509v3 Key Usage",
        "X509v3 Subject Key Identifier",
    ):
        assert output.count(extension) == 1
    subprocess.run(  # noqa: S603
        [openssl, "verify", "-purpose", "sslserver", "-CAfile", str(ca), str(cert)],
        check=True,
        capture_output=True,
    )


@pytest.mark.asyncio
async def test_put_async_path_streams_through_public_api():
    await run_smoke()


@pytest.mark.asyncio
async def test_single_part_paths_above_32_mib_still_stream():
    await run_smoke(large_size=33 * 1024 * 1024 + 1)
