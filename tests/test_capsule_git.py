from __future__ import annotations

from typing import Any

import pytest
import requests

from aind_codeocean_dispatch import capsule_git
from aind_codeocean_dispatch.capsule_git import capsule_head_commit, head_commit, run_owner_email


def _pkt(payload: bytes) -> bytes:
    return b"%04x" % (len(payload) + 4) + payload


SHA = "0123456789abcdef0123456789abcdef01234567"
REFS = (
    _pkt(b"# service=git-upload-pack\n")
    + b"0000"
    + _pkt(SHA.encode() + b" HEAD\0multi_ack symref=HEAD:refs/heads/master\n")
    + _pkt(SHA.encode() + b" refs/heads/master\n")
    + b"0000"
)


class _Response:
    def __init__(self, content: bytes = b"", status: int = 200, payload: Any = None) -> None:
        self.content = content
        self.status = status
        self.payload = payload

    def json(self) -> Any:
        return self.payload

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise requests.HTTPError(str(self.status))


class _Session:
    def __init__(self) -> None:
        self.paths: list[str] = []

    def get(self, path: str) -> _Response:
        self.paths.append(path)
        return _Response(payload={"owner_email": "someone@example.org"})


class _Capsules:
    def get_capsule(self, capsule_id: str) -> Any:
        return type("Capsule", (), {"slug": "1234567"})()


class _Client:
    domain = "https://example.org"
    token = "token"

    def __init__(self) -> None:
        self.capsules = _Capsules()
        self.session = _Session()


def test_head_commit_reads_the_advertisement():
    assert head_commit(REFS) == SHA


def test_head_commit_without_head():
    assert head_commit(_pkt(b"# service=git-upload-pack\n") + b"0000") is None


def test_run_owner_email_reads_this_run(monkeypatch):
    monkeypatch.setenv("CO_COMPUTATION_ID", "comp-1")
    client = _Client()
    assert run_owner_email(client) == "someone@example.org"  # type: ignore[arg-type]
    assert client.session.paths == ["computations/comp-1"]


def test_run_owner_email_off_code_ocean(monkeypatch):
    monkeypatch.delenv("CO_COMPUTATION_ID", raising=False)
    with pytest.raises(ValueError, match="CO_COMPUTATION_ID"):
        run_owner_email(_Client())  # type: ignore[arg-type]


def test_capsule_head_commit_authenticates_as_the_run_owner(monkeypatch):
    monkeypatch.setenv("CO_COMPUTATION_ID", "comp-1")
    seen: dict[str, Any] = {}

    def fake_get(url: str, **kwargs: Any) -> _Response:
        seen.update(kwargs, url=url)
        return _Response(REFS)

    monkeypatch.setattr(capsule_git.requests, "get", fake_get)
    assert capsule_head_commit(_Client(), "capsule-id") == SHA  # type: ignore[arg-type]
    assert seen["url"] == "https://example.org/capsule-1234567.git/info/refs"
    assert seen["params"] == {"service": "git-upload-pack"}
    assert seen["auth"] == ("someone@example.org", "token")


def test_capsule_head_commit_with_explicit_username(monkeypatch):
    monkeypatch.delenv("CO_COMPUTATION_ID", raising=False)
    seen: dict[str, Any] = {}

    def fake_get(url: str, **kwargs: Any) -> _Response:
        seen.update(kwargs)
        return _Response(REFS)

    monkeypatch.setattr(capsule_git.requests, "get", fake_get)
    assert capsule_head_commit(_Client(), "capsule-id", username="me@example.org") == SHA  # type: ignore[arg-type]
    assert seen["auth"] == ("me@example.org", "token")


def test_capsule_head_commit_raises_on_auth_failure(monkeypatch):
    monkeypatch.setattr(capsule_git.requests, "get", lambda url, **kw: _Response(status=401))
    with pytest.raises(requests.HTTPError):
        capsule_head_commit(_Client(), "capsule-id", username="me@example.org")  # type: ignore[arg-type]
