"""Identify the code a capsule will run when no release is pinned.

The REST API exposes only released versions, so an unreleased capsule's commit has
to be read from its Code Ocean git repo.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from codeocean import CodeOcean

__all__ = ["capsule_head_commit", "head_commit", "run_owner_email"]


def head_commit(refs_advertisement: bytes) -> str | None:
    """Return the ``HEAD`` sha from a git smart-HTTP ref advertisement, if any."""
    pos = 0
    while pos + 4 <= len(refs_advertisement):
        size = int(refs_advertisement[pos : pos + 4], 16)
        if size < 4:  # flush / delimiter packets carry no ref
            pos += 4
            continue
        line = refs_advertisement[pos + 4 : pos + size].split(b"\0")[0].strip()
        pos += size
        sha, _, ref = line.partition(b" ")
        if ref == b"HEAD":
            return sha.decode()
    return None


def run_owner_email(client: CodeOcean, computation_id: str | None = None) -> str:
    """Login email of whoever launched *computation_id*, by default this run.

    Inside a run, that is whose token the run holds; no API route names a token's
    owner directly. Read raw because SDKs before 0.17 drop ``owner_email``.

    Raises
    ------
    ValueError
        If no id is given and ``CO_COMPUTATION_ID`` is unset (not on Code Ocean).
    """
    computation_id = computation_id or os.environ.get("CO_COMPUTATION_ID")
    if not computation_id:
        raise ValueError("CO_COMPUTATION_ID is unset; not running on Code Ocean")
    response = client.session.get(f"computations/{computation_id}")
    response.raise_for_status()
    return str(response.json()["owner_email"])


def capsule_head_commit(
    client: CodeOcean, capsule_id: str, *, username: str | None = None, timeout_s: float = 60.0
) -> str | None:
    """Return the HEAD commit of a capsule's Code Ocean git repo, or ``None`` if it has none.

    Git, unlike the API, authenticates with the token owner's login email as the
    username; *username* defaults to :func:`run_owner_email`, which works only inside
    a run.

    Raises
    ------
    codeocean.error.Error, requests.RequestException
        If the capsule or its repo cannot be read.
    ValueError
        If *username* is omitted outside a Code Ocean run.
    """
    slug = client.capsules.get_capsule(capsule_id).slug
    response = requests.get(
        f"{client.domain}/capsule-{slug}.git/info/refs",
        params={"service": "git-upload-pack"},
        auth=(username or run_owner_email(client), client.token),
        timeout=timeout_s,
    )
    response.raise_for_status()
    return head_commit(response.content)
