"""Capture a finished run as an internal data asset, from a Code Ocean post-run automation.

Code Ocean runs a pipeline's automation capsule after every run, passing the run as
``CO_SOURCE_COMPUTATION_ID`` and ``CO_SOURCE_EXIT_CODE``. A capsule whose ``code/run`` is
``python -m aind_codeocean_dispatch.automation`` captures that run whatever its results
hold: the name comes from a derived ``data_description.json`` when one declares it, and
otherwise from the first input asset and the capture time. It writes no external bucket
and no DocDB record, so it needs only a Code Ocean token, and it is no substitute for a
publishing automation that checks metadata before registering it.

Configured by the capsule's environment:

- ``CODEOCEAN_TOKEN`` (required) and ``CODEOCEAN_DOMAIN``.
- ``CAPTURE_TAGS``: comma-separated tags for the asset.
- ``CAPTURE_FAILED``: truthy to capture failed runs that left results.
- ``CAPTURE_PERMISSIONS``: a ``Permissions`` object as JSON, applied once the asset is Ready.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING

from aind_code_ocean_pipeline_utils import parse_truthy
from codeocean import CodeOcean
from codeocean.components import Permissions
from codeocean.computation import Computation
from codeocean.data_asset import DataAsset, DataAssetState
from urllib3.util import Retry

from .capture import capture_named_by_data_description
from .runner import succeeded

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["capture_finished_run", "main", "read_only_retries"]

logger = logging.getLogger(__name__)

DEFAULT_DOMAIN = "https://codeocean.allenneuraldynamics.org"

# Indirected so tests need not wait.
_sleep = time.sleep


def read_only_retries() -> Retry:
    """Retry 429 and 5xx on GET only; a resent ``create_data_asset`` could duplicate the asset."""
    return Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
    )


def capture_finished_run(
    client: CodeOcean,
    computation_id: str,
    exit_code: int,
    *,
    tags: Sequence[str] = (),
    capture_failed: bool = False,
    permissions: Permissions | None = None,
    poll_interval_s: float = 60.0,
) -> DataAsset | None:
    """Capture *computation_id*'s results and wait until the asset is Ready.

    Parameters
    ----------
    client : CodeOcean
        Authenticated client; see :func:`read_only_retries` for a safe ``retries``.
    computation_id, exit_code
        The finished run, as Code Ocean passes it to the automation.
    tags : Sequence[str], optional
        Tags for the asset.
    capture_failed : bool, optional
        Capture a failed run too, provided it left results.
    permissions : Permissions, optional
        Applied once the asset is Ready; ``None`` leaves Code Ocean's default.
    poll_interval_s : float, optional
        Seconds between checks of the asset's state.

    Returns
    -------
    DataAsset or None
        The Ready asset, or ``None`` when the run was skipped.

    Raises
    ------
    RuntimeError
        If the capture ends in the Failed state.
    """
    source = client.computations.get_computation(computation_id)
    if not (exit_code == 0 and succeeded(source)) and not capture_failed:
        logger.info("computation %s did not succeed (exit code %d); not capturing", computation_id, exit_code)
        return None
    if source.has_results is False:
        logger.info("computation %s left no results; nothing to capture", computation_id)
        return None
    params = capture_named_by_data_description(client, _fallback_name(client, source), tags)(source)
    created = client.data_assets.create_data_asset(params)
    logger.info("capturing computation %s as %s (%s)", computation_id, created.id, params.name)
    ready = _wait_until_settled(client, created.id, poll_interval_s)
    if ready.state == DataAssetState.Failed:
        raise RuntimeError(f"capture {created.id} of computation {computation_id} failed")
    if permissions is not None:
        client.data_assets.update_permissions(ready.id, permissions)
    logger.info("captured computation %s as %s", computation_id, ready.id)
    return ready


def _fallback_name(client: CodeOcean, source: Computation) -> str:
    """Name a capture after the run's first input asset, or the run itself if it has none."""
    if source.data_assets:
        try:
            return str(client.data_assets.get_data_asset(source.data_assets[0].id).name)
        except Exception as exc:  # noqa: BLE001 - the name is cosmetic; the capture is not
            logger.warning("could not read input asset %s: %s", source.data_assets[0].id, exc)
    return str(source.name)


def _wait_until_settled(client: CodeOcean, asset_id: str, poll_interval_s: float) -> DataAsset:
    while True:
        asset = client.data_assets.get_data_asset(asset_id)
        if asset.state in (DataAssetState.Ready, DataAssetState.Failed):
            return asset
        _sleep(poll_interval_s)


def _client_from_env(env: Mapping[str, str]) -> CodeOcean:
    return CodeOcean(
        domain=env.get("CODEOCEAN_DOMAIN", DEFAULT_DOMAIN),
        token=env["CODEOCEAN_TOKEN"],
        retries=read_only_retries(),
    )


def main(env: Mapping[str, str] | None = None) -> None:
    """Capture the run Code Ocean names in the environment; see the module docstring."""
    env = os.environ if env is None else env
    logging.basicConfig(level=env.get("LOG_LEVEL", "INFO"))
    raw_permissions = env.get("CAPTURE_PERMISSIONS")
    capture_finished_run(
        _client_from_env(env),
        env["CO_SOURCE_COMPUTATION_ID"],
        int(env["CO_SOURCE_EXIT_CODE"]),
        tags=[tag.strip() for tag in env.get("CAPTURE_TAGS", "").split(",") if tag.strip()],
        capture_failed=parse_truthy(env.get("CAPTURE_FAILED", "")),
        permissions=Permissions.from_dict(json.loads(raw_permissions)) if raw_permissions else None,
    )


if __name__ == "__main__":
    main()
