"""Builders for :attr:`Job.capture <aind_codeocean_dispatch.Job.capture>`."""

from __future__ import annotations

import json
import logging
import re
import urllib.request
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from codeocean.computation import Computation
from codeocean.data_asset import ComputationSource, DataAssetParams, Source

if TYPE_CHECKING:
    from codeocean import CodeOcean

__all__ = ["capture_as", "capture_named_by_data_description", "derived_name"]

logger = logging.getLogger(__name__)

_DATA_DESCRIPTION = "data_description.json"
_DATETIME = r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
# aind-data-schema-models ``DataRegex.DERIVED``: <input>_<datetime>_<process>_<datetime>.
_DERIVED_NAME = re.compile(rf"^.+?_{_DATETIME}_.+?_{_DATETIME}")


def capture_as(name: str, tags: Sequence[str], *, mount: str | None = None) -> Callable[[Computation], DataAssetParams]:
    """Capture a computation's results under a fixed *name*, mounted as *mount* (default *name*)."""

    def build(computation: Computation) -> DataAssetParams:
        return DataAssetParams(
            name=name,
            tags=list(tags),
            mount=mount or name,
            source=Source(computation=ComputationSource(id=computation.id)),
        )

    return build


def derived_name(data_description: dict[str, Any]) -> str | None:
    """Return the asset name a derived ``data_description.json`` declares, or ``None``.

    Only a ``derived`` level whose name follows the AIND derived-data pattern counts;
    anything else would mislabel the asset.
    """
    name = data_description.get("name")
    if data_description.get("data_level") != "derived" or not isinstance(name, str):
        return None
    return name if _DERIVED_NAME.match(name) else None


def capture_named_by_data_description(
    client: CodeOcean, fallback_name: str, tags: Sequence[str]
) -> Callable[[Computation], DataAssetParams]:
    """Capture under the name the results' top-level ``data_description.json`` declares.

    Without a usable one (see :func:`derived_name`), the name is *fallback_name* plus
    the UTC capture time, ``<fallback_name>_YYYY-MM-DD_HH-MM-SS``. The mount is the
    name. A failed read falls back rather than failing the capture, since the name
    is cosmetic and the results are not.
    """

    def build(computation: Computation) -> DataAssetParams:
        name = None
        try:
            name = _declared_name(client, computation.id)
        except Exception as exc:  # noqa: BLE001 - see docstring
            logger.warning("could not read %s of computation %s: %s", _DATA_DESCRIPTION, computation.id, exc)
        if name is None:
            name = f"{fallback_name}_{datetime.now(UTC):%Y-%m-%d_%H-%M-%S}"
        return capture_as(name, tags)(computation)

    return build


def _declared_name(client: CodeOcean, computation_id: str) -> str | None:
    results = client.computations.list_computation_results(computation_id)
    if _DATA_DESCRIPTION not in {item.path for item in results.items}:
        return None
    url = client.computations.get_result_file_urls(computation_id, _DATA_DESCRIPTION).download_url
    with urllib.request.urlopen(url, timeout=60) as fh:
        data_description = json.load(fh)
    name = derived_name(data_description)
    if name is None:
        logger.warning("%s of computation %s declares no derived name", _DATA_DESCRIPTION, computation_id)
    return name
