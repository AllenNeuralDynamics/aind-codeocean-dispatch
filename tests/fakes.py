"""In-memory stand-ins for the Code Ocean client and clock.

Each launch of a capsule consumes the next *attempt* scripted for its ``capsule_id``:
a list of computation snapshots (or exceptions) returned by successive status checks,
the last repeating. Captures and asset status checks are scripted the same way, in
creation order.
"""

from __future__ import annotations

import dataclasses
import itertools
from dataclasses import dataclass, field
from typing import Any

import requests
from codeocean.computation import Computation, ComputationEndStatus, ComputationState, RunParams
from codeocean.data_asset import (
    DataAsset,
    DataAssetParams,
    DataAssetSearchParams,
    DataAssetSearchResults,
    DataAssetState,
    DataAssetType,
)
from codeocean.error import Error as CodeOceanError


def build(cls: Any, **values: Any) -> Any:
    """Construct an SDK model, dropping fields this SDK version does not declare.

    Required fields differ across supported versions (0.17 added ``owner``).
    """
    declared = {f.name for f in dataclasses.fields(cls)}
    return cls(**{name: value for name, value in values.items() if name in declared})


class FakeClock:
    """Monotonic clock that only moves when the code under test sleeps."""

    def __init__(self) -> None:
        self.t = 0.0

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        self.t += seconds


def http_error(status: int, *, retry_after: str | None = None) -> CodeOceanError:
    """Build the SDK's exception the way its response hook does."""
    response = requests.Response()
    response.status_code = status
    response._content = b'{"message": "scripted"}'
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    return CodeOceanError(requests.HTTPError(f"{status} scripted", response=response))


def snapshot(
    state: ComputationState,
    *,
    end_status: ComputationEndStatus | None = None,
    exit_code: int | None = None,
    has_results: bool | None = None,
) -> Any:
    """A partial computation; the fake fills in the id."""
    return {"state": state, "end_status": end_status, "exit_code": exit_code, "has_results": has_results}


RUNNING = snapshot(ComputationState.Running)
SUCCEEDED = snapshot(
    ComputationState.Completed, end_status=ComputationEndStatus.Succeeded, exit_code=0, has_results=True
)
# The shape Code Ocean reports for a run that never started.
FAILED_START = snapshot(
    ComputationState.Completed, end_status=ComputationEndStatus.Failed, exit_code=0, has_results=False
)
FAILED_STATE_START = snapshot(ComputationState.Failed, end_status=ComputationEndStatus.Failed, has_results=False)
CRASHED = snapshot(ComputationState.Completed, end_status=ComputationEndStatus.Failed, exit_code=1, has_results=True)


def asset(asset_id: str, state: DataAssetState, *, tags: list[str] | None = None, created: int = 0) -> DataAsset:
    return build(
        DataAsset,
        id=asset_id,
        created=created,
        name=asset_id,
        mount=asset_id,
        last_used=0,
        owner="owner",
        state=state,
        type=DataAssetType.Result,
        tags=tags,
    )


@dataclass
class FakeComputations:
    clock: FakeClock
    attempts: dict[str, list[list[Any]]]
    launch_errors: dict[str, list[Exception]] = field(default_factory=dict)
    launches: list[tuple[float, str, str]] = field(default_factory=list)  # (time, capsule_id, computation_id)
    max_running: int = 0
    deleted: list[str] = field(default_factory=list)
    delete_errors: list[Exception] = field(default_factory=list)
    _ids: itertools.count[int] = field(default_factory=itertools.count)
    _scripts: dict[str, list[Any]] = field(default_factory=dict)
    _running: set[str] = field(default_factory=set)

    def run_capsule(self, run_params: RunParams) -> Computation:
        errors = self.launch_errors.get(run_params.capsule_id, [])
        if errors:
            raise errors.pop(0)
        computation_id = f"comp-{next(self._ids)}"
        self._scripts[computation_id] = list(self.attempts[run_params.capsule_id].pop(0))
        self.launches.append((self.clock.t, run_params.capsule_id, computation_id))
        self._running.add(computation_id)
        self.max_running = max(self.max_running, len(self._running))
        return self._computation(computation_id, snapshot(ComputationState.Initializing))

    def delete_computation(self, computation_id: str) -> None:
        if self.delete_errors:
            raise self.delete_errors.pop(0)
        self.deleted.append(computation_id)
        self._running.discard(computation_id)

    def get_computation(self, computation_id: str) -> Computation:
        script = self._scripts[computation_id]
        step = script.pop(0) if len(script) > 1 else script[0]
        if isinstance(step, Exception):
            raise step
        if step["state"] in (ComputationState.Completed, ComputationState.Failed):
            self._running.discard(computation_id)
        return self._computation(computation_id, step)

    @staticmethod
    def _computation(computation_id: str, step: dict[str, Any]) -> Computation:
        return build(Computation, id=computation_id, created=0, name=computation_id, owner="owner", run_time=0, **step)


@dataclass
class FakeDataAssets:
    capture_states: list[list[Any]] = field(default_factory=list)
    create_errors: list[Exception] = field(default_factory=list)
    existing: list[DataAsset] = field(default_factory=list)
    search_errors: list[Exception] = field(default_factory=list)
    created: list[DataAssetParams] = field(default_factory=list)
    searches: list[DataAssetSearchParams] = field(default_factory=list)
    _scripts: dict[str, list[Any]] = field(default_factory=dict)

    def create_data_asset(self, data_asset_params: DataAssetParams) -> DataAsset:
        if self.create_errors:
            raise self.create_errors.pop(0)
        asset_id = f"asset-{len(self.created)}"
        self.created.append(data_asset_params)
        states = self.capture_states.pop(0) if self.capture_states else [DataAssetState.Ready]
        self._scripts[asset_id] = list(states)
        return asset(asset_id, DataAssetState.Draft, tags=data_asset_params.tags)

    def get_data_asset(self, data_asset_id: str) -> DataAsset:
        script = self._scripts[data_asset_id]
        step = script.pop(0) if len(script) > 1 else script[0]
        if isinstance(step, Exception):
            raise step
        return asset(data_asset_id, step)

    def search_data_assets(self, search_params: DataAssetSearchParams) -> DataAssetSearchResults:
        self.searches.append(search_params)
        if self.search_errors:
            raise self.search_errors.pop(0)
        return DataAssetSearchResults(has_more=False, results=list(self.existing))


@dataclass
class FakeClient:
    computations: FakeComputations
    data_assets: FakeDataAssets
    retries: Any = 0
