from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

import pytest
from codeocean.components import EveryoneRole, Permissions
from codeocean.computation import Computation, ComputationEndStatus, ComputationState, InputDataAsset
from codeocean.data_asset import DataAsset, DataAssetParams, DataAssetState
from urllib3.util import Retry

from aind_codeocean_dispatch import automation
from aind_codeocean_dispatch.automation import capture_finished_run, read_only_retries
from tests.fakes import asset, build


def computation(
    *,
    exit_code: int = 0,
    end_status: ComputationEndStatus = ComputationEndStatus.Succeeded,
    has_results: bool = True,
    inputs: tuple[str, ...] = ("raw-1",),
) -> Computation:
    return build(
        Computation,
        id="comp-1",
        created=0,
        name="Run 1",
        owner="owner",
        run_time=0,
        state=ComputationState.Completed,
        end_status=end_status,
        exit_code=exit_code,
        has_results=has_results,
        data_assets=[InputDataAsset(id=i, mount="in") for i in inputs],
    )


@dataclass
class _Computations:
    source: Computation

    def get_computation(self, computation_id: str) -> Computation:
        assert computation_id == self.source.id
        return self.source

    def list_computation_results(self, computation_id: str, path: str = "") -> Any:
        return type("Folder", (), {"items": []})()


@dataclass
class _DataAssets:
    capture_states: list[DataAssetState] = field(default_factory=lambda: [DataAssetState.Draft, DataAssetState.Ready])
    input_error: Exception | None = None
    created: list[DataAssetParams] = field(default_factory=list)
    permissions: list[tuple[str, Permissions]] = field(default_factory=list)

    def get_data_asset(self, data_asset_id: str) -> DataAsset:
        if data_asset_id == "capture-0":
            state = self.capture_states.pop(0) if len(self.capture_states) > 1 else self.capture_states[0]
            return asset(data_asset_id, state)
        if self.input_error is not None:
            raise self.input_error
        found = asset(data_asset_id, DataAssetState.Ready)
        return build(DataAsset, **{**vars(found), "name": f"{data_asset_id}-name"})

    def create_data_asset(self, data_asset_params: DataAssetParams) -> DataAsset:
        self.created.append(data_asset_params)
        return asset("capture-0", DataAssetState.Draft)

    def update_permissions(self, data_asset_id: str, permissions: Permissions) -> None:
        self.permissions.append((data_asset_id, permissions))


@dataclass
class _Client:
    computations: _Computations
    data_assets: _DataAssets


@pytest.fixture(autouse=True)
def no_wait(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    sleeps: list[float] = []
    monkeypatch.setattr(automation, "_sleep", sleeps.append)
    return sleeps


def client_for(source: Computation, **assets: Any) -> Any:
    return _Client(_Computations(source), _DataAssets(**assets))


def test_captures_a_successful_run_named_after_its_input(no_wait: list[float]) -> None:
    client = client_for(computation())

    result = capture_finished_run(client, "comp-1", 0, tags=["exp"], poll_interval_s=7)

    assert result is not None and result.state == DataAssetState.Ready
    [params] = client.data_assets.created
    assert re.fullmatch(r"raw-1-name_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", params.name)
    assert params.tags == ["exp"]
    assert params.source.computation.id == "comp-1"
    assert no_wait == [7]
    assert client.data_assets.permissions == []


@pytest.mark.parametrize(
    ("exit_code", "end_status"), [(1, ComputationEndStatus.Failed), (0, ComputationEndStatus.Failed)]
)
def test_skips_a_failed_run_by_default(exit_code: int, end_status: ComputationEndStatus) -> None:
    client = client_for(computation(exit_code=exit_code, end_status=end_status))

    assert capture_finished_run(client, "comp-1", exit_code) is None
    assert client.data_assets.created == []


def test_captures_a_failed_run_when_asked() -> None:
    client = client_for(computation(exit_code=1, end_status=ComputationEndStatus.Failed))

    assert capture_finished_run(client, "comp-1", 1, capture_failed=True) is not None
    assert len(client.data_assets.created) == 1


def test_skips_a_run_without_results_even_when_asked() -> None:
    client = client_for(computation(exit_code=1, end_status=ComputationEndStatus.Failed, has_results=False))

    assert capture_finished_run(client, "comp-1", 1, capture_failed=True) is None
    assert client.data_assets.created == []


def test_falls_back_to_the_run_name_without_a_readable_input() -> None:
    for client in (client_for(computation(inputs=())), client_for(computation(), input_error=RuntimeError("gone"))):
        capture_finished_run(client, "comp-1", 0)
        assert client.data_assets.created[0].name.startswith("Run 1_")


def test_failed_capture_raises() -> None:
    client = client_for(computation(), capture_states=[DataAssetState.Failed])

    with pytest.raises(RuntimeError, match="capture-0"):
        capture_finished_run(client, "comp-1", 0)
    assert client.data_assets.permissions == []


def test_applies_permissions_once_ready() -> None:
    client = client_for(computation())
    permissions = Permissions(everyone=EveryoneRole.Viewer)

    capture_finished_run(client, "comp-1", 0, permissions=permissions)

    assert client.data_assets.permissions == [("capture-0", permissions)]


def test_main_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    client = client_for(computation(exit_code=1, end_status=ComputationEndStatus.Failed))
    monkeypatch.setattr(automation, "_client_from_env", lambda env: client)

    automation.main(
        {
            "CODEOCEAN_TOKEN": "x",
            "CO_SOURCE_COMPUTATION_ID": "comp-1",
            "CO_SOURCE_EXIT_CODE": "1",
            "CAPTURE_TAGS": "exp, ecephys,,",
            "CAPTURE_FAILED": "yes",
            "CAPTURE_PERMISSIONS": json.dumps({"everyone": "viewer"}),
        }
    )

    assert client.data_assets.created[0].tags == ["exp", "ecephys"]
    assert client.data_assets.permissions == [("capture-0", Permissions(everyone=EveryoneRole.Viewer))]


def test_client_from_env_retries_reads_only() -> None:
    client = automation._client_from_env({"CODEOCEAN_TOKEN": "x"})

    assert client.domain == automation.DEFAULT_DOMAIN
    retries = client.retries
    assert isinstance(retries, Retry)
    assert retries.is_retry("GET", 503) and not retries.is_retry("POST", 503)
    assert read_only_retries().allowed_methods == frozenset({"GET"})
