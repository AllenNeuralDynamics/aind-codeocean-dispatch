from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass, field
from typing import Any

import pytest
from codeocean.computation import Computation, ComputationState

from aind_codeocean_dispatch import capture
from aind_codeocean_dispatch.capture import capture_as, capture_named_by_data_description, derived_name
from tests.fakes import build

DERIVED = "SmartSPIM_1_2026-01-02_03-04-05_preprocessed_2026-02-03_04-05-06"
COMPUTATION = build(
    Computation, id="comp-1", created=0, name="comp-1", owner="owner", run_time=0, state=ComputationState.Completed
)


@dataclass
class _Item:
    path: str


@dataclass
class _Computations:
    files: dict[str, bytes]
    error: Exception | None = None
    reads: list[str] = field(default_factory=list)

    def list_computation_results(self, computation_id: str, path: str = "") -> Any:
        if self.error is not None:
            raise self.error
        return type("Folder", (), {"items": [_Item(p) for p in self.files]})()

    def get_result_file_urls(self, computation_id: str, path: str) -> Any:
        self.reads.append(path)
        return type("FileURLs", (), {"download_url": f"mem://{path}"})()


@dataclass
class _Client:
    computations: _Computations


@pytest.fixture
def client(monkeypatch) -> _Client:
    fake = _Client(_Computations({}))
    monkeypatch.setattr(
        capture.urllib.request,
        "urlopen",
        lambda url, timeout: io.BytesIO(fake.computations.files[url.removeprefix("mem://")]),
    )
    return fake


def _description(**values: Any) -> bytes:
    return json.dumps(values).encode()


def test_capture_as_targets_the_computation():
    params = capture_as("name", ["t"], mount="m")(COMPUTATION)
    assert (params.name, params.mount, params.tags) == ("name", "m", ["t"])
    assert params.source.computation.id == "comp-1"


def test_capture_as_mounts_under_the_name_by_default():
    assert capture_as("name", [])(COMPUTATION).mount == "name"


@pytest.mark.parametrize(
    ("description", "expected"),
    [
        ({"data_level": "derived", "name": DERIVED}, DERIVED),
        ({"data_level": "raw", "name": DERIVED}, None),
        ({"data_level": "derived", "name": "not_a_derived_name"}, None),
        ({"data_level": "derived"}, None),
    ],
)
def test_derived_name(description, expected):
    assert derived_name(description) == expected


def test_named_by_data_description(client):
    client.computations.files["data_description.json"] = _description(data_level="derived", name=DERIVED)
    params = capture_named_by_data_description(client, "fallback", ["t"])(COMPUTATION)  # type: ignore[arg-type]
    assert (params.name, params.mount, params.tags) == (DERIVED, DERIVED, ["t"])


def _assert_fallback(name: str) -> None:
    assert re.fullmatch(r"fallback_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", name), name


def test_falls_back_without_a_data_description(client):
    params = capture_named_by_data_description(client, "fallback", [])(COMPUTATION)  # type: ignore[arg-type]
    _assert_fallback(params.name)
    assert params.mount == params.name
    assert client.computations.reads == []


def test_falls_back_on_an_unusable_data_description(client):
    client.computations.files["data_description.json"] = _description(data_level="raw", name=DERIVED)
    _assert_fallback(capture_named_by_data_description(client, "fallback", [])(COMPUTATION).name)  # type: ignore[arg-type]


def test_falls_back_when_results_cannot_be_read(client):
    client.computations.error = RuntimeError("boom")
    _assert_fallback(capture_named_by_data_description(client, "fallback", [])(COMPUTATION).name)  # type: ignore[arg-type]
