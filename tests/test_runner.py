from __future__ import annotations

from typing import Any

import pytest
import requests
from aind_code_ocean_pipeline_utils.process import GracefulExit
from codeocean.models.computation import Computation, DataAssetsRunParam, NamedRunParam, RunParams
from codeocean.models.data_asset import DataAssetParams, DataAssetState

from aind_codeocean_dispatch import (
    DispatchFailed,
    DispatchInterrupted,
    Job,
    job_key,
    key_tag,
    raise_for_failures,
    run_jobs,
    runner,
)
from tests.fakes import (
    CRASHED,
    FAILED_START,
    RUNNING,
    SUCCEEDED,
    FakeClient,
    FakeClock,
    FakeComputations,
    FakeDataAssets,
    asset,
    http_error,
)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(runner, "_now", fake.now)
    monkeypatch.setattr(runner, "_sleep", fake.sleep)
    return fake


def make_client(clock: FakeClock, attempts: dict[str, list[list[Any]]], **assets: Any) -> FakeClient:
    return FakeClient(FakeComputations(clock, attempts), FakeDataAssets(**assets))


def capture_as(name: str) -> Any:
    def build(computation: Computation) -> DataAssetParams:
        return DataAssetParams(name=f"{name}-{computation.id}", tags=["mine"], mount=name)

    return build


def job(label: str, *, capture: bool = True, key: str | None = None) -> Job:
    return Job(label, RunParams(capsule_id=label), capture=capture_as(label) if capture else None, key=key)


# -- the happy path -----------------------------------------------------------


def test_captures_every_job_and_reports_in_input_order(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[RUNNING, SUCCEEDED]], "b": [[SUCCEEDED]]})

    outcomes = run_jobs(client, [job("a"), job("b")])

    assert [(o.job.label, o.status) for o in outcomes] == [("a", "captured"), ("b", "captured")]
    assert all(o.asset_id for o in outcomes)
    assert [p.name for p in client.data_assets.created] == ["b-comp-1", "a-comp-0"]


def test_launches_everything_at_once_by_default(clock: FakeClock) -> None:
    client = make_client(clock, {label: [[RUNNING, SUCCEEDED]] for label in "abcd"})

    run_jobs(client, [job(label) for label in "abcd"])

    assert [t for t, _, _ in client.computations.launches] == [0.0] * 4


def test_without_capture_stops_at_completed(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]})

    (outcome,) = run_jobs(client, [job("a", capture=False)])

    assert outcome.status == "completed"
    assert outcome.asset_id is None
    assert client.data_assets.created == []


def test_key_tag_is_added_to_capture(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]})

    run_jobs(client, [job("a", key="k1")])

    assert client.data_assets.created[0].tags == ["mine", key_tag("k1")]


# -- failed starts and real failures ------------------------------------------


def test_failed_start_is_relaunched_once_by_default(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[FAILED_START], [SUCCEEDED]]})

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "captured"
    assert outcome.computation_ids == ("comp-0", "comp-1")


def test_failed_start_beyond_retries_fails_with_every_attempt(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[FAILED_START], [FAILED_START]]})

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "failed"
    assert outcome.computation_ids == ("comp-0", "comp-1")
    assert "has_results=False" in (outcome.error or "")


def test_start_retries_zero_disables_relaunch(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[FAILED_START]]})

    (outcome,) = run_jobs(client, [job("a")], start_retries=0)

    assert outcome.status == "failed"
    assert len(client.computations.launches) == 1


def test_capsule_crash_is_not_relaunched(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[CRASHED]], "b": [[SUCCEEDED]]})

    outcomes = run_jobs(client, [job("a"), job("b")])

    assert [o.status for o in outcomes] == ["failed", "captured"]
    assert "exit_code=1" in (outcomes[0].error or "")
    assert len(client.computations.launches) == 2


# -- API errors ---------------------------------------------------------------


def test_rate_limited_launch_honours_retry_after_and_holds_other_launches(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]], "b": [[SUCCEEDED]]})
    client.computations.launch_errors["a"] = [http_error(429, retry_after="30")]

    outcomes = run_jobs(client, [job("a"), job("b")])

    assert [o.status for o in outcomes] == ["captured", "captured"]
    assert [(t, capsule) for t, capsule, _ in client.computations.launches] == [(30.0, "a"), (30.0, "b")]


def test_server_error_on_launch_is_not_retried(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]})
    client.computations.launch_errors["a"] = [http_error(503)]

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "failed"
    assert "may have taken effect" in (outcome.error or "")
    assert client.computations.launches == []


def test_transient_errors_on_status_checks_do_not_fail_a_running_job(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[RUNNING, http_error(429), http_error(503), SUCCEEDED]]})

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "captured"


def test_client_error_on_status_check_fails_the_job(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[http_error(404)]]})

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "failed"
    assert "status check failed" in (outcome.error or "")
    assert "may have taken effect" not in (outcome.error or "")


def test_rate_limited_capture_is_retried(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]}, create_errors=[http_error(429)])

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "captured"
    assert len(client.data_assets.created) == 1


# -- opt-in policy ------------------------------------------------------------


def test_max_in_flight_caps_running_computations(clock: FakeClock) -> None:
    client = make_client(clock, {label: [[RUNNING, RUNNING, SUCCEEDED]] for label in "abcde"})

    outcomes = run_jobs(client, [job(label) for label in "abcde"], max_in_flight=2)

    assert all(o.status == "captured" for o in outcomes)
    assert client.computations.max_running == 2


def test_launch_interval_spaces_launches(clock: FakeClock) -> None:
    client = make_client(clock, {label: [[SUCCEEDED]] for label in "abc"})

    run_jobs(client, [job(label) for label in "abc"], launch_interval_s=2.0)

    assert [t for t, _, _ in client.computations.launches] == [0.0, 2.0, 4.0]


def test_failed_capture_is_retried_when_asked(clock: FakeClock) -> None:
    client = make_client(
        clock,
        {"a": [[SUCCEEDED]]},
        capture_states=[[DataAssetState.Draft, DataAssetState.Failed], [DataAssetState.Ready]],
    )

    (outcome,) = run_jobs(client, [job("a")], capture_retries=1)

    assert outcome.status == "captured"
    assert outcome.asset_id == "asset-1"


def test_failed_capture_fails_the_job_by_default(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]}, capture_states=[[DataAssetState.Failed]])

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "failed"
    assert "capture asset-0" in (outcome.error or "")


def test_timeout_fails_the_job_and_leaves_it_running(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[RUNNING]]})

    (outcome,) = run_jobs(client, [job("a")], timeout_s=300, poll_interval_s=60)

    assert outcome.status == "failed"
    assert "comp-0 left running" in (outcome.error or "")


# -- reuse --------------------------------------------------------------------


def test_reuse_returns_newest_ready_asset_with_exact_tag(clock: FakeClock) -> None:
    tag = key_tag("k1")
    existing = [
        asset("old", DataAssetState.Ready, tags=[tag], created=1),
        asset("new", DataAssetState.Ready, tags=[tag], created=2),
        asset("draft", DataAssetState.Draft, tags=[tag], created=3),
        asset("other", DataAssetState.Ready, tags=[f"{tag}0"], created=4),
    ]
    client = make_client(clock, {"a": [[SUCCEEDED]]}, existing=existing)

    (outcome,) = run_jobs(client, [job("a", key="k1")], reuse=True)

    assert outcome.status == "reused"
    assert outcome.asset_id == "new"
    assert client.computations.launches == []


def test_reuse_launches_when_nothing_matches(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]}, existing=[asset("x", DataAssetState.Ready, tags=["unrelated"])])

    (outcome,) = run_jobs(client, [job("a", key="k1")], reuse=True)

    assert outcome.status == "captured"


def test_reuse_off_never_searches(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]}, existing=[asset("x", DataAssetState.Ready, tags=[key_tag("k1")])])

    (outcome,) = run_jobs(client, [job("a", key="k1")])

    assert outcome.status == "captured"
    assert client.data_assets.searches == []


# -- interruption and misuse --------------------------------------------------


def test_interrupt_reports_computations_left_running(clock: FakeClock, monkeypatch: pytest.MonkeyPatch) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]], "b": [[RUNNING]]})

    # Late enough for "a" to finish capturing; "b" never finishes.
    def check_shutdown() -> None:
        if clock.t >= 30:
            raise GracefulExit(15)

    monkeypatch.setattr(runner, "check_shutdown", check_shutdown)

    with pytest.raises(DispatchInterrupted) as info:
        run_jobs(client, [job("a"), job("b")], poll_interval_s=5)

    assert info.value.in_flight == ("comp-1",)
    assert [o.job.label for o in info.value.finished] == ["a"]
    assert "comp-1" in str(info.value)


def test_capture_callback_error_fails_only_that_job(clock: FakeClock) -> None:
    def broken(computation: Computation) -> DataAssetParams:
        raise KeyError("name")

    client = make_client(clock, {"a": [[SUCCEEDED]], "b": [[SUCCEEDED]]})
    jobs = [Job("a", RunParams(capsule_id="a"), capture=broken), job("b")]

    outcomes = run_jobs(client, jobs)

    assert [o.status for o in outcomes] == ["failed", "captured"]


def test_duplicate_labels_are_rejected(clock: FakeClock) -> None:
    with pytest.raises(ValueError, match="unique"):
        run_jobs(make_client(clock, {}), [job("a"), job("a")])


def test_invalid_key_is_rejected() -> None:
    with pytest.raises(ValueError, match="key"):
        job("a", key="has space")


def test_raise_for_failures_lists_failed_jobs(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[CRASHED]], "b": [[SUCCEEDED]]})
    outcomes = run_jobs(client, [job("a"), job("b")])

    with pytest.raises(DispatchFailed) as info:
        raise_for_failures(outcomes)

    assert [o.job.label for o in info.value.failures] == ["a"]
    raise_for_failures(outcomes[1:])


# -- job_key ------------------------------------------------------------------


def test_job_key_ignores_asset_and_named_parameter_order() -> None:
    assets = [DataAssetsRunParam(id="x", mount="mx"), DataAssetsRunParam(id="y", mount="my")]
    params = [NamedRunParam(param_name="p", value="1"), NamedRunParam(param_name="q", value="2")]

    forward = RunParams(capsule_id="c", data_assets=assets, named_parameters=params)
    backward = RunParams(capsule_id="c", data_assets=assets[::-1], named_parameters=params[::-1])

    assert job_key(forward) == job_key(backward)
    assert len(job_key(forward)) == 16


def test_job_key_changes_with_inputs_and_salt() -> None:
    base = RunParams(capsule_id="c", data_assets=[DataAssetsRunParam(id="x", mount="m")])

    assert job_key(base) != job_key(RunParams(capsule_id="c", data_assets=[DataAssetsRunParam(id="y", mount="m")]))
    assert job_key(base) != job_key(base, salt="commit-abc")
    assert job_key(RunParams(capsule_id="c", parameters=["1", "2"])) != job_key(
        RunParams(capsule_id="c", parameters=["2", "1"])
    )


# -- remaining error paths ----------------------------------------------------


def test_connection_error_on_status_check_is_retried(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[requests.ConnectionError("reset"), SUCCEEDED]]})

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "captured"


def test_connection_error_on_launch_fails_as_ambiguous(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]})
    client.computations.launch_errors["a"] = [requests.ConnectionError("reset")]

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "failed"
    assert "may have taken effect" in (outcome.error or "")


def test_http_date_retry_after_falls_back_to_backoff(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]})
    client.computations.launch_errors["a"] = [http_error(429, retry_after="Wed, 21 Oct 2026 07:28:00 GMT")]

    run_jobs(client, [job("a")])

    assert client.computations.launches[0][0] == runner._BACKOFF_BASE_S


def test_transient_error_on_asset_status_check_is_retried(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]}, capture_states=[[http_error(502), DataAssetState.Ready]])

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "captured"


def test_rate_limited_reuse_search_is_retried(clock: FakeClock) -> None:
    client = make_client(
        clock,
        {"a": [[SUCCEEDED]]},
        existing=[asset("x", DataAssetState.Ready, tags=[key_tag("k1")])],
        search_errors=[http_error(429)],
    )

    (outcome,) = run_jobs(client, [job("a", key="k1")], reuse=True)

    assert outcome.status == "reused"
    assert len(client.data_assets.searches) == 2


def test_failed_reuse_search_falls_back_to_launching(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]}, search_errors=[http_error(400)])

    (outcome,) = run_jobs(client, [job("a", key="k1")], reuse=True)

    assert outcome.status == "captured"


@pytest.mark.parametrize(
    ("jobs", "options"),
    [
        ([job("a", key="k"), job("b", key="k")], {}),
        ([job("a")], {"max_in_flight": 0}),
        ([job("a")], {"start_retries": -1}),
    ],
)
def test_invalid_batches_are_rejected(clock: FakeClock, jobs: list[Job], options: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        run_jobs(make_client(clock, {}), jobs, **options)
