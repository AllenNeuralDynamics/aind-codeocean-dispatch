from __future__ import annotations

import dataclasses
from typing import Any

import pytest
import requests
from aind_code_ocean_pipeline_utils.process import GracefulExit
from codeocean import CodeOcean
from codeocean.components import Ownership
from codeocean.computation import Computation, DataAssetsRunParam, NamedRunParam, RunParams
from codeocean.data_asset import DataAssetParams, DataAssetState
from urllib3.util import Retry

from aind_codeocean_dispatch import (
    DispatchFailed,
    DispatchInterrupted,
    Job,
    Outcome,
    job_key,
    key_marker,
    key_tag,
    raise_for_failures,
    run_jobs,
    runner,
)
from tests.fakes import (
    CRASHED,
    FAILED_START,
    FAILED_STATE_START,
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


def test_failed_start_reported_as_failed_state_is_relaunched(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[FAILED_STATE_START], [SUCCEEDED]]})

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "captured"
    assert outcome.computation_ids == ("comp-0", "comp-1")


def test_failed_start_beyond_retries_fails_with_every_attempt(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[FAILED_START], [FAILED_START]]})

    (outcome,) = run_jobs(client, [job("a")])

    assert outcome.status == "failed"
    assert outcome.computation_ids == ("comp-0", "comp-1")
    assert "has_results=False" in (outcome.error or "")
    assert (outcome.failure, outcome.transient) == ("failed_start", True)


def test_failed_starts_back_off_and_hold_other_launches(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[FAILED_START], [FAILED_START], [SUCCEEDED]], "b": [[SUCCEEDED]]})

    outcomes = run_jobs(client, [job("a"), job("b")], start_retries=2, start_retry_backoff_s=60, poll_interval_s=5)

    assert [o.status for o in outcomes] == ["captured", "captured"]
    a_launches = [t for t, capsule, _ in client.computations.launches if capsule == "a"]
    # Relaunches wait 60 s, then 120 s, after each failed start is seen.
    assert a_launches[1] - a_launches[0] >= 60
    assert a_launches[2] - a_launches[1] >= 120
    assert len(client.computations.launches) == 4


def test_failed_start_holds_a_launch_still_pending(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[FAILED_START], [SUCCEEDED]], "b": [[SUCCEEDED]]})

    run_jobs(client, [job("a"), job("b")], max_in_flight=1, start_retry_backoff_s=60, poll_interval_s=5)

    first_a = client.computations.launches[0][0]
    (b_launch,) = [t for t, capsule, _ in client.computations.launches if capsule == "b"]
    assert b_launch - first_a >= 60


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
    assert (outcomes[0].failure, outcomes[0].transient) == ("computation", False)
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
    assert (outcome.failure, outcome.transient) == ("api_unavailable", True)


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
    assert (outcome.failure, outcome.transient) == ("api_rejected", False)


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
    assert (outcome.failure, outcome.transient) == ("capture", True)


def test_timeout_fails_the_job_and_leaves_it_running(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[RUNNING]]})

    (outcome,) = run_jobs(client, [job("a")], timeout_s=300, poll_interval_s=60)

    assert outcome.status == "failed"
    assert "comp-0 left running" in (outcome.error or "")
    assert (outcome.failure, outcome.transient) == ("timeout", False)


# -- cancelling on failure ----------------------------------------------------


def test_successful_outcomes_carry_no_failure(clock: FakeClock) -> None:
    (outcome,) = run_jobs(make_client(clock, {"a": [[SUCCEEDED]]}), [job("a")])

    assert (outcome.failure, outcome.transient, outcome.ok) == (None, False, True)


def test_failure_cancels_running_and_pending_jobs(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[CRASHED]], "b": [[RUNNING]], "c": [[SUCCEEDED]]})

    outcomes = run_jobs(client, [job("a"), job("b"), job("c")], max_in_flight=2, cancel_on_failure=True)

    assert [o.status for o in outcomes] == ["failed", "cancelled", "cancelled"]
    assert client.computations.deleted == ["comp-1"]
    assert [capsule for _, capsule, _ in client.computations.launches] == ["a", "b"]
    assert outcomes[1].computation_ids == ("comp-1",)
    assert outcomes[2].computation_ids == ()
    assert outcomes[1].error == "cancelled after a failed"
    assert not outcomes[1].ok
    assert outcomes[1].failure is None


def test_capture_under_way_finishes_when_cancelling(clock: FakeClock) -> None:
    client = make_client(
        clock,
        {"a": [[RUNNING, CRASHED]], "b": [[SUCCEEDED]]},
        capture_states=[[DataAssetState.Draft] * 10 + [DataAssetState.Ready]],
    )

    outcomes = run_jobs(client, [job("a"), job("b")], cancel_on_failure=True)

    assert [o.status for o in outcomes] == ["failed", "captured"]
    assert client.computations.deleted == []


def test_cancel_predicate_decides_per_failure(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[FAILED_START]], "b": [[RUNNING, SUCCEEDED]]})

    outcomes = run_jobs(
        client, [job("a"), job("b")], start_retries=0, cancel_on_failure=lambda outcome: not outcome.transient
    )

    assert [o.status for o in outcomes] == ["failed", "captured"]


def test_failed_stop_is_reported_on_the_cancelled_job(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[CRASHED]], "b": [[RUNNING]]})
    client.computations.delete_errors = [http_error(503)]

    outcomes = run_jobs(client, [job("a"), job("b")], cancel_on_failure=True)

    assert outcomes[1].status == "cancelled"
    assert "could not stop computation comp-1" in (outcomes[1].error or "")


def test_failures_continue_by_default(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[CRASHED]], "b": [[RUNNING, SUCCEEDED]]})

    outcomes = run_jobs(client, [job("a"), job("b")])

    assert [o.status for o in outcomes] == ["failed", "captured"]
    assert client.computations.deleted == []


def test_raise_for_failures_lists_cancelled_jobs(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[CRASHED]], "b": [[RUNNING]]})
    outcomes = run_jobs(client, [job("a"), job("b")], cancel_on_failure=True)

    with pytest.raises(DispatchFailed) as info:
        raise_for_failures(outcomes)

    assert [o.job.label for o in info.value.failures] == ["a", "b"]


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
    (search,) = client.data_assets.searches
    assert search.ownership == Ownership.Created


def test_reuse_launches_when_nothing_matches(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]}, existing=[asset("x", DataAssetState.Ready, tags=["unrelated"])])

    (outcome,) = run_jobs(client, [job("a", key="k1")], reuse=True)

    assert outcome.status == "captured"


def test_reuse_off_never_searches(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]}, existing=[asset("x", DataAssetState.Ready, tags=[key_tag("k1")])])

    (outcome,) = run_jobs(client, [job("a", key="k1")])

    assert outcome.status == "captured"
    assert client.data_assets.searches == []


# -- on_outcome -------------------------------------------------------------------


def test_on_outcome_sees_each_job_as_it_finishes(clock: FakeClock) -> None:
    existing = [asset("old", DataAssetState.Ready, tags=[key_tag("k1")])]
    client = make_client(clock, {"slow": [[RUNNING] * 5 + [SUCCEEDED]], "crash": [[CRASHED]]}, existing=existing)
    seen: list[tuple[Outcome, float]] = []

    outcomes = run_jobs(
        client,
        [job("slow"), job("crash"), job("cached", key="k1")],
        reuse=True,
        poll_interval_s=10,
        on_outcome=lambda outcome: seen.append((outcome, clock.t)),
    )

    assert [(o.job.label, o.status) for o, _ in seen] == [
        ("cached", "reused"),
        ("crash", "failed"),
        ("slow", "captured"),
    ]
    # Each is reported when reached, not when the batch ends.
    assert seen[1][1] < seen[2][1]
    assert sorted((o for o, _ in seen), key=lambda o: o.job.label) == sorted(outcomes, key=lambda o: o.job.label)


def test_on_outcome_sees_cancelled_jobs(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[CRASHED]], "b": [[RUNNING]]})
    seen: list[Outcome] = []

    run_jobs(client, [job("a"), job("b")], cancel_on_failure=True, on_outcome=seen.append)

    assert [(o.job.label, o.status) for o in seen] == [("a", "failed"), ("b", "cancelled")]


def test_on_outcome_error_does_not_affect_the_batch(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]], "b": [[RUNNING, SUCCEEDED]]})

    def broken(outcome: Outcome) -> None:
        raise RuntimeError("hook")

    outcomes = run_jobs(client, [job("a"), job("b")], on_outcome=broken)

    assert [o.status for o in outcomes] == ["captured", "captured"]


# -- resume -------------------------------------------------------------------------


def test_keyed_computations_are_named_for_resume(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]], "b": [[SUCCEEDED]]})

    run_jobs(client, [job("a", key="k1"), job("b")])

    assert client.computations.names == {"comp-0": f"a {key_marker('k1')}"}


def test_failed_naming_is_tried_again(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[RUNNING, RUNNING, SUCCEEDED]]})
    client.computations.rename_errors = [http_error(429)]

    (outcome,) = run_jobs(client, [job("a", key="k1")])

    assert outcome.status == "captured"
    assert client.computations.names["comp-0"] == f"a {key_marker('k1')}"


def test_resume_follows_the_owners_running_computation(clock: FakeClock) -> None:
    client = make_client(clock, {})
    client.computations.existing("a", "earlier", [RUNNING, SUCCEEDED], name=f"a {key_marker('k1')}")

    (outcome,) = run_jobs(client, [job("a", key="k1")], resume_owner="me")

    assert (outcome.status, outcome.resumed, outcome.computation_ids) == ("captured", True, ("earlier",))
    assert client.computations.launches == []


def test_resume_matches_the_owner_by_email(clock: FakeClock) -> None:
    client = make_client(clock, {})
    client.computations.existing("a", "earlier", [SUCCEEDED], name=f"a {key_marker('k1')}", owner="me@x.org")

    (outcome,) = run_jobs(client, [job("a", key="k1")], resume_owner="me@x.org")

    assert outcome.resumed


def test_resume_ignores_other_owners_failed_runs_and_other_keys(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]})
    client.computations.existing("a", "theirs", [RUNNING], name=f"a {key_marker('k1')}", owner="someone")
    client.computations.existing("a", "failed", [CRASHED], name=f"a {key_marker('k1')}")
    client.computations.existing("a", "longer", [RUNNING], name=f"a {key_marker('k10')}")

    (outcome,) = run_jobs(client, [job("a", key="k1")], resume_owner="me")

    assert (outcome.status, outcome.resumed, outcome.computation_ids) == ("captured", False, ("comp-0",))


def test_resume_prefers_a_live_computation_to_a_finished_one(clock: FakeClock) -> None:
    client = make_client(clock, {})
    marker = key_marker("k1")
    client.computations.existing("a", "done", [SUCCEEDED], name=f"a {marker}", created=5)
    client.computations.existing("a", "live", [RUNNING, SUCCEEDED], name=f"a {marker}", created=1)

    (outcome,) = run_jobs(client, [job("a", key="k1")], resume_owner="me")

    assert outcome.computation_ids == ("live",)


def test_resume_captures_a_succeeded_uncaptured_computation(clock: FakeClock) -> None:
    client = make_client(clock, {})
    client.computations.existing("a", "earlier", [SUCCEEDED], name=f"a {key_marker('k1')}")

    (outcome,) = run_jobs(client, [job("a", key="k1")], resume_owner="me")

    assert (outcome.status, outcome.resumed) == ("captured", True)
    assert [p.name for p in client.data_assets.created] == ["a-earlier"]


def test_resume_does_not_capture_a_succeeded_computation_again(clock: FakeClock) -> None:
    existing = [asset("old", DataAssetState.Ready, tags=[key_tag("k1")])]
    client = make_client(clock, {"a": [[SUCCEEDED]]}, existing=existing)
    client.computations.existing("a", "earlier", [SUCCEEDED], name=f"a {key_marker('k1')}")

    (outcome,) = run_jobs(client, [job("a", key="k1")], resume_owner="me")

    assert (outcome.status, outcome.resumed, outcome.computation_ids) == ("captured", False, ("comp-0",))


def test_resume_waits_on_a_capture_in_progress(clock: FakeClock) -> None:
    client = make_client(clock, {}, existing=[asset("capturing", DataAssetState.Draft, tags=[key_tag("k1")])])
    client.data_assets._scripts["capturing"] = [DataAssetState.Draft, DataAssetState.Ready]

    (outcome,) = run_jobs(client, [job("a", key="k1")], resume_owner="me")

    assert (outcome.status, outcome.asset_id, outcome.resumed) == ("captured", "capturing", True)
    assert client.computations.launches == []
    assert client.data_assets.created == []


def test_resume_lists_each_capsule_once(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED], [SUCCEEDED]]})
    jobs = [Job(label, RunParams(capsule_id="a"), capture=capture_as(label), key=label) for label in ("x", "y")]

    run_jobs(client, jobs, resume_owner="me")

    assert client.computations.listings == ["a"]


def test_resume_lists_a_pipelines_computations(clock: FakeClock) -> None:
    client = make_client(clock, {})
    client.computations.existing("p", "earlier", [SUCCEEDED], name=f"p {key_marker('k1')}")

    (outcome,) = run_jobs(
        client, [Job("p", RunParams(pipeline_id="p"), capture=capture_as("p"), key="k1")], resume_owner="me"
    )

    assert outcome.computation_ids == ("earlier",)


def test_rate_limited_listing_is_retried(clock: FakeClock) -> None:
    client = make_client(clock, {})
    client.computations.existing("a", "earlier", [SUCCEEDED], name=f"a {key_marker('k1')}")
    client.computations.list_errors = [http_error(429)]

    (outcome,) = run_jobs(client, [job("a", key="k1")], resume_owner="me")

    assert outcome.resumed
    assert client.computations.listings == ["a", "a"]


def test_failed_listing_falls_back_to_launching(clock: FakeClock) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]})
    client.computations.list_errors = [http_error(403)]

    (outcome,) = run_jobs(client, [job("a", key="k1")], resume_owner="me")

    assert (outcome.status, outcome.resumed) == ("captured", False)


def test_resume_needs_an_sdk_whose_computations_carry_an_owner(
    clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    @dataclasses.dataclass
    class OldComputation:
        id: str

    monkeypatch.setattr(runner, "Computation", OldComputation)

    with pytest.raises(ValueError, match="0.15"):
        run_jobs(make_client(clock, {}), [job("a", key="k1")], resume_owner="me")


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
    assert outcomes[0].failure == "capture_callback"


def test_duplicate_labels_are_rejected(clock: FakeClock) -> None:
    with pytest.raises(ValueError, match="unique"):
        run_jobs(make_client(clock, {}), [job("a"), job("a")])


@pytest.mark.parametrize("retries", [3, Retry(total=5, allowed_methods=["POST"])])
def test_client_that_retries_itself_still_runs(clock: FakeClock, retries: Any) -> None:
    client = make_client(clock, {"a": [[SUCCEEDED]]})
    client.retries = retries

    outcomes = run_jobs(client, [job("a")])

    assert [o.status for o in outcomes] == ["captured"]
    assert client.retries is retries


def test_retrying_client_is_copied_without_retries() -> None:
    original = CodeOcean(domain="https://codeocean.example", token="x", retries=Retry(total=5))

    copy = runner._without_transport_retries(original)

    assert copy is not original and copy.session is not original.session
    assert (copy.retries, copy.domain, copy.token) == (0, original.domain, original.token)
    assert isinstance(original.retries, Retry)


def test_non_retrying_client_is_used_as_is() -> None:
    client = CodeOcean(domain="https://codeocean.example", token="x")

    assert runner._without_transport_retries(client) is client


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
