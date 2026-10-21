"""Launch a batch of Code Ocean runs, wait for each, and capture its results.

:func:`run_jobs` drives every job from one polling loop. Each job moves
pending → running → capturing → done, so the concurrency cap and launch pacing are
counters rather than locks, and no thread is parked per job.

Error policy
------------
Every 429 or 5xx must surface here, so :func:`run_jobs` works on a copy of the
client with ``retries=0``: transport-level retries would resend POSTs below this
policy, and their exhausted retries raise without the status code it reads.

- **429** is retried on every call, honouring ``Retry-After``: the server rejected
  the request without acting on it.
- **5xx and connection errors** are retried on reads only. A POST that fails this way
  may already have created its computation or data asset, so retrying it could
  duplicate one; the job fails instead, saying so.
- **Any other HTTP error** fails the job.

Policy beyond that -- concurrency cap, launch pacing, relaunching failed starts,
retrying failed captures, reusing captured results, resuming computations a
previous batch left running -- is opt-in.
"""

from __future__ import annotations

import dataclasses
import logging
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Literal, TypeVar

import requests
from aind_code_ocean_pipeline_utils.cache import input_fingerprint
from aind_code_ocean_pipeline_utils.process import GracefulExit, check_shutdown

# Import paths that exist across supported SDK versions; 0.17 moved the models to
# ``codeocean.models`` and kept these as re-exports.
from codeocean.components import Ownership
from codeocean.computation import Computation, ComputationEndStatus, ComputationState, RunParams
from codeocean.data_asset import DataAsset, DataAssetParams, DataAssetSearchParams, DataAssetState
from codeocean.error import Error as CodeOceanError

if TYPE_CHECKING:
    from codeocean import CodeOcean

__all__ = [
    "TRANSIENT_FAILURES",
    "DispatchFailed",
    "DispatchInterrupted",
    "Failure",
    "Job",
    "Outcome",
    "failed_without_results",
    "job_key",
    "key_marker",
    "key_tag",
    "raise_for_failures",
    "run_jobs",
    "succeeded",
]

logger = logging.getLogger(__name__)

# Indirected so tests can drive the loop with a fake clock.
_now = time.monotonic
_sleep = time.sleep

_ASSET_POLL_S = 10.0
# Sleep in short chunks: a flag-only signal handler does not interrupt time.sleep.
_MAX_SLEEP_S = 5.0
_BACKOFF_BASE_S = 5.0
_BACKOFF_CAP_S = 300.0
_RATE_LIMITED = 429
_TRANSIENT_SERVER = frozenset({500, 502, 503, 504})
_KEY_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,64}")
_KEY_TAG_PREFIX = "dispatch-"

Status = Literal["captured", "completed", "reused", "failed", "cancelled"]
#: Why a job failed. ``failed_start`` means the relaunch budget ran out;
#: ``computation`` that it ran and failed; ``api_rejected`` a 4xx other than 429;
#: ``api_unavailable`` a 5xx or connection error on a call that cannot be retried.
Failure = Literal[
    "failed_start", "computation", "timeout", "capture", "capture_callback", "api_rejected", "api_unavailable"
]
#: Failures on the platform's side, which a rerun of the same job can get past.
TRANSIENT_FAILURES: frozenset[Failure] = frozenset({"failed_start", "capture", "api_unavailable"})
_T = TypeVar("_T")


@dataclass(frozen=True)
class Job:
    """One capsule or pipeline run to launch, and how to capture its results.

    Parameters
    ----------
    label : str
        Unique within a batch; names the job in logs and outcomes.
    run_params : RunParams
        Passed verbatim to ``client.computations.run_capsule``.
    capture : callable, optional
        Builds the capture request from the finished computation, so the asset name
        can depend on it. ``None`` stops at a successful computation.
    key : str, optional
        Resume key (see :func:`job_key`), added to the captured asset as
        :func:`key_tag` and to the computation's name as :func:`key_marker`. With
        ``reuse=True``, a Ready asset carrying it that the caller created is
        returned instead of launching; with ``resume_owner``, the caller's
        computation carrying it is followed instead. ``None`` does neither.
    """

    label: str
    run_params: RunParams
    capture: Callable[[Computation], DataAssetParams] | None = None
    key: str | None = None

    def __post_init__(self) -> None:
        """Reject keys that cannot round-trip through a Code Ocean tag search."""
        if self.key is not None and not _KEY_PATTERN.fullmatch(self.key):
            raise ValueError(f"job {self.label!r}: key {self.key!r} must match {_KEY_PATTERN.pattern}")


@dataclass(frozen=True)
class Outcome:
    """How one job ended.

    ``computation_ids`` lists every launch attempt in order, so a relaunched job's
    failed starts stay traceable; ``resumed`` says the first of them was found
    running, or finished, rather than launched by this batch. ``failure`` is set
    only when ``status`` is ``"failed"``.
    """

    job: Job
    status: Status
    asset_id: str | None = None
    computation_ids: tuple[str, ...] = ()
    error: str | None = None
    failure: Failure | None = None
    resumed: bool = False

    @property
    def ok(self) -> bool:
        """Whether the job produced what was asked of it."""
        return self.status not in ("failed", "cancelled")

    @property
    def transient(self) -> bool:
        """Whether the job failed for a platform reason a rerun can get past."""
        return self.failure in TRANSIENT_FAILURES


class DispatchFailed(RuntimeError):
    """Raised by :func:`raise_for_failures`; ``failures`` holds the failed outcomes."""

    def __init__(self, failures: Sequence[Outcome]) -> None:
        self.failures = tuple(failures)
        lines = [f"{len(self.failures)} job(s) failed:"]
        lines += [f"  {o.job.label}: {o.error}" for o in self.failures]
        super().__init__("\n".join(lines))


class DispatchInterrupted(GracefulExit):
    """A shutdown signal stopped :func:`run_jobs` before every job finished.

    Computations already launched keep running: deleting them would discard work
    that may be about to succeed. ``in_flight`` names them; ``finished`` holds the
    outcomes reached before the signal.
    """

    def __init__(self, signum: int, *, in_flight: Sequence[str], finished: Sequence[Outcome]) -> None:
        super().__init__(signum)
        self.in_flight = tuple(in_flight)
        self.finished = tuple(finished)

    def __str__(self) -> str:
        """List the computations left running."""
        running = ", ".join(self.in_flight) or "none"
        return (
            f"interrupted by {self.signame}; {len(self.finished)} job(s) finished, computations left running: {running}"
        )


def succeeded(computation: Computation) -> bool:
    """Whether a finished computation ran to a clean exit."""
    return (
        computation.state == ComputationState.Completed
        and computation.end_status not in (ComputationEndStatus.Failed, ComputationEndStatus.Stopped)
        and computation.exit_code in (None, 0)
    )


def failed_without_results(computation: Computation) -> bool:
    """Whether a computation looks like it never started: failed, with no results.

    Code Ocean reports a failed start as ``state=Completed`` with
    ``end_status=Failed``, ``exit_code=0`` and no results, so this keys on
    ``end_status``, not ``state``. A capsule that crashes before writing anything
    can match too, so a relaunch can be spent on a real bug; ``start_retries``
    bounds that cost.
    """
    return computation.end_status == ComputationEndStatus.Failed and not computation.has_results


def key_tag(key: str) -> str:
    """Return the data-asset tag that carries resume *key*."""
    return f"{_KEY_TAG_PREFIX}{key}"


def key_marker(key: str) -> str:
    """Return the text a computation's name carries to be found again by resume *key*."""
    return f"[{key_tag(key)}]"


def job_key(run_params: RunParams, *, salt: str = "") -> str:
    """Return a resume key: 16 hex characters fingerprinting *run_params*.

    Covers the capsule or pipeline id, version, attached assets, and parameters.
    Attached assets and named parameters are order-insensitive; positional
    parameters are not. Unset fields are dropped, so an SDK release that adds an
    optional field does not change existing keys.

    Parameters
    ----------
    run_params : RunParams
        The launch request.
    salt : str, optional
        Mixed in for what the request cannot show, such as the git commit of a
        non-release capsule whose code changes without its id or version changing.
    """
    spec = _prune(run_params.to_dict())
    for name, sort_key in (("data_assets", ("id", "mount")), ("named_parameters", ("param_name",))):
        if name in spec:
            spec[name] = sorted(spec[name], key=lambda item: tuple(str(item.get(k)) for k in sort_key))
    return input_fingerprint({"run_params": spec, "salt": salt}).removeprefix("sha256:")[:16]


def raise_for_failures(outcomes: Sequence[Outcome]) -> None:
    """Raise :class:`DispatchFailed` if any outcome failed."""
    failures = [o for o in outcomes if not o.ok]
    if failures:
        raise DispatchFailed(failures)


def run_jobs(
    client: CodeOcean,
    jobs: Sequence[Job],
    *,
    max_in_flight: int | None = None,
    launch_interval_s: float = 0.0,
    start_retries: int = 1,
    start_retry_backoff_s: float = 60.0,
    is_failed_start: Callable[[Computation], bool] = failed_without_results,
    capture_retries: int = 0,
    reuse: bool = False,
    poll_interval_s: float = 60.0,
    timeout_s: float | None = None,
    cancel_on_failure: bool | Callable[[Outcome], bool] = False,
    resume_owner: str | None = None,
    on_outcome: Callable[[Outcome], None] | None = None,
) -> list[Outcome]:
    """Launch *jobs*, wait for each, capture its results, and report every outcome.

    A job's failure never raises; check the outcomes, or pass them to
    :func:`raise_for_failures`.

    Parameters
    ----------
    client : CodeOcean
        Authenticated client. Its own ``retries`` setting is overridden on a copy;
        the caller's client is left unchanged.
    jobs : Sequence[Job]
        Launched in order, subject to the cap and pacing.
    max_in_flight : int, optional
        Most computations running at once. ``None`` launches everything.
    launch_interval_s : float, optional
        Minimum seconds between launches. Rate limiting is handled by retrying 429s
        regardless; this only spreads the burst.
    start_retries : int, optional
        Relaunches allowed per job when *is_failed_start* matches.
    start_retry_backoff_s : float, optional
        Wait before the first relaunch, doubling for each later one. Every pending
        launch is held too: a start failure usually means machine capacity ran out,
        and the runs holding it will not free it at once.
    is_failed_start : callable, optional
        Classifies a failed computation as a platform start failure.
    capture_retries : int, optional
        New capture attempts per job after a capture ends in the Failed state.
    reuse : bool, optional
        Return a Ready asset tagged with a job's key instead of launching it. Only
        assets the caller created count: another user running identical inputs
        produces the same key, and their asset is not the caller's to consume.
    poll_interval_s : float, optional
        Seconds between status checks of each running computation.
    timeout_s : float, optional
        Give up waiting on an attempt after this long. The computation is left
        running, not stopped. ``None`` waits indefinitely.
    cancel_on_failure : bool or callable, optional
        Whether a failed job cancels the rest, or a predicate on its outcome. On
        cancel, pending jobs are not launched and running computations are stopped
        and deleted, all ending ``"cancelled"``; captures already under way finish.
    resume_owner : str, optional
        The caller's Code Ocean user id or email. When set, a keyed job whose
        computation this owner launched earlier, and which is still running or
        succeeded uncaptured, is followed from there instead of launched again; a
        capture of it still in progress is waited on. Code Ocean lists every
        user's computations and has no call naming the caller, hence the
        argument. Needs codeocean 0.15 or later, whose computations carry an owner.
    on_outcome : callable, optional
        Called with each job's outcome as soon as it is reached, whatever the
        status, so results can be used before the batch ends. An exception it
        raises is logged and does not affect the batch.

    Returns
    -------
    list[Outcome]
        One per job, in input order.

    Raises
    ------
    ValueError
        On duplicate labels or keys, or nonsensical limits.
    DispatchInterrupted
        When a shutdown signal arrives (handlers installed by the caller, see
        :func:`aind_code_ocean_pipeline_utils.process.install_shutdown_handlers`).
    """
    _validate(
        jobs,
        max_in_flight=max_in_flight,
        start_retries=start_retries,
        start_retry_backoff_s=start_retry_backoff_s,
        capture_retries=capture_retries,
        resume_owner=resume_owner,
    )
    return _Dispatcher(
        _without_transport_retries(client),
        jobs,
        max_in_flight=max_in_flight,
        launch_interval_s=launch_interval_s,
        start_retries=start_retries,
        start_retry_backoff_s=start_retry_backoff_s,
        is_failed_start=is_failed_start,
        capture_retries=capture_retries,
        reuse=reuse,
        poll_interval_s=poll_interval_s,
        timeout_s=timeout_s,
        cancel_on_failure=cancel_on_failure,
        resume_owner=resume_owner,
        on_outcome=on_outcome,
    ).run()


def _without_transport_retries(client: CodeOcean) -> CodeOcean:
    """Return *client*, or a copy with ``retries=0`` if it retries requests itself.

    The copy opens a new session, so anything mounted on the original's ``session``
    by hand is not carried over.
    """
    if client.retries in (0, None):
        return client
    logger.info("using a copy of the client with retries=0 (was %r); run_jobs retries what is safe", client.retries)
    return dataclasses.replace(client, retries=0)


def _validate(
    jobs: Sequence[Job],
    *,
    max_in_flight: int | None,
    start_retries: int,
    start_retry_backoff_s: float,
    capture_retries: int,
    resume_owner: str | None,
) -> None:
    labels = [job.label for job in jobs]
    if len(set(labels)) != len(labels):
        raise ValueError(f"job labels must be unique: {sorted(labels)}")
    keys = [job.key for job in jobs if job.key is not None]
    if len(set(keys)) != len(keys):
        raise ValueError("job keys must be unique within a batch")
    if max_in_flight is not None and max_in_flight < 1:
        raise ValueError(f"max_in_flight must be at least 1, got {max_in_flight}")
    if start_retries < 0 or capture_retries < 0 or start_retry_backoff_s < 0:
        raise ValueError("retry counts and back-off cannot be negative")
    if resume_owner is not None and "owner" not in {f.name for f in dataclasses.fields(Computation)}:
        raise ValueError("resume_owner needs codeocean 0.15 or later, whose computations carry an owner")


def _prune(value: Any) -> Any:
    """Drop ``None`` and empty containers, recursively."""
    if isinstance(value, dict):
        pruned = {k: _prune(v) for k, v in value.items()}
        return {k: v for k, v in pruned.items() if v not in (None, [], {})}
    if isinstance(value, list):
        return [_prune(v) for v in value]
    return value


def _retry_after(exc: CodeOceanError) -> float | None:
    """Seconds from a ``Retry-After`` header; HTTP-date values are ignored."""
    response = getattr(exc.http_err, "response", None)
    header = response.headers.get("Retry-After") if response is not None else None
    try:
        return max(0.0, float(header)) if header is not None else None
    except ValueError:
        return None


def _retry_delay(exc: Exception, attempt: int, *, idempotent: bool) -> float | None:
    """Seconds to wait before retrying the call that raised *exc*, or ``None`` if it must not be."""
    backoff = min(_BACKOFF_CAP_S, _BACKOFF_BASE_S * 2**attempt)
    if isinstance(exc, CodeOceanError):
        if exc.status_code == _RATE_LIMITED:
            after = _retry_after(exc)
            return backoff if after is None else after
        return backoff if idempotent and exc.status_code in _TRANSIENT_SERVER else None
    return backoff if idempotent and isinstance(exc, requests.RequestException) else None


def _is_ambiguous(exc: Exception) -> bool:
    """Whether a failed POST may still have taken effect."""
    if isinstance(exc, CodeOceanError):
        return exc.status_code in _TRANSIENT_SERVER
    return isinstance(exc, requests.RequestException)


def _describe(computation: Computation) -> str:
    return (
        f"computation {computation.id} ended state={computation.state} end_status={computation.end_status} "
        f"exit_code={computation.exit_code} has_results={computation.has_results}"
    )


class _Phase(Enum):
    PENDING = auto()
    RUNNING = auto()
    CAPTURING = auto()
    DONE = auto()


@dataclass
class _Slot:
    job: Job
    phase: _Phase = _Phase.PENDING
    next_at: float = 0.0
    computation_ids: list[str] = field(default_factory=list)
    launched_at: float = 0.0
    asset_id: str | None = None
    # Consecutive retryable errors on the current call; drives backoff.
    api_errors: int = 0
    start_retries_used: int = 0
    capture_retries_used: int = 0
    reuse_checked: bool = False
    # Whether the current computation's name carries the key marker yet.
    marked: bool = True
    resumed: bool = False
    outcome: Outcome | None = None


class _Dispatcher:
    def __init__(
        self,
        client: CodeOcean,
        jobs: Sequence[Job],
        *,
        max_in_flight: int | None,
        launch_interval_s: float,
        start_retries: int,
        start_retry_backoff_s: float,
        is_failed_start: Callable[[Computation], bool],
        capture_retries: int,
        reuse: bool,
        poll_interval_s: float,
        timeout_s: float | None,
        cancel_on_failure: bool | Callable[[Outcome], bool],
        resume_owner: str | None,
        on_outcome: Callable[[Outcome], None] | None,
    ) -> None:
        self.client = client
        self.slots = [_Slot(job) for job in jobs]
        self.max_in_flight = max_in_flight
        self.launch_interval_s = launch_interval_s
        self.start_retries = start_retries
        self.start_retry_backoff_s = start_retry_backoff_s
        self.is_failed_start = is_failed_start
        self.capture_retries = capture_retries
        self.reuse = reuse
        self.poll_interval_s = poll_interval_s
        self.timeout_s = timeout_s
        self.cancel_on_failure = cancel_on_failure
        self.resume_owner = resume_owner
        self.on_outcome = on_outcome
        # Per capsule or pipeline id: its computations carrying a key marker, by key.
        self.resumable: dict[str, dict[str, list[Computation]]] = {}
        self.cancelled_by: str | None = None
        self.next_launch_at = 0.0

    def run(self) -> list[Outcome]:
        try:
            while any(slot.phase is not _Phase.DONE for slot in self.slots):
                check_shutdown()
                self._check_reuse()
                self._launch_ready()
                for slot in self.slots:
                    if slot.phase is _Phase.RUNNING and slot.next_at <= _now():
                        self._poll_computation(slot)
                    elif slot.phase is _Phase.CAPTURING and slot.next_at <= _now():
                        self._poll_asset(slot)
                self._sleep_until_due()
        except GracefulExit as exc:
            in_flight = [slot.computation_ids[-1] for slot in self.slots if slot.phase is _Phase.RUNNING]
            finished = [slot.outcome for slot in self.slots if slot.outcome is not None]
            logger.error("interrupted; computations left running: %s", ", ".join(in_flight) or "none")
            raise DispatchInterrupted(exc.signum, in_flight=in_flight, finished=finished) from exc
        return [slot.outcome for slot in self.slots if slot.outcome is not None]

    # -- scheduling ---------------------------------------------------------

    def _in_flight(self) -> int:
        return sum(slot.phase is _Phase.RUNNING for slot in self.slots)

    def _launch_ready(self) -> None:
        for slot in self.slots:
            if slot.phase is not _Phase.PENDING or slot.next_at > _now():
                continue
            if self._searches_first() and slot.job.key is not None and not slot.reuse_checked:
                continue  # still waiting on its reuse or resume search
            if self.max_in_flight is not None and self._in_flight() >= self.max_in_flight:
                return
            if _now() < self.next_launch_at:
                return
            self._launch(slot)

    def _sleep_until_due(self) -> None:
        now = _now()
        pending = any(slot.phase is _Phase.PENDING for slot in self.slots)
        due = [slot.next_at for slot in self.slots if slot.phase is not _Phase.DONE and slot.next_at > now]
        if pending and self.next_launch_at > now:
            due.append(self.next_launch_at)
        if not due:
            return
        _sleep(min(_MAX_SLEEP_S, min(due) - now))

    def _finish(self, slot: _Slot, status: Status, *, error: str | None = None, failure: Failure | None = None) -> None:
        slot.phase = _Phase.DONE
        slot.outcome = Outcome(
            job=slot.job,
            status=status,
            asset_id=slot.asset_id,
            computation_ids=tuple(slot.computation_ids),
            error=error,
            failure=failure,
            resumed=slot.resumed,
        )
        if error is None:
            logger.info("%s: %s", slot.job.label, status)
        else:
            logger.error("%s: %s: %s", slot.job.label, status, error)
        if self.on_outcome is not None:
            try:
                self.on_outcome(slot.outcome)
            # Broad on purpose: the caller's hook must not orphan the other jobs' computations.
            except Exception:
                logger.exception("%s: on_outcome raised", slot.job.label)
        if status == "failed" and self.cancelled_by is None and self._should_cancel(slot.outcome):
            self._cancel_rest(slot.job.label)

    def _should_cancel(self, outcome: Outcome) -> bool:
        if callable(self.cancel_on_failure):
            return self.cancel_on_failure(outcome)
        return self.cancel_on_failure

    def _cancel_rest(self, failed_label: str) -> None:
        """Stop every job not yet past its computation; captures under way finish."""
        self.cancelled_by = failed_label
        to_cancel = [slot for slot in self.slots if slot.phase in (_Phase.PENDING, _Phase.RUNNING)]
        if to_cancel:
            logger.warning("%s failed; cancelling %d other job(s)", failed_label, len(to_cancel))
        for slot in to_cancel:
            error = f"cancelled after {failed_label} failed"
            if slot.phase is _Phase.RUNNING:
                computation_id = slot.computation_ids[-1]
                try:
                    # Code Ocean has no stop call; deleting stops a running computation.
                    self.client.computations.delete_computation(computation_id)
                except (CodeOceanError, requests.RequestException) as exc:
                    error += f"; could not stop computation {computation_id}: {exc}"
            self._finish(slot, "cancelled", error=error)

    def _call(self, slot: _Slot, what: str, fn: Callable[[], _T], *, idempotent: bool) -> _T | None:
        """Run one API call for *slot*; on error, schedule a retry or fail the job.

        Returns ``None`` when the call did not succeed; the slot is then either
        rescheduled (``next_at`` set) or done.
        """
        try:
            result = fn()
        except (CodeOceanError, requests.RequestException) as exc:
            delay = _retry_delay(exc, slot.api_errors, idempotent=idempotent)
            if delay is None:
                ambiguous = _is_ambiguous(exc)
                note = " It may have taken effect anyway; check before rerunning." if ambiguous else ""
                self._finish(
                    slot,
                    "failed",
                    error=f"{what} failed: {exc}.{note}",
                    failure="api_unavailable" if ambiguous else "api_rejected",
                )
                return None
            slot.api_errors += 1
            slot.next_at = _now() + delay
            logger.warning("%s: %s failed (%s); retrying in %.0f s", slot.job.label, what, exc, delay)
            return None
        slot.api_errors = 0
        return result

    # -- phases -------------------------------------------------------------

    def _searches_first(self) -> bool:
        return self.reuse or self.resume_owner is not None

    def _check_reuse(self) -> None:
        """Resolve pending keyed jobs to existing captures or computations, without using launch capacity."""
        if not self._searches_first():
            return
        for slot in self.slots:
            if slot.phase is not _Phase.PENDING or slot.reuse_checked or slot.job.key is None:
                continue
            if slot.next_at > _now():
                continue
            tag = key_tag(slot.job.key)
            params = DataAssetSearchParams(query=f"tag:{tag}", ownership=Ownership.Created, limit=100)
            try:
                results = self.client.data_assets.search_data_assets(params)
            except (CodeOceanError, requests.RequestException) as exc:
                delay = _retry_delay(exc, slot.api_errors, idempotent=True)
                if delay is not None:
                    slot.api_errors += 1
                    slot.next_at = _now() + delay
                    continue
                # Reuse and resume are optimisations: launching is still correct.
                logger.warning("%s: reuse search failed (%s); launching", slot.job.label, exc)
                slot.reuse_checked = True
                continue
            slot.api_errors = 0
            # Tag search may tokenize, so confirm the exact tag client-side.
            tagged: list[DataAsset] = [asset for asset in results.results if tag in (asset.tags or [])]
            ready = [asset for asset in tagged if asset.state == DataAssetState.Ready]
            capturing = [asset for asset in tagged if asset.state not in (DataAssetState.Ready, DataAssetState.Failed)]
            if self.reuse and ready:
                slot.reuse_checked = True
                slot.asset_id = max(ready, key=lambda asset: asset.created).id
                logger.info("%s: reusing captured asset %s", slot.job.label, slot.asset_id)
                self._finish(slot, "reused")
            elif self.resume_owner is not None and capturing:
                slot.reuse_checked = True
                slot.asset_id = max(capturing, key=lambda asset: asset.created).id
                slot.resumed = True
                slot.phase = _Phase.CAPTURING
                slot.next_at = _now()
                logger.info("%s: resuming capture %s", slot.job.label, slot.asset_id)
            elif self.resume_owner is not None:
                # A succeeded computation already captured is not resumed: with reuse
                # off, the caller asked for a new run, not a second capture.
                self._resume_computation(slot, finished_too=not ready)
            else:
                slot.reuse_checked = True

    def _resume_computation(self, slot: _Slot, *, finished_too: bool) -> None:
        """Follow the owner's running, or else succeeded, computation of *slot*'s job, if there is one."""
        assert slot.job.key is not None
        source = slot.job.run_params.capsule_id or slot.job.run_params.pipeline_id
        if source is None:
            slot.reuse_checked = True
            return
        if source not in self.resumable:
            listing = self._list_resumable(slot, source)
            if listing is None:
                return  # retrying, or done
            self.resumable[source] = listing
        slot.reuse_checked = True
        candidates = self.resumable[source].get(slot.job.key, [])
        live = [c for c in candidates if c.state not in (ComputationState.Completed, ComputationState.Failed)]
        done = [c for c in candidates if succeeded(c)] if finished_too else []
        if not (live or done):
            return
        # A live computation is newer than any finished one worth resuming.
        computation = max(live or done, key=lambda c: c.created)
        slot.computation_ids.append(computation.id)
        slot.resumed = True
        slot.phase = _Phase.RUNNING
        # Timeouts count from here: how long it ran before is unknown to this batch.
        slot.launched_at = _now()
        slot.next_at = _now()
        logger.info("%s: resuming computation %s (%s)", slot.job.label, computation.id, computation.state)

    def _list_resumable(self, slot: _Slot, source: str) -> dict[str, list[Computation]] | None:
        """Index the owner's computations of *source* by the key their name carries."""
        lister = (
            self.client.capsules.list_computations
            if slot.job.run_params.capsule_id
            else self.client.pipelines.list_computations
        )
        try:
            computations = lister(source)
        except (CodeOceanError, requests.RequestException) as exc:
            delay = _retry_delay(exc, slot.api_errors, idempotent=True)
            if delay is not None:
                slot.api_errors += 1
                slot.next_at = _now() + delay
                return None
            logger.warning("%s: listing computations of %s failed (%s); launching", slot.job.label, source, exc)
            return {}
        slot.api_errors = 0
        keys = {job.key for job in (s.job for s in self.slots) if job.key is not None}
        index: dict[str, list[Computation]] = {}
        for computation in computations:
            if self.resume_owner not in (
                getattr(computation, "owner", None),
                getattr(computation, "owner_email", None),
            ):
                continue
            for key in keys:
                if key_marker(key) in (computation.name or ""):
                    index.setdefault(key, []).append(computation)
        return index

    def _launch(self, slot: _Slot) -> None:
        computation = self._call(
            slot, "launch", lambda: self.client.computations.run_capsule(slot.job.run_params), idempotent=False
        )
        if computation is None:
            if slot.phase is _Phase.PENDING:
                # The rate limit is per user, so hold every launch, not just this one.
                self.next_launch_at = max(self.next_launch_at, slot.next_at)
            return
        slot.computation_ids.append(computation.id)
        slot.phase = _Phase.RUNNING
        slot.launched_at = _now()
        slot.next_at = slot.launched_at + self.poll_interval_s
        self.next_launch_at = slot.launched_at + self.launch_interval_s
        logger.info("%s: launched %s (attempt %d)", slot.job.label, computation.id, len(slot.computation_ids))
        slot.marked = slot.job.key is None
        if not slot.marked:
            self._mark(slot)

    def _mark(self, slot: _Slot) -> None:
        """Name the computation after its job and key, so a later batch can resume it.

        Best effort: until it succeeds, each status check tries again, and a
        computation left unnamed is relaunched rather than resumed.
        """
        assert slot.job.key is not None
        computation_id = slot.computation_ids[-1]
        name = f"{slot.job.label} {key_marker(slot.job.key)}"
        try:
            self.client.computations.rename_computation(computation_id, name)
        except (CodeOceanError, requests.RequestException) as exc:
            logger.warning("%s: naming computation %s failed (%s); trying again", slot.job.label, computation_id, exc)
            return
        slot.marked = True

    def _poll_computation(self, slot: _Slot) -> None:
        if not slot.marked:
            self._mark(slot)
        computation_id = slot.computation_ids[-1]
        computation = self._call(
            slot, "status check", lambda: self.client.computations.get_computation(computation_id), idempotent=True
        )
        if computation is None:
            return
        if computation.state not in (ComputationState.Completed, ComputationState.Failed):
            if self.timeout_s is not None and _now() - slot.launched_at > self.timeout_s:
                self._finish(
                    slot,
                    "failed",
                    error=f"timed out after {self.timeout_s:.0f} s; computation {computation_id} left running",
                    failure="timeout",
                )
            else:
                slot.next_at = _now() + self.poll_interval_s
            return
        if succeeded(computation):
            if slot.job.capture is None:
                self._finish(slot, "completed")
            else:
                self._start_capture(slot, computation)
        elif self.is_failed_start(computation) and slot.start_retries_used < self.start_retries:
            slot.start_retries_used += 1
            slot.phase = _Phase.PENDING
            backoff_s = self.start_retry_backoff_s * 2 ** (slot.start_retries_used - 1)
            slot.next_at = _now() + backoff_s
            self.next_launch_at = max(self.next_launch_at, slot.next_at)
            logger.warning("%s: %s; relaunching in %.0f s", slot.job.label, _describe(computation), backoff_s)
        else:
            failure: Failure = "failed_start" if self.is_failed_start(computation) else "computation"
            self._finish(slot, "failed", error=_describe(computation), failure=failure)

    def _start_capture(self, slot: _Slot, computation: Computation) -> None:
        assert slot.job.capture is not None
        try:
            params = slot.job.capture(computation)
        # Broad on purpose: one job's bad callback must not orphan the others' computations.
        except Exception as exc:
            logger.exception("%s: capture callback raised", slot.job.label)
            self._finish(
                slot,
                "failed",
                error=f"capture callback raised for computation {computation.id}: {exc!r}",
                failure="capture_callback",
            )
            return
        if slot.job.key is not None and key_tag(slot.job.key) not in params.tags:
            params = dataclasses.replace(params, tags=[*params.tags, key_tag(slot.job.key)])
        asset = self._call(
            slot,
            f"capture of computation {computation.id}",
            lambda: self.client.data_assets.create_data_asset(params),
            idempotent=False,
        )
        # On a retryable error the slot stays RUNNING; the next poll sees the same
        # finished computation and comes back here.
        if asset is None:
            return
        slot.asset_id = asset.id
        slot.phase = _Phase.CAPTURING
        slot.next_at = _now() + _ASSET_POLL_S
        logger.info("%s: capturing %s as %s", slot.job.label, computation.id, asset.id)

    def _poll_asset(self, slot: _Slot) -> None:
        asset_id = slot.asset_id
        assert asset_id is not None
        asset = self._call(
            slot,
            f"status check of asset {asset_id}",
            lambda: self.client.data_assets.get_data_asset(asset_id),
            idempotent=True,
        )
        if asset is None:
            return
        if asset.state == DataAssetState.Ready:
            self._finish(slot, "captured")
        elif asset.state == DataAssetState.Failed:
            if slot.capture_retries_used < self.capture_retries:
                slot.capture_retries_used += 1
                slot.asset_id = None
                slot.phase = _Phase.RUNNING
                slot.next_at = _now()
                logger.warning("%s: capture %s failed; capturing again", slot.job.label, asset_id)
            else:
                self._finish(
                    slot,
                    "failed",
                    error=f"capture {asset_id} of computation {slot.computation_ids[-1]} failed",
                    failure="capture",
                )
        else:
            slot.next_at = _now() + _ASSET_POLL_S
