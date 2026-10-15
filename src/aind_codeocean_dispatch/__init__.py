"""Launch, monitor, and capture batches of Code Ocean capsule runs."""

from importlib.metadata import PackageNotFoundError, version

from .runner import (
    DispatchFailed,
    DispatchInterrupted,
    Job,
    Outcome,
    failed_without_results,
    job_key,
    key_tag,
    raise_for_failures,
    run_jobs,
    succeeded,
)

__all__ = [
    "DispatchFailed",
    "DispatchInterrupted",
    "Job",
    "Outcome",
    "failed_without_results",
    "job_key",
    "key_tag",
    "raise_for_failures",
    "run_jobs",
    "succeeded",
]

try:
    __version__ = version("aind-codeocean-dispatch")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"
