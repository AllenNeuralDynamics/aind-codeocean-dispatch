"""Launch, monitor, and capture batches of Code Ocean capsule runs."""

from importlib.metadata import PackageNotFoundError, version

from .capsule_git import capsule_head_commit, head_commit, run_owner_email
from .capture import capture_as, capture_named_by_data_description, derived_name
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
    "capsule_head_commit",
    "capture_as",
    "capture_named_by_data_description",
    "derived_name",
    "failed_without_results",
    "head_commit",
    "job_key",
    "key_tag",
    "raise_for_failures",
    "run_jobs",
    "run_owner_email",
    "succeeded",
]

try:
    __version__ = version("aind-codeocean-dispatch")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"
