# AIND Code Ocean dispatch

![CI](https://github.com/AllenNeuralDynamics/aind-codeocean-dispatch/actions/workflows/ci-call.yml/badge.svg)
[![PyPI - Version](https://img.shields.io/pypi/v/aind-codeocean-dispatch)](https://pypi.org/project/aind-codeocean-dispatch/)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Copier](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/copier-org/copier/master/img/badge/badge-grayscale-inverted-border.json)](https://github.com/copier-org/copier)

Launch, monitor, and capture batches of Code Ocean capsule runs

## Installation

If you choose to clone the repository, you can install the package by running the following command from the root directory of the repository:

```bash
pip install .
```

Otherwise, you can use pip:

```bash
pip install aind-codeocean-dispatch
```

## Usage

### Running a batch of runs

`run_jobs` launches a batch of capsule or pipeline runs, waits for them from one polling
loop, and captures each successful run as a data asset:

```python
from codeocean import CodeOcean
from codeocean.computation import DataAssetsRunParam, NamedRunParam, RunParams

from aind_codeocean_dispatch import Job, capture_as, job_key, raise_for_failures, run_jobs

client = CodeOcean(domain="https://codeocean.allenneuraldynamics.org", token=token)

jobs = []
for stream in streams:
    params = RunParams(
        capsule_id=WORKER_CAPSULE_ID,
        data_assets=[DataAssetsRunParam(id=raw_asset_id, mount="raw")],
        named_parameters=[NamedRunParam(param_name="stream", value=stream)],
    )
    jobs.append(
        Job(
            label=stream,
            run_params=params,
            capture=capture_as(f"{session}_{stream}_sorted", tags=["sorted"]),
            key=job_key(params),
        )
    )

outcomes = run_jobs(client, jobs, launch_interval_s=1, start_retries=3, reuse=True)
raise_for_failures(outcomes)
asset_ids = {o.job.label: o.asset_id for o in outcomes}
```

A failed job does not raise; each `Outcome` carries its `status`, `asset_id`, every
`computation_ids` attempt, and on failure an `error` and a `failure` kind.
`raise_for_failures` turns failures into one `DispatchFailed`.

Behaviour worth knowing:

- **Failed starts.** Code Ocean sometimes fails to start a run (`end_status=Failed`, no
  results). Up to `start_retries` relaunches follow, after `start_retry_backoff_s` (60 s)
  doubling each time, and every pending launch waits too, since the cause is usually
  machine capacity.
- **Rate limits and errors.** 429s are retried honouring `Retry-After`. Other 5xx and
  connection errors are retried on reads only: a launch or capture POST may already have
  taken effect, so resending it could duplicate the computation or asset.
- **Pacing.** `max_in_flight` caps concurrent runs and `launch_interval_s` spaces
  launches; both are off by default.
- **Reuse.** A job's `key` is added to its asset as a tag. With `reuse=True`, a Ready
  asset you created with that tag is returned instead of launching. `job_key` fingerprints
  the launch request; pass `salt=` for what the request cannot show, such as the commit
  of an unreleased capsule (`capsule_head_commit(client, capsule_id)` reads it inside a
  run).
- **Cancelling a doomed batch.** `cancel_on_failure=True`, or a predicate on the failed
  `Outcome` such as `lambda o: not o.transient`, stops launching and stops and deletes
  running computations; captures already under way finish.
- **Capture names.** `capture_as` uses a fixed name. `capture_named_by_data_description`
  uses the name a derived `data_description.json` in the results declares, and otherwise
  a fallback name plus the capture time.

### Capturing runs from a post-run automation

A Code Ocean pipeline can name an automation capsule that runs after every run. A capsule
whose `code/run` is

```bash
#!/usr/bin/env bash
set -e
python -m aind_codeocean_dispatch.automation
```

captures each successful run as an internal data asset, named by the results'
`data_description.json` when it declares a derived name and otherwise after the first
input asset. It needs only `CODEOCEAN_TOKEN`; `CAPTURE_TAGS`, `CAPTURE_FAILED` and
`CAPTURE_PERMISSIONS` adjust it. It registers nothing in DocDB and writes no external
bucket, so pipelines that publish use `asset-creation-capsule` instead.

## Contributing

Setup, checks, tests and conventions are in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
