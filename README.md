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
