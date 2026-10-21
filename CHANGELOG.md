# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.6.0 (2026-10-21)

### Feat

- report each job's outcome as it finishes and resume running computations by key

## v0.5.0 (2026-10-15)

### Feat

- add a post-run automation that captures runs as internal assets

### Fix

- make check script exit nonzero when any step fails

## v0.4.0 (2026-10-15)

### Feat

- classify failures and optionally cancel the batch on one

## v0.3.0 (2026-10-15)

### Feat

- add capture-naming and capsule-commit helpers
- back off before relaunching a failed start

### Fix

- recognise failed starts by end_status, not state

## v0.2.2 (2026-10-15)

### Fix

- run on a non-retrying copy of the caller's client

## v0.2.1 (2026-10-15)

### Fix

- support codeocean 0.13 and reuse only the caller's own captures

## v0.2.0 (2026-10-15)

### Feat

- add run_jobs to launch, monitor, and capture batches of runs
