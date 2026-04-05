# Benchmarking

`chainrep` includes a separate benchmark operator CLI:

- binary: `chainrep-bench`
- entrypoint: [`cmd/chainrep-bench/main.go`](./cmd/chainrep-bench/main.go)
- default profile: [`profiles/bench/aws_i8ge_steady.yaml`](./profiles/bench/aws_i8ge_steady.yaml)

This is the tool that creates AWS infrastructure, runs the benchmark, pulls
artifacts locally, analyzes the run, and tears the stack down.

## Build

```bash
go build -o ./bin/chainrep-bench ./cmd/chainrep-bench
```

## Prerequisites

Install locally:

- `go`
- `terraform`
- `aws`
- `ssh`
- `scp`

Provide AWS credentials in:

```bash
.env.local
```

The benchmark runner reads `.env.local` itself. It does not require you to
`source` the file in your shell.

## Main Commands

Create infra from scratch, run the benchmark, pull artifacts, and destroy on
success:

```bash
./bin/chainrep-bench run \
  --profile profiles/bench/aws_i8ge_steady.yaml \
  --run-name my-bench
```

Useful run overrides:

```bash
./bin/chainrep-bench run \
  --profile profiles/bench/aws_i8ge_steady.yaml \
  --region us-east-1 \
  --topology multi-az \
  --client-placement remote-az \
  --run-name multi-az-worst-case
```

## Run Overrides

Current `run` command overrides:

- `--profile`
  - default: `profiles/bench/aws_i8ge_steady.yaml`
  - selects the benchmark profile YAML
- `--region`
  - default: value from the profile
  - overrides the AWS region for this run
- `--topology`
  - values: `single-az`, `multi-az`
  - default: `single-az`
  - controls whether the stack is placed in one AZ or spread across three AZs
- `--client-placement`
  - values: `same-az`, `remote-az`
  - default: `same-az`
  - relevant for `--topology multi-az`; `remote-az` places the client outside the primary AZ for a worse client path
- `--run-name`
  - default: `bench`
  - used as the human-readable prefix for the generated run ID

You can inspect the exact flag surface from:

- [`cmd/chainrep-bench/main.go`](./cmd/chainrep-bench/main.go)

The current `destroy` and `analyze` commands each take:

- `--run-dir`
  - points at an existing local run directory under `artifacts/benchmarks/...`

Force teardown for a prior run directory:

```bash
./bin/chainrep-bench destroy \
  --run-dir artifacts/benchmarks/<run-id>
```

Analyze an already-pulled run directory:

```bash
./bin/chainrep-bench analyze \
  --run-dir artifacts/benchmarks/<run-id>
```

## Run Layout

Each run gets its own local directory under:

```text
artifacts/benchmarks/<run-id>/
```

That directory contains:

- local Terraform working directory and state
- rendered manifest and configs
- run metadata and run state
- pulled artifacts
- generated analysis output

The most important files are:

- `run-state.json`
- `run-metadata.json`
- `artifacts/artifact-manifest.json`
- `artifacts/artifacts.tar.gz`
- `analysis/summary.json`
- `analysis/index.html`

## Safety Notes

- `terraform destroy` is scoped to the benchmark run's local Terraform state.
- The benchmark code does not perform separate AWS delete or terminate calls
  outside Terraform.
- The post-destroy AWS CLI call is read-only and is used only to audit whether
  any resources tagged with the run ID are still present.
- If artifact verification fails, the run is marked `needs_cleanup` and infra is
  intentionally preserved so you can recover data before running `destroy`.

## Current Scope

The current benchmark implementation is:

- AWS only
- steady-state only
- designed around `i8ge.6xlarge` storage nodes
- one coordinator, three storage nodes, one client
- storage-node data directories live on mounted local NVMe under `/var/lib/chainrep-bench/storage-data`

It is not currently a failure-injection benchmark.
