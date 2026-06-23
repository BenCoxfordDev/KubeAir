# Cheatsheet

## Building

```bash
# Build for the local machine
just build

# Cross-compile for Linux amd64
just build amd64

# Cross-compile for Linux arm64
just build arm64

# Run all tests
just test

# Run specific suites
just conformance
just smoke

# Run individual integration tests
just test //tests/integration:container_state_test
just test //tests/integration:pod_lifecycle_test
just test //tests/integration:runtime_network_storage_test
just test //tests/integration:resource_orchestration_test

# Lint and format check
just verify

# Format code
just fmt
```

## Running Benchmarks

```bash
just bench
```

## Local E2E

Use [hack/e2e/run-e2e.sh](hack/e2e/run-e2e.sh) to provision a single-node k3s cluster in a privileged container (podman or docker) and run the full test suite. Requires podman (preferred) or docker.

```bash
just e2e
```

Skip the binary rebuild if you've already built:

```bash
SKIP_BUILD=1 bash hack/e2e/run-e2e.sh
```
