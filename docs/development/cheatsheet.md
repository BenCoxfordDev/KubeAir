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

## Local Colima E2E

Use [hack/e2e/colima-run.sh](hack/e2e/colima-run.sh) to provision a single-node Kubernetes cluster in a Colima VM (containerd + kubeadm + Calico) and run the full test suite.

```bash
bash hack/e2e/colima-run.sh
```

Skip the binary rebuild and repo sync if you've already built:

```bash
SKIP_BUILD_BINARY=1 bash hack/e2e/colima-run.sh
```
