# Kubernetes Golang E2E Tests

This directory contains tooling to run upstream Kubernetes Go test suites
against an already-provisioned cluster.

## Local usage

### Against the existing Colima VM (macOS)

The `colima-go-e2e.sh` script reuses the kubeadm cluster provisioned by
`hack/e2e/colima-run.sh`. The cluster runs entirely inside the Colima VM, so
the script SSHes in, copies `run-upstream-go-e2e.sh` there, and runs it inside
the VM where the API server endpoint (`127.0.0.1:6443`) is reachable and test
binaries are the correct linux architecture.

Provision the cluster first (if not already done):

```bash
bash hack/e2e/colima-run.sh
```

Then run the Go e2e/conformance suite against it:

```bash
# Conformance only (default)
bash hack/e2e/kubernetes/colima-go-e2e.sh

# sig-node e2e slice
RUN_CONFORMANCE=0 RUN_E2E=1 E2E_FOCUS='\[sig-node\]' \
bash hack/e2e/kubernetes/colima-go-e2e.sh

# Custom Colima profile
COLIMA_PROFILE=my-profile bash hack/e2e/kubernetes/colima-go-e2e.sh
```

Artifacts are pulled back to `$LOCAL_ARTIFACT_DIR`
(default `/tmp/k8s-upstream-go-e2e-colima-artifacts`) after the run.

### Against any cluster

Run conformance only:

```bash
KUBECONFIG=/path/to/kubeconfig \
RUN_CONFORMANCE=1 \
RUN_E2E=0 \
bash hack/e2e/kubernetes/run-upstream-go-e2e.sh
```

Run conformance plus a focused e2e slice:

```bash
KUBECONFIG=/path/to/kubeconfig \
RUN_CONFORMANCE=1 \
RUN_E2E=1 \
E2E_FOCUS='\[sig-node\]' \
bash hack/e2e/kubernetes/run-upstream-go-e2e.sh
```

The runner downloads upstream test binaries from `dl.k8s.io` and does not run
any kube-air Rust test targets.

## CI usage

Use the dispatch-only workflow:

- `.github/workflows/k8s-go-e2e.yml`

That workflow is intentionally separate from the Rust e2e workflow and only
runs upstream Kubernetes Go e2e/conformance suites.
