# Kubernetes Golang E2E Tests

This directory contains tooling to run upstream Kubernetes Go test suites
against an already-provisioned cluster.

## Local usage

SSH to the container running the cluster (while `just e2e` is active):

```bash
podman exec -it $(podman ps -q) bash
```

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
