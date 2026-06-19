# Development

## Architecture: Hexagonal (Ports & Adapters)

The codebase follows the hexagonal architecture pattern used in the Go kubelet source, mapping Go packages to Rust modules:

```
┌────────────────────────────────────────────────────────────┐
│                        Binary                              │
│                      src/main.rs                           │
└─────────────────────────┬──────────────────────────────────┘
                          │
┌─────────────────────────▼──────────────────────────────────┐
│                    kubelet-app                             │
│   Application layer: wires domain + ports + adapters       │
│   - cli.rs         (argument parsing)                      │
│   - server.rs      (HTTP API: /healthz, /pods, /metrics)   │
│   - sync_loop.rs   (pod reconciliation loop)               │
│   - metrics.rs     (Prometheus metrics)                    │
└──────┬──────────────────┬──────────────────────────────────┘
       │                  │
┌──────▼──────┐  ┌────────▼─────────────────────────────────┐
│kubelet-ports│  │         kubelet-adapters                 │
│  (Interfaces│  │  (Concrete implementations)              │
│   / Traits) │  │  - mock_runtime  (test CRI)              │
│             │  │  - file_config   (static pod source)     │
│ Driving:    │  │  - kube_client   (API server reporter)   │
│  kubelet_api│  │  - eviction      (resource pressure)     │
│             │  │  - volume        (EmptyDir/HostPath)     │
│ Driven:     │  │  - network       (no-op CNI)             │
│  container_ │  │  - prober        (liveness/readiness)    │
│  runtime    │  └──────────────────────────────────────────┘
│  network    │
│  storage    │
│  node_      │
│  reporter   │
│  pod_source │
└──────┬──────┘
       │
┌──────▼───────────────────────────────────────────────────┐
│                    kubelet-core                          │
│              Domain layer (pure Rust, no I/O)            │
│   - pod/       (PodSpec, lifecycle FSM, manager, sync)   │
│   - container/ (RuntimeContainer, ContainerID)           │
│   - node/      (NodeStatus, conditions, capacity)        │
│   - config/    (KubeletConfig with defaults + validation)│
│   - qos/       (BestEffort / Burstable / Guaranteed)     │
│   - types/     (PodUID, PodRef, ResourceQuantity)        │
│   - error.rs   (KubeletError enum)                       │
└──────────────────────────────────────────────────────────┘
```

## Crate Structure

| Crate                | Role                                                           |
| -------------------- | -------------------------------------------------------------- |
| `kubelet-core`     | Pure domain: types, lifecycle FSM, pod manager, QoS            |
| `kubelet-ports`    | Port traits: CRI, CNI, storage, node reporter, pod source      |
| `kubelet-adapters` | Concrete adapters: mock CRI, file pod source, eviction, volume |
| `kubelet-app`      | Application: sync loop, HTTP server, CLI, metrics              |
| `kubelet` (root)   | Binary entry point                                             |

## Go → Rust Mapping

| Go package                        | Rust module                                       |
| --------------------------------- | ------------------------------------------------- |
| `cmd/kubelet/app`               | `kubelet-app` crate                             |
| `pkg/kubelet/kubelet.go`        | `kubelet-app/src/lib.rs`                        |
| `pkg/kubelet/pod`               | `kubelet-core/src/pod/`                         |
| `pkg/kubelet/container`         | `kubelet-core/src/container/`                   |
| `pkg/kubelet/config`            | `kubelet-core/src/config/`                      |
| `pkg/kubelet/qos`               | `kubelet-core/src/qos/`                         |
| `pkg/kubelet/eviction`          | `kubelet-adapters/src/eviction/`                |
| `pkg/kubelet/prober`            | `kubelet-adapters/src/prober/`                  |
| `pkg/kubelet/volumemanager`     | `kubelet-adapters/src/volume/`                  |
| `pkg/kubelet/network`           | `kubelet-adapters/src/network/`                 |
| `pkg/kubelet/server`            | `kubelet-app/src/server.rs`                     |
| `pkg/kubelet/metrics`           | `kubelet-app/src/metrics.rs`                    |
| CRI (container runtime interface) | `kubelet-ports/src/driven/container_runtime.rs` |
| CNI (network plugin)              | `kubelet-ports/src/driven/network.rs`           |
