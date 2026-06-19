## Post-Phase 72 Gate: Phase 73 (Production Rollout Criteria)

This phase is complete only when every gate below is satisfied and recorded.

## 1) Conformance and Environment Parity

Exit criteria:

- Kubernetes node conformance passes in CI on a real containerd + CNI environment.
- No unresolved deviations from Go kubelet behavior remain undocumented.
- Every intentional deviation has owner approval and operational mitigation.

Evidence to record:

- CI run URL and commit SHA for latest passing run.
- Exact conformance invocation and environment details.
- Link to deviation document updates.

## 2) Operational Safety, Canary, and Rollback

### 2.1 Canary stages

Roll out in order:

1. Stage A: 1 node.
2. Stage B: 5% of nodes (minimum 3 nodes).
3. Stage C: 25% of nodes.
4. Stage D: 100% of target pool.

Minimum soak at each stage:

- Stage A: 24 hours
- Stage B: 48 hours
- Stage C: 72 hours

Promotion rule:

- Promote only if all stage stop conditions stay clear for the full soak window.

### 2.2 Stop conditions (block promotion immediately)

Stop and hold the rollout when any condition below is true for 10 consecutive minutes:

- Node Ready ratio < 99.5% in canary set.
- Pod sandbox creation failure rate > 1%.
- Probe failure rate > 2x baseline for canary workloads.
- CRI error rate > 2x baseline.
- PLEG unhealthy on any canary node for > 5 minutes.

Immediate rollback triggers:

- Any Sev-1 incident attributable to kube-air kubelet.
- Two or more Sev-2 incidents in 24 hours attributable to kube-air kubelet.
- Widespread pod startup regression: P95 pod start latency > 2x baseline for 30 minutes.

### 2.3 Rollback drill (must pass before Stage B)

Preconditions:

- Previous known-good kubelet artifact is available.
- Configuration management can switch binary/version for a node pool.
- On-call and platform owner are present for validation.

Drill steps:

1. Select one canary node currently running kube-air kubelet.
2. Cordon node.
3. Switch kubelet binary/version to last known-good release.
4. Restart kubelet service.
5. Verify node returns Ready and critical DaemonSet pods recover.
6. Uncordon node.
7. Validate no sustained error increase for 30 minutes.

Drill pass criteria:

- Recovery time objective met: node back to Ready in <= 10 minutes.
- No data-plane outage for workloads outside acceptable disruption budget.
- No unresolved critical alerts after 30-minute observation.

## 3) Reliability Soak

Exit criteria:

- 14 continuous days in staging with realistic workload churn.
- Zero Sev-1 and zero Sev-2 incidents caused by kube-air kubelet.
- Daily validation completed for pod lifecycle, restart behavior, networking, and volumes.

Evidence to record:

- Daily soak log (date, result, incident links, owner sign-off).
- Aggregated error and latency trend snapshots.

## 4) SLO and Observability Readiness

Required SLO definitions:

- Node readiness SLO.
- Pod start latency SLO (P50/P95/P99).
- Probe success SLO.
- Runtime/CRI error budget.

Required alert coverage:

- Kubelet health and restart loop detection.
- PLEG health.
- Probe failure spikes.
- OOM event spikes.
- CRI request failure spikes.

Exit criteria:

- Dashboards are published and linked in on-call docs.
- Alerts are routed, tested, and acknowledged by owners.

## 5) Security and Hardening

Exit criteria:

- Required host capabilities/permissions are documented, including behavior when absent.
- TLS, authentication, and authorization posture reviewed for node APIs and streaming endpoints.
- Any exceptions have compensating controls and owner sign-off.

## 6) Production Sign-off

Broad rollout is blocked until all parties sign off:

- Platform operations owner.
- Service owner representative.
- On-call lead.

Sign-off must include:

- Date and commit SHA.
- Confirmation all gates above are met.
- Explicit rollback owner and communication channel.

## 7) Release Checklist (Operator Copy/Paste)

Use this checklist during rollout execution:

- [ ] Conformance CI passed on target environment.
- [ ] Deviation doc reviewed and approved.
- [ ] Stage A complete with no stop conditions.
- [ ] Rollback drill passed before Stage B.
- [ ] Stage B complete with no stop conditions.
- [ ] Stage C complete with no stop conditions.
- [ ] 14-day soak complete.
- [ ] Dashboards and alerts validated.
- [ ] Security review complete.
- [ ] Joint production sign-off recorded.
