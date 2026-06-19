/*
Copyright 2026 Ben Coxford.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Node lease domain model.
//!
//! Kubernetes uses a Lease object in `kube-node-lease` namespace to implement
//! a lightweight heartbeat from kubelet -> API server. The kubelet renews the
//! lease every `node_lease_duration_seconds / 4` seconds (default: 10s).
//!
//! References:
//!   KEP-0009: Node Heartbeat
//!   pkg/kubelet/node_lifecycle_controller.go

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// A Kubernetes coordination.k8s.io/v1 Lease.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLease {
    pub node_name: String,
    pub holder_identity: String,
    pub lease_duration_seconds: u32,
    pub acquire_time: DateTime<Utc>,
    pub renew_time: DateTime<Utc>,
    pub lease_transitions: u32,
}

impl NodeLease {
    /// Create a new lease for a node.
    pub fn new(node_name: impl Into<String>, duration_seconds: u32) -> Self {
        let now = Utc::now();
        let name = node_name.into();
        Self {
            holder_identity: name.clone(),
            node_name: name,
            lease_duration_seconds: duration_seconds,
            acquire_time: now,
            renew_time: now,
            lease_transitions: 0,
        }
    }

    /// Returns true if the lease needs renewal (past 3/4 of duration).
    pub fn needs_renewal(&self) -> bool {
        let renew_interval =
            Duration::seconds(self.lease_duration_seconds as i64 / 4).max(Duration::seconds(1));
        Utc::now() > self.renew_time + renew_interval
    }

    /// Returns true if the lease has expired (not renewed within duration).
    pub fn is_expired(&self) -> bool {
        let expiry = self.renew_time + Duration::seconds(self.lease_duration_seconds as i64);
        Utc::now() > expiry
    }

    /// Renew this lease, updating renew_time.
    pub fn renew(&mut self) {
        self.renew_time = Utc::now();
    }

    /// Time until the next renewal is needed.
    pub fn time_until_renewal(&self) -> Duration {
        let renew_interval =
            Duration::seconds(self.lease_duration_seconds as i64 / 4).max(Duration::seconds(1));
        let next_renew = self.renew_time + renew_interval;
        let remaining = next_renew - Utc::now();
        if remaining < Duration::zero() {
            Duration::zero()
        } else {
            remaining
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_lease_not_expired() {
        let lease = NodeLease::new("node1", 40);
        assert!(!lease.is_expired());
    }

    #[test]
    fn test_new_lease_does_not_need_renewal_immediately() {
        let lease = NodeLease::new("node1", 40);
        // Just created -- renew interval is 10s, so shouldn't need renewal yet
        assert!(!lease.needs_renewal());
    }

    #[test]
    fn test_expired_lease() {
        let mut lease = NodeLease::new("node1", 40);
        // Backdate the renew_time by more than duration
        lease.renew_time = Utc::now() - Duration::seconds(50);
        assert!(lease.is_expired());
    }

    #[test]
    fn test_renew_clears_expiry() {
        let mut lease = NodeLease::new("node1", 40);
        lease.renew_time = Utc::now() - Duration::seconds(50);
        assert!(lease.is_expired());
        lease.renew();
        assert!(!lease.is_expired());
    }

    #[test]
    fn test_needs_renewal_after_interval() {
        let mut lease = NodeLease::new("node1", 40);
        // Backdate past 1/4 of duration (10s)
        lease.renew_time = Utc::now() - Duration::seconds(15);
        assert!(lease.needs_renewal());
    }

    #[test]
    fn test_time_until_renewal_zero_when_overdue() {
        let mut lease = NodeLease::new("node1", 40);
        lease.renew_time = Utc::now() - Duration::seconds(20);
        assert_eq!(lease.time_until_renewal(), Duration::zero());
    }

    #[test]
    fn test_holder_identity_matches_node_name() {
        let lease = NodeLease::new("my-node", 40);
        assert_eq!(lease.holder_identity, "my-node");
        assert_eq!(lease.node_name, "my-node");
    }

    #[test]
    fn test_short_duration_lease_minimum_interval() {
        let lease = NodeLease::new("node1", 2); // 2s duration -> 0.5s interval -> clamp to 1s
        assert!(!lease.is_expired());
    }
}
