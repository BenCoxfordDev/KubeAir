# Security Policy

## Supported Versions

KubeAir supports the two most recent release lines. Security fixes are backported to the current and previous minor release.

| Version    | Supported |
| ---------- | --------- |
| `1.33.x` | ✅ Yes    |
| `< 1.33` | ❌ No     |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Report vulnerabilities privately via [GitHub Security Advisories](https://github.com/bencoxford/kubeair/security/advisories/new). This keeps the details confidential until a fix is ready.

When reporting, please include:

- A description of the vulnerability and its impact
- Steps to reproduce or a proof-of-concept (if safe to share)
- The affected version(s)
- Any suggested mitigations you are aware of

You will receive an acknowledgement within **48 hours**. We aim to release a fix within **7 days** for critical issues and **30 days** for lower severity issues.

## Disclosure Policy

We follow [coordinated vulnerability disclosure](https://en.wikipedia.org/wiki/Coordinated_vulnerability_disclosure). Once a fix is released, we will publish a security advisory with full details.

## Scope

KubeAir is a node agent (kubelet replacement). Security issues in scope include:

- Privilege escalation via the kubelet API (`/exec`, `/attach`, `/portForward`, `/logs`)
- Authentication or authorisation bypass in the kubelet HTTPS server
- Container escape through incorrect security context handling
- Credential leakage (kubeconfig, image pull secrets, service account tokens)
- Memory safety issues in unsafe code blocks

Out of scope: vulnerabilities in the underlying container runtime (containerd), the Kubernetes API server, or the host OS kernel.

## Dependency Vulnerabilities

We run `cargo audit` before every release. If you discover a vulnerability in a dependency used by KubeAir, please report it to the upstream crate maintainer first, then notify us so we can update.
