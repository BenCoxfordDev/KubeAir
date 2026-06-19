## Description

<!-- What does this PR do and why? Link the related issue if one exists. -->

Fixes #

## Type of change

<!-- Check all that apply. -->

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that changes existing behaviour)
- [ ] Performance improvement
- [ ] Refactor (no behaviour change)
- [ ] Documentation / comments only
- [ ] Test only

## Changes

<!-- Bullet-point summary of the key changes. Helps reviewers and feeds release notes. -->

- 

## Testing

<!-- Describe how you tested this change. -->

- [ ] Unit tests added / updated
- [ ] Integration tests added / updated (`cargo test --test integration`)
- [ ] Conformance tests pass (`just conformance-smoke`)
- [ ] Smoke tests pass (`cargo test --test smoke`)
- [ ] Manually tested against a running cluster

<!-- For bug fixes: describe the regression test added. -->

## Checklist

- [ ] `just lint` passes (clippy + fmt)
- [ ] `just test` passes
- [ ] No new `unwrap`/`expect` in production paths without a justifying comment
- [ ] No new `unsafe` blocks without a safety comment
- [ ] CLAUDE.md / docs updated if behaviour visible to operators changes
