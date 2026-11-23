# Branch policy

PostemberDB development is restricted to these four existing branches:

| Branch | Purpose | Allowed state |
|---|---|---|
| `dev` | Active development and fixes | New work, refactors, tests, documentation, and integration changes |
| `canary` | Early validation | Implemented features under wider or less controlled testing |
| `unstable` | Release-candidate line | Integrated and polished work that is not yet declared stable |
| `stable` | Supported release line | Only changes that passed the complete release gate |

Do not create another development branch for ordinary project work. Do not use or recreate `main`.

## Promotion path

The intended direction is:

`dev` → `canary` → `unstable` → `stable`

A promotion is a deliberate review and validation step, not an automatic branch sync.

- **dev → canary:** promote a coherent feature/fix after targeted tests and the full CI workflow pass.
- **canary → unstable:** promote only after the behavior has been exercised beyond the initial implementation, compatibility and migration paths have been checked, and CI is green.
- **unstable → stable:** promote only after release acceptance, recovery/fault-injection coverage, security review, backup/restore validation, and repeatable performance results are complete.

Never promote a failing or partially tested commit. A green unit-test run alone does not satisfy the stable release gate.

## Diverged histories

Before any promotion, compare the source and target histories. If they have diverged, do not force-push and do not blindly merge one branch over the other. Review the unique commits on both sides, reconcile changes intentionally, and preserve work that exists only on the target branch.

A branch is not made stable merely by being named `stable`. Its contents must satisfy the release gate in [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) and the security requirements in [SECURITY_MODEL.md](SECURITY_MODEL.md).

## Safety rules for repository changes

1. Work only in `dev`, `canary`, `unstable`, and `stable`.
2. Never rewrite branch history as a shortcut for promotion.
3. Use expected-head checks when updating a branch ref so concurrent work is not overwritten.
4. Keep `stable` unchanged when required evidence is missing.
5. Record what was changed, which branch received it, and which checks actually passed. Do not claim a check passed until its result is visible.
