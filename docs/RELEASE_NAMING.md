# PostemberDB release naming

PostemberDB uses two separate version identifiers:

1. **SemVer package/engine version** (the value in Cargo.toml) is authoritative for
   dependency resolution, compatibility checks, and build metadata.
2. **Release series and codename** are human-facing product identity, inspired by
   the release naming traditions used by Debian and Android.

## Current identity

- Product label: **PostemberDB 1 — Ember**
- Release series: `1`
- Codename: `Ember`
- Current SemVer package version: `0.1.0`

`PostemberDB 1 — Ember` names the first product series. It does **not** mean that
the current package is SemVer `1.0.0`, stable, or production-ready. Those claims
must be made only when the corresponding compatibility and release gates pass.

## Rules

- A codename is assigned to a release series and is immutable after that series
  has shipped.
- SemVer continues to change independently according to API and storage-format
  compatibility.
- Development builds may display the current series codename, but must not be
  described as stable solely because they have a series number.
- Never reuse a codename for a later series.
- The public Rust version module is the single source for the human-facing
  series and codename; Cargo.toml remains the source for SemVer.
