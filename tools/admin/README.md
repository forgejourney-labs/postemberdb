# PostemberDB Admin

A separate web application with a phpMyAdmin-inspired administration layout.

## First milestone

- Starts a local HTTP interface and opens it in a browser.
- Opens one local PostemberDB file in embedded mode.
- Provides a SQL console and renders result grids.
- Uses a classic layout: left navigation, top bar, content workspace, query editor, and status area.

## Run

From the repository root:

```bash
cargo run --manifest-path tools/admin/Cargo.toml -- ./data/postemberdb.pbd
```

Then open `http://127.0.0.1:7480`.

An optional bind address may be supplied, but it must be loopback-only:

```bash
cargo run --manifest-path tools/admin/Cargo.toml -- ./data/postemberdb.pbd 127.0.0.1:7480
```

## Security

This initial panel is deliberately local-only. It has no remote login flow and must not be exposed through a reverse proxy or public interface. Remote administration requires a stable authenticated protocol, TLS certificate validation, RBAC enforcement for every operation, CSRF protection, request limits, and audit integration.

## Current limitations

- One database file per process.
- Catalog navigation, table structure, indexes, user/role management, and server metrics require additional read-only/admin APIs.
- No remote server mode yet.
