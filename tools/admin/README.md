# PostemberDB Admin

A separate Rust web application with a phpMyAdmin-inspired administration layout.

## Implemented

- Starts a local HTTP interface and opens it in a browser.
- Opens one local PostemberDB file in embedded mode.
- Provides a live catalog endpoint with table names, row counts, columns, data types, nullability, indexes, and uniqueness.
- Shows a left-hand table navigator, database overview, table structure inspector, SQL console, and result grids.
- Uses a compact classic layout inspired by database administration tools from 2014–2015.

## Run

From the repository root:

    cargo run --manifest-path tools/admin/Cargo.toml -- ./data/postemberdb.pbd

Then open http://127.0.0.1:7480.

An optional bind address may be supplied, but it must be loopback-only:

    cargo run --manifest-path tools/admin/Cargo.toml -- ./data/postemberdb.pbd 127.0.0.1:7480

## Security

This initial panel is deliberately local-only. It has no remote login flow and must not be exposed through a reverse proxy or public interface. Remote administration requires a stable authenticated protocol, TLS certificate validation, RBAC enforcement for every operation, CSRF protection, request limits, and audit integration.

## Current limitations

- One database file per process.
- User/role management and server metrics are not implemented.
- Remote server mode and authentication are not implemented.
- No automatic migration or destructive maintenance actions.
