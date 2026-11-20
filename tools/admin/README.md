# PostemberDB Admin

A separate Rust web application with a phpMyAdmin-inspired administration layout.

## Implemented

- Starts a local HTTP interface and opens it in a browser.
- Opens one local PostemberDB file in embedded mode.
- Provides a live catalog with tables, row counts, columns, data types, nullability, indexes, and uniqueness.
- Provides bounded row paging (50 rows per page by default, up to 200 per request).
- Allows inserting rows and editing/deleting individual rows when the table has a UNIQUE index or PRIMARY KEY.
- Edits one cell per request, with type/null validation performed by the server and core engine.
- Exports the current visible page as CSV or JSON.
- Shows a classic left-hand table navigator, database overview, structure inspector, SQL console, and result grids.

## Run

From the repository root:

    cargo run --manifest-path tools/admin/Cargo.toml -- ./data/postemberdb.pbd

Then open http://127.0.0.1:7480.

An optional bind address may be supplied, but it must be loopback-only:

    cargo run --manifest-path tools/admin/Cargo.toml -- ./data/postemberdb.pbd 127.0.0.1:7480

## Editing safety

- Insert validates the value count and every column type.
- Update/delete require a UNIQUE index or PRIMARY KEY and verify exactly one row is affected.
- Tables without a unique index remain browseable and read-only through row actions.
- The Admin service remains loopback-only and limits JSON request bodies.

## Security

This initial panel is deliberately local-only. It has no remote login flow and must not be exposed through a reverse proxy or public interface. Remote administration requires a stable authenticated protocol, TLS certificate validation, RBAC enforcement for every operation, CSRF protection, request limits, and audit integration.

## Current limitations

- One database file per process.
- User/role management and server metrics are not implemented.
- Remote server mode and authentication are not implemented.
- CSV/JSON exports apply to the currently visible page, not a full-table streamed dump.
- No automatic migration or destructive maintenance actions.
