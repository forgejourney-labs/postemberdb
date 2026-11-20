# PostemberDB tools

This directory contains two independent operator applications.

| Application | Purpose | UI |
|---|---|---|
| [Browser](browser/README.md) | Native desktop database browser | Compact classic desktop UI inspired by 2014–2015 tools |
| [Admin](admin/README.md) | Local web administration panel | Browser-based, phpMyAdmin-style layout |

Both applications use the Rust core catalog and bounded row-page APIs. Remote database connections use the TLS-enabled Binary/1 transport with certificate-chain and server-name validation. Neither application uses the unauthenticated development JSONL/1 protocol for remote access.

## Safety boundaries

- The plain server command is unauthenticated and loopback-only.
- Remote connections require the server-tls listener, a trusted CA PEM bundle, a matching TLS server name, and a provisioned user account.
- The web Admin panel itself binds to loopback only. Its Rust backend opens the remote TLS connection; do not publish the web panel through a reverse proxy or public interface.
- Remote user, role, and audit operations are enforced by the database server's Admin permission and actor-attributed audit layer.
- No application silently migrates a database or runs destructive maintenance operations.

## Implemented

- Open local .pbd and legacy .db files in Browser.
- Connect Browser to a remote TLS server with mandatory CA and hostname validation.
- Inspect tables, columns, types, nullability, indexes, uniqueness, and row counts.
- Browse bounded pages of rows, insert, edit non-key fields transactionally, and delete after confirmation in Browser.
- Export desktop Browser results as CSV or JSON.
- Connect Admin to a remote TLS server from its loopback-only web interface.
- Use live catalog browsing, paged rows, CRUD with a UNIQUE key, and current-page CSV/JSON export in Admin.
- View remote server status, list/create/reset/manage users, manage roles and memberships, and inspect audit events in Admin.
- Run SQL and render result grids in both applications.

## Still pending

- Mutual TLS, authentication rate limiting, query deadlines/cancellation, per-user quotas, and finer-grained schema/table grants.
- Full-table streaming exports, sorting/filtering, query history, and a native desktop file picker.
- Hosting the web Admin on a remote machine is intentionally unsupported; it remains a loopback operator console.
