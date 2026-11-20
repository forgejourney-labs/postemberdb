# PostemberDB tools

This directory contains two independent operator applications.

| Application | Purpose | UI |
|---|---|---|
| [Browser](browser/README.md) | Native desktop database browser | Compact classic desktop UI inspired by 2014–2015 tools |
| [Admin](admin/README.md) | Local web administration panel | Browser-based, phpMyAdmin-style layout |

Both applications use the same read-only SQL catalog API in the Rust core. The API exposes table metadata without copying table row payloads. Remote connections are a separate milestone and must use a stable authenticated protocol with TLS, not the current development JSONL/1 transport.

## Safety boundary

- Admin binds to loopback only.
- Do not expose development protocols to untrusted networks.
- Remote support requires TLS, authentication, authorization, bounded requests, and explicit permission checks.
- Neither application silently migrates a database or runs destructive maintenance operations.

## Implementation status

Implemented locally:
- Open .pbd and legacy .db files in the desktop Browser.
- Inspect tables, columns, types, nullability, indexes, uniqueness, and row counts.
- Browse the first 100 rows from a selected table.
- Run SQL and render result grids in both applications.
- Browse the live catalog in Admin through /api/catalog.

Still pending:
- Remote connections and remote administration.
- Admin authentication, CSRF protection, RBAC-backed user/role management, and server metrics.
- Native desktop file picker, data editing forms, import/export, and query history.
