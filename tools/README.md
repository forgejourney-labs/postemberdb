# PostemberDB tools

This directory contains two independent operator applications.

| Application | Purpose | UI |
|---|---|---|
| [Browser](browser/README.md) | Native desktop database browser | Compact classic desktop UI inspired by 2014–2015 tools |
| [Admin](admin/README.md) | Local web administration panel | Browser-based, phpMyAdmin-style layout |

Both applications use the SQL catalog and bounded row-page APIs in the Rust core. Remote connections require a stable authenticated protocol with TLS, not the development JSONL/1 transport.

## Safety boundary

- Admin binds to loopback only.
- Do not expose development protocols to untrusted networks.
- Remote support requires TLS, authentication, authorization, bounded requests, and explicit permission checks.
- Neither application silently migrates a database or runs destructive maintenance operations.

## Implemented

- Open .pbd and legacy .db files in desktop Browser.
- Inspect tables, columns, types, nullability, indexes, uniqueness, and row counts.
- Read bounded pages of rows from the Rust core.
- Insert rows, edit non-key fields transactionally, and delete rows after confirmation in desktop Browser.
- Export desktop Browser results as CSV or JSON.
- Use the live catalog, browse pages, insert rows, and edit/delete rows with a UNIQUE key in Admin.
- Export the currently visible Admin page to CSV or JSON.
- Run SQL and render result grids in both applications.

## Still pending

- Remote connections and remote administration.
- Admin authentication, CSRF protection, RBAC-backed user/role management, and server metrics.
- Native desktop file picker, query history, sorting/filtering, and full-table streaming exports.
