# PostemberDB Admin

A separate Rust web application with a phpMyAdmin-inspired administration layout. The web listener is deliberately loopback-only; it is an operator console for the local machine.

## Implemented

- Starts a local HTTP interface and opens it in a browser.
- Opens one local PostemberDB file in embedded mode by default.
- Connects to a remote PostemberDB server from the Rust backend using TLS-enabled Binary/1, a trusted CA PEM bundle, and server-name validation.
- Uses remote catalog, table-page, SQL, and row CRUD operations after a remote connection is established.
- Provides bounded row paging (50 rows per page by default, up to 200 per request).
- Allows inserting rows and editing/deleting individual rows when the table has a UNIQUE index or PRIMARY KEY.
- Exports the current visible page as CSV or JSON.
- Provides remote server status, user list/create/enable/disable/delete/password reset, role create/permission updates/assignment/revocation/deletion, and audit-event browsing.
- Provides a live table-name filter in the database navigator, a current-page row filter, and displays the active local/remote connection mode in the header.
- User password reset is available in the user action list and validates the minimum password length before sending the request.
- All remote administrative actions are checked by the database server for the Admin permission; mutations are attributed to the authenticated principal in the durable audit log.
- Server-side RBAC changes are rejected if they would leave no enabled administrator.
- Shows a classic left-hand table navigator, database overview, structure inspector, SQL console, results, and administration workspace.

## Run

From the repository root:

    cargo run --manifest-path tools/admin/Cargo.toml -- ./data/postemberdb.pbd

Then open http://127.0.0.1:7480.

An optional bind address may be supplied, but it must be loopback-only:

    cargo run --manifest-path tools/admin/Cargo.toml -- ./data/postemberdb.pbd 127.0.0.1:7480

## Remote connection setup

Start the PostemberDB server with TLS on the database host:

    cargo run -- server-tls 0.0.0.0:7432 ./data/postemberdb.pbd ./cert.pem ./key.pem

Provision an account with the CLI user command. In Admin, enter the server address, the DNS name covered by the certificate, the PEM CA bundle trusted by the client, the username, and password, then select Connect securely.

The password is submitted to the Admin backend over its loopback-only HTTP listener and is then used for authentication inside the verified TLS connection. It is not stored in browser local storage or returned by the API. Do not expose Admin through a reverse proxy or public interface.

## Editing safety

- Insert validates the value count and every column type.
- Update/delete require a UNIQUE index or PRIMARY KEY and verify exactly one row is affected.
- Tables without a unique index remain browseable and read-only through row actions.
- The remote server independently applies SQL permissions and Admin authorization.

## Current limitations

- One local database file per process, plus one optional remote session.
- Admin's web listener is not designed to be hosted on a remote machine.
- Mutual TLS, query cancellation/deadlines, per-user quotas, schema/table grants, and full-table streaming exports are not implemented.
- No automatic migration or destructive maintenance actions.
