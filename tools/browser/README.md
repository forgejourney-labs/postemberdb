# PostemberDB Browser

A separate native desktop application inspired by SQLite Browser and classic database tools from around 2014–2015.

## Implemented

- Open a local .pbd or legacy .db file through the PostemberDB library.
- Read the SQL catalog without copying row payloads: tables, columns, data types, nullability, indexes, uniqueness, and row counts.
- Browse a selected table by loading its first 100 rows into the result grid.
- Execute SQL and refresh the catalog after statements.
- Keep the interface compact: toolbar, connection pane, collapsible table structure, SQL editor, result grid, and status bar.

## Build and run

From the repository root:

    cargo run --manifest-path tools/browser/Cargo.toml

Enter the database path and select Open. Click a table in Explorer to browse its rows.

## Current limitations

- Remote connections are not available yet. They will use a stable authenticated TLS client protocol, not the current development JSONL/1 transport.
- No automatic migration, vacuum, or destructive maintenance actions.
- The file path is entered manually; a native file picker is a later UI improvement.
