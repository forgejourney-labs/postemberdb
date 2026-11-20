# PostemberDB Browser

A separate native desktop application inspired by SQLite Browser and classic database tools from around 2014–2015.

## Implemented

- Open a local .pbd or legacy .db file.
- Inspect tables, columns, types, nullability, indexes, uniqueness, and row counts.
- Browse table rows in bounded pages of 100 records.
- Navigate previous/next pages without building a full-table result in memory.
- Execute SQL and refresh catalog metadata.
- Export the current result page or SQL result as CSV or JSON, preserving JSON scalar types.
- Keep the interface compact: toolbar, connection pane, collapsible table structure, SQL editor, result grid, and status bar.

## Build and run

From the repository root:

    cargo run --manifest-path tools/browser/Cargo.toml

Enter a database path and select Open. Click a table in Explorer to browse its rows.

CSV and JSON exports are saved in the current working directory.

## Current limitations

- Remote connections are not available. They will use a stable authenticated TLS client protocol, not the development JSONL/1 transport.
- No automatic migration, vacuum, or destructive maintenance actions.
- The file path is entered manually; a native file picker is a later UI improvement.
- Data editing forms are not yet available in the desktop application; use its SQL console for changes.
