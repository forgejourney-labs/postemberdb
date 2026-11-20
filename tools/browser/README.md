# PostemberDB Browser

A separate native desktop application inspired by SQLite Browser and classic database tools from around 2014–2015.

## Implemented

- Open local .pbd or legacy .db files.
- Inspect tables, columns, types, nullability, indexes, uniqueness, and row counts.
- Browse table rows in bounded pages of 100 records.
- Navigate previous/next pages without materializing a full table.
- Insert rows through schema-aware forms.
- Edit non-key fields in one database transaction; primary/unique key fields stay locked in the form.
- Delete a row only after a separate confirmation and only when a UNIQUE index or PRIMARY KEY can identify exactly one row.
- Execute SQL and refresh catalog metadata.
- Export current table pages or SQL results as CSV or JSON, preserving JSON scalar types.
- Keep the interface compact: toolbar, connection pane, collapsible table structure, SQL editor, result grid, and status bar.

## Build and run

From the repository root:

    cargo run --manifest-path tools/browser/Cargo.toml

Enter a database path and select Open. Click a table in Explorer to browse its rows.

CSV and JSON exports are saved in the current working directory.

## Editing safety

- The form validates integer, text, boolean, and NULL values against the catalog.
- Editing/deleting requires a unique key; key columns are locked in the form.
- Multiple changed non-key fields are committed in one database transaction.
- Tables without a unique key can still be browsed and inserted into, but row edits/deletes remain disabled.

## Current limitations

- Remote connections are not available. They will use a stable authenticated TLS client protocol, not the development JSONL/1 transport.
- No automatic migration, vacuum, or destructive maintenance actions.
- The file path is entered manually; a native file picker is a later UI improvement.
- Sorting/filtering and full-table streaming exports are later improvements.
