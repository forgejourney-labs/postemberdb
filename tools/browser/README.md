# PostemberDB Browser

A separate native desktop application inspired by SQLite Browser and classic database tools from around 2014–2015.

## First milestone

- Open a local `.pbd` or legacy `.db` file through the PostemberDB library.
- Execute SQL against the opened database.
- Display result columns, rows, affected-row count, and errors.
- Keep the interface compact: menu/toolbar, connection pane, SQL editor, result grid, and status bar.

## Build and run

From the repository root:

```bash
cargo run --manifest-path tools/browser/Cargo.toml
```

The first milestone is local/embedded only. Remote connections are deliberately not wired to JSONL/1 or Binary/1 development transports. They will be added after a stable authenticated TLS client protocol is available.

## Current limitations

- Table/catalog tree and detailed schema/index inspectors need public catalog introspection APIs in the core.
- No remote connections yet.
- No automatic migration, vacuum, or destructive actions.
