# PostemberDB

**PostemberDB** is an independent SQL-like database engine written in Rust. It is designed around two ways to use the same engine:

- **Embedded** — link the Rust library directly into an application.
- **Database Server** — run the engine as a separate process and connect over a network protocol.

PostemberDB is not a wrapper around SQLite or PostgreSQL. Its storage, execution, and protocol layers are intended to be implemented in this project.

## KSL integration

KSL is an optional integration target, not a requirement for using PostemberDB. The engine will expose a stable core API for ordinary Rust applications, with a dedicated integration layer planned to reduce conversion and execution overhead for KSL workloads.

## Current status

The project is at an early development stage. The first prototype provides:

- an in-memory database shared by the Embedded API and server process;
- basic `CREATE TABLE`, `INSERT INTO ... VALUES`, and `SELECT * FROM` statements;
- integer, text, boolean, and nullable values;
- basic column type and `NOT NULL` validation;
- a minimal line-oriented TCP server for development and experimentation.

**Current limitations:** data is memory-only and is lost when the process exits. The SQL parser is deliberately small; this is not yet a production database, and the server protocol is not yet suitable for untrusted networks.

## Build and test

Requires a recent stable Rust toolchain.

```bash
cargo test
cargo build --release
```

## Run the server

```bash
cargo run -- server 127.0.0.1:7432
```

The development protocol accepts one SQL statement per line. Send `QUIT` to close a client session.

## Use as an embedded library

```rust
use postemberdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::new();
    db.execute("CREATE TABLE users (id INTEGER NOT NULL, name TEXT)")?;
    db.execute("INSERT INTO users VALUES (1, 'Ada')")?;

    let result = db.execute("SELECT * FROM users")?;
    println!("{}", result.message);
    Ok(())
}
```

## Roadmap

1. Replace the prototype parser with a tokenizer and structured SQL AST.
2. Introduce a page-oriented storage layer and durable on-disk format.
3. Add write-ahead logging, crash recovery, and transactions.
4. Add indexes and a cost-aware query planner.
5. Harden the server protocol with authentication, framing, and resource limits.
6. Add KSL-specific integration and benchmark it against the general API.

## Development branches

- `dev` — active development.
- `canary` — early integration builds.
- `unstable` — experimental work.
- `stable` — reviewed releases.
