# PostemberDB CLI

The CLI is being built in Rust alongside the database engine. Current commands are intentionally small and safe; commands that could destroy or rewrite data are not added until their recovery semantics are tested.

## Available commands

- postemberdb help — show command usage.
- postemberdb version — show the SemVer build version and PostemberDB release series/codename.
- postemberdb create-pdb6 [data-file] — create a new empty PDB6 database. Refuses to overwrite an existing file or PDB6 page store.
- postemberdb migrate-pdb6 [data-file] — explicitly migrate an existing PDB5 database to PDB6 while retaining the PDB5 snapshot.
- postemberdb server [127.0.0.1:7432] [data-file] — start the development server. The plain-text server remains loopback-only; use server-tls for authenticated remote access. A missing data file is initialized as PDB6; an existing PDB5 file remains readable and is not silently migrated.
- postemberdb check [data-file] — open the database, decode the snapshot, and replay/validate the WAL. Opening may truncate an incomplete final WAL frame, so this is a recovery check rather than a read-only forensic check.
- postemberdb checkpoint [data-file] — recover the database, write a snapshot, sync it, and safely truncate the WAL.

The data-file argument defaults to postemberdb.pbd for the commands that accept it. Check and checkpoint require an existing database file; they do not silently create one.

## Planned command groups

- db: info, create, open, close, list.
- schema: tables, describe, indexes, constraints, migrations.
- query: execute SQL, explain, plan, export with bounded output.
- wal: status, replay-check, checkpoint, inspect.
- storage: page stats, allocator stats, cache stats, fragmentation stats.
- defrag: key, range, step with a strict byte budget, pause/resume.
- snapshot: create, list, verify, restore.
- backup: full, incremental, verify, restore, prune.
- security: users, roles, grants, revoke, rotate-key, audit.
- config: validate, show effective budgets, print defaults.
- bench: run named KSL/Minecraft workload profiles.

## CLI safety principles

1. Destructive commands require explicit confirmation or a machine-readable --yes flag.
2. Restore and migration commands require a verified backup or an explicit override.
3. Commands print whether they are read-only, recovery-capable, or destructive.
4. Output is bounded and can be emitted as human-readable text or JSON.
5. Secrets are never echoed. Key material is accepted only through a secure OS/key-provider path, not command-line arguments.
6. Long-running defragmentation and backup operations expose progress and accept byte/time budgets.
7. No command starts hidden background garbage collection or compaction.
