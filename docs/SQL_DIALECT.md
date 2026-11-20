# PostemberDB SQL dialect

PostemberDB is a Rust SQL database engine with a deliberately documented dialect. It is not yet a complete PostgreSQL or SQLite replacement. This page describes implemented syntax and remaining compatibility work.

## Implemented

| Area | Supported syntax / behavior |
|---|---|
| Schema | `CREATE TABLE`, `CREATE TABLE IF NOT EXISTS`, column definitions, `PRIMARY KEY`, composite primary keys |
| Types | `INTEGER` / `INT` / `BIGINT` / `SMALLINT`, `TEXT` / `VARCHAR` / `CHAR` / `STRING`, `BOOLEAN` / `BOOL` |
| Indexes | `CREATE INDEX`, unique indexes created from primary-key constraints |
| Inserts | `INSERT INTO ... VALUES`, explicit insert column lists, bounded bulk insert API |
| Upsert | `ON CONFLICT (...) DO UPDATE SET`, KSL-compatible `ON DUPLICATE KEY UPDATE` |
| Updates | Single-column assignment with optional equality predicates |
| Deletes | `DELETE FROM` with optional equality predicates |
| Queries | `SELECT *`, selected columns, equality predicates joined by `AND`, `COUNT(*)`, `LIMIT` |
| Sorting | `ORDER BY column [ASC|DESC]`, multiple sort keys |
| Parameters | Typed positional `?` parameters through the Rust API and client sessions |
| Transactions | Rust transaction callback and owned client sessions; server transaction-control commands |

Example:

```sql
CREATE TABLE player_stats (
    uuid TEXT NOT NULL,
    coins BIGINT,
    level INTEGER,
    PRIMARY KEY (uuid)
);

CREATE INDEX player_stats_level ON player_stats (level);

INSERT INTO player_stats (uuid, coins, level)
VALUES ('player-a', 250, 12);

SELECT uuid, coins, level
FROM player_stats
WHERE uuid = 'player-a'
ORDER BY coins DESC
LIMIT 50;
```

PDB6 queries scan row IDs and decode only required row payloads in bounded batches. `LIMIT` can stop an unordered scan early. `ORDER BY` currently requires collecting matching rows before sorting, so large ordered result sets still consume memory proportional to the result size.

## Not implemented yet

- General comparison predicates: `<`, `<=`, `>`, `>=`, `!=`, `<>`.
- `IS NULL`, `IS NOT NULL`, `LIKE`, `IN`, `BETWEEN`, `OR`, nested boolean expressions.
- `JOIN`, `GROUP BY`, `HAVING`, `DISTINCT`, subqueries and CTEs.
- Aggregates beyond `COUNT(*)`, including `SUM`, `AVG`, `MIN`, `MAX`.
- `OFFSET`, window functions, unions and set operators.
- `DROP TABLE`, `DROP INDEX`, `ALTER TABLE`, foreign keys, check constraints and defaults.
- Explicit SQL `BEGIN`, `COMMIT`, `ROLLBACK`, savepoints and isolation levels.
- Cost-based query planning, `EXPLAIN`, prepared execution plans and index range scans.

Unsupported SQL must return a clear error; the engine must not silently accept syntax it cannot execute.