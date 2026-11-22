# PostemberDB SQL dialect

PostemberDB implements its own SQL dialect. It is not yet a complete SQLite or PostgreSQL replacement. This page describes supported behavior and intentionally unfinished areas.

## Implemented

| Area | Supported syntax / behavior |
|---|---|
| Schema | \`CREATE TABLE\`, \`CREATE TABLE IF NOT EXISTS\`, column definitions, \`PRIMARY KEY\`, composite primary keys |
| Types | \`INTEGER\` / \`INT\` / \`BIGINT\` / \`SMALLINT\`, \`TEXT\` / \`VARCHAR\` / \`CHAR\` / \`STRING\`, \`BOOLEAN\` / \`BOOL\` |
| Indexes | \`CREATE INDEX\`, unique indexes created from primary-key constraints, equality-index lookup |
| Inserts | \`INSERT INTO ... VALUES\`, explicit insert column lists, bounded Rust bulk-insert API |
| Upsert | \`ON CONFLICT (...) DO UPDATE SET\`, \`ON DUPLICATE KEY UPDATE\` |
| Updates | Single-target \`UPDATE SET\` with constants or integer scalar expressions evaluated against each matched row |
| Deletes | \`DELETE FROM\` with optional nested predicates |
| Queries | \`SELECT *\`, selected columns, \`COUNT(*)\`, \`WHERE\` comparisons, \`IS NULL\`, \`IS NOT NULL\`, \`IS [NOT] DISTINCT FROM\`, \`LIKE\` / \`NOT LIKE\`, \`IN\` / \`NOT IN\`, \`BETWEEN\` / \`NOT BETWEEN\`, nested \`AND\` / \`OR\` / \`NOT\`, \`LIMIT\` |
| Scalar expressions | Integer literals, quoted identifiers, escaped text literals, unary signs, arithmetic `+` / `-` / `*` / `/` / `%`, parentheses, column-to-column comparisons, and `COALESCE`, `NULLIF`, `ABS`, `LENGTH`, `LOWER`, `UPPER` in `WHERE` and single-target `UPDATE SET` |
| Sorting | \`ORDER BY column [ASC|DESC]\`, multiple sort keys |
| Parameters | Typed positional \`?\` values through Rust Database / session APIs and the KSL JSONL/1 \`SQL_PARAMS\` command |
| Transactions | Rust transaction callback and sessions, per-connection \`BEGIN\` / \`COMMIT\` / \`ROLLBACK\`, and atomic JSONL/1 \`SQL_BATCH\` requests from KSL |

SQL comparisons follow three-valued logic. Comparisons involving NULL yield UNKNOWN; WHERE keeps only TRUE rows. Division by zero yields NULL. Integer arithmetic is checked and reports overflow instead of wrapping.

The KSL parameter wire format supports NULL, signed 64-bit integer, text, and boolean values. \`SQL_PARAMS\` supports up to 1,024 values for one statement. \`SQL_BATCH\` executes 1..=500 parameterized statements in one transaction and returns one result per statement. Both are bounded by the JSONL/1 request-line limit of 64 KiB.

Example:

\`\`\`sql
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
WHERE (level >= 10 AND coins + 50 > 200)
   OR uuid = 'player-a'
ORDER BY coins DESC
LIMIT 50;

UPDATE player_stats
SET coins = coins + 25
WHERE uuid = 'player-a';
\`\`\`

KSL should bind dynamic values rather than concatenate them into SQL:

\`\`\`kotlin
postemberDb().queryParamsAsync(
    "SELECT coins FROM player_stats WHERE uuid = ?",
    player.uniqueId.toString()
)
\`\`\`

PDB6 queries read row IDs and row payloads in bounded batches. An unordered scan can stop early when \`LIMIT\` is reached. \`ORDER BY\` currently collects matching rows before sorting, so large ordered result sets still consume memory proportional to the matched result set.

## Not implemented yet

- \`JOIN\`, \`GROUP BY\`, \`HAVING\`, \`DISTINCT\`, subqueries, \`EXISTS\`, CTEs, and set operators.
- Aggregates beyond \`COUNT(*)\`, including \`SUM\`, \`AVG\`, \`MIN\`, and \`MAX\`.
- \`OFFSET\`, window functions, and recursive queries.
- Multiple \`UPDATE SET\` assignments and expression projections.
- Additional scalar functions, `CASE`, casts, REAL values, collations, and date/time types.
- \`DROP TABLE\`, \`DROP INDEX\`, \`ALTER TABLE\`, foreign keys, check constraints, and default expressions.
- Savepoints, configurable isolation levels, and cross-domain transactions spanning SQL, KV, Registry, and EF7.
- Cost-based optimization, index range scans, \`EXPLAIN\`, server-side prepared-statement handles, and parsed-plan caching.

The KSL JSONL/1 parameter API binds typed values for each execution; it does not yet retain prepared-statement handles or parsed execution plans between requests.

Unsupported SQL must return a clear error. The engine must not silently accept syntax it cannot execute.
