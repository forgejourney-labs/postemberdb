use postemberdb::{Database, Value};
use std::time::Instant;

fn main() {
    const ROWS: usize = 100_000;
    const BATCH: usize = 1_000;

    let mut db = Database::new();
    db.execute("CREATE TABLE player_stats (uuid TEXT NOT NULL, coins INTEGER, playtime INTEGER)")
        .expect("create player_stats");

    let insert_start = Instant::now();
    for start in (0..ROWS).step_by(BATCH) {
        let end = (start + BATCH).min(ROWS);
        let rows = (start..end)
            .map(|id| vec![
                Value::Text(format!("player-{id:08}")),
                Value::Integer((id % 50_000) as i64),
                Value::Integer((id * 17) as i64),
            ])
            .collect::<Vec<_>>();
        db.insert_many("player_stats", rows).expect("bulk insert");
    }
    let insert_elapsed = insert_start.elapsed();

    let index_start = Instant::now();
    db.execute("CREATE INDEX idx_player_stats_uuid ON player_stats (uuid)")
        .expect("create UUID index");
    let index_elapsed = index_start.elapsed();

    let lookup_start = Instant::now();
    for id in (0..ROWS).step_by(10) {
        let sql = format!("SELECT * FROM player_stats WHERE uuid = 'player-{id:08}'");
        let result = db.execute(&sql).expect("indexed lookup");
        assert_eq!(result.rows.len(), 1);
    }
    let lookup_elapsed = lookup_start.elapsed();

    println!("dataset_rows={ROWS}");
    println!("insert_batch_size={BATCH}");
    println!("bulk_insert_elapsed_ms={}", insert_elapsed.as_millis());
    println!("index_build_elapsed_ms={}", index_elapsed.as_millis());
    println!("indexed_lookups={}", ROWS / 10);
    println!("indexed_lookup_elapsed_ms={}", lookup_elapsed.as_millis());
}
