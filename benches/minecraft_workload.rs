use postemberdb::{Database, SqlRowStore, Value};
use std::fs;
use std::path::PathBuf;
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

    // Exercise the real page-backed record path separately from the in-memory
    // SQL executor so the benchmark does not imply SQL is already disk-backed.
    const DISK_ROWS: usize = 10_000;
    const DISK_BATCH: usize = 500;
    let disk_path = std::env::temp_dir().join(format!(
        "postemberdb-minecraft-row-bench-{}.pbd",
        std::process::id()
    ));
    let disk_insert_start = Instant::now();
    {
        let mut rows = SqlRowStore::open(&disk_path, 3).expect("open page-backed SQL row store");
        for start in (0..DISK_ROWS).step_by(DISK_BATCH) {
            let end = (start + DISK_BATCH).min(DISK_ROWS);
            let batch = (start..end).map(|id| (
                id as u64 + 1,
                vec![
                    Value::Text(format!("player-{id:08}")),
                    Value::Integer((id % 50_000) as i64),
                    Value::Integer((id * 17) as i64),
                ],
            ));
            rows.put_many(batch).expect("write page-backed SQL rows");
        }
        let disk_insert_elapsed = disk_insert_start.elapsed();

        let disk_lookup_start = Instant::now();
        for id in (0..DISK_ROWS).step_by(10) {
            let row = rows.get(id as u64 + 1).expect("read page-backed SQL row");
            assert!(row.is_some());
        }
        let disk_lookup_elapsed = disk_lookup_start.elapsed();

        let scan_start = Instant::now();
        let mut cursor = None;
        let mut scanned = 0usize;
        loop {
            let page = rows.scan_ids(cursor, 1_000).expect("scan page-backed row IDs");
            if page.is_empty() { break; }
            scanned += page.len();
            cursor = page.last().copied();
        }
        assert_eq!(scanned, DISK_ROWS);
        println!("disk_rows={DISK_ROWS}");
        println!("disk_insert_batch_size={DISK_BATCH}");
        println!("disk_row_insert_elapsed_ms={}", disk_insert_elapsed.as_millis());
        println!("disk_point_lookups={}", DISK_ROWS / 10);
        println!("disk_row_lookup_elapsed_ms={}", disk_lookup_elapsed.as_millis());
        println!("disk_id_scan_elapsed_ms={}", scan_start.elapsed().as_millis());
        println!("disk_page_cache={:?}", rows.page_cache_stats());
    }
    for suffix in [".lock", ".pages", ".pages.wal"] {
        let mut path = PathBuf::from(disk_path.as_os_str());
        path.as_mut_os_string().push(suffix);
        let _ = fs::remove_file(path);
    }
}
