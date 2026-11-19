use postemberdb::{Database, EmberForge7NamespaceKind, EmberForge7PlayerProfile, EmberForge7Store, SqlRowStore, Value};
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
    // EF7-specific Minecraft data path: one atomic player batch, point UUID
    // reads, typed profile lookups, and world chunk scans. This is deliberately
    // measured separately from Database's in-memory SQL executor.
    let ef7_path = std::env::temp_dir().join(format!(
        "postemberdb-ef7-minecraft-bench-{}.pbd", std::process::id()
    ));
    {
        let mut ef7 = EmberForge7Store::open(&ef7_path, 128).expect("open EF7 engine");
        ef7.create_namespace(1, None, EmberForge7NamespaceKind::Server, "benchmark")
            .expect("create EF7 server namespace");
        ef7.create_namespace(2, Some(1), EmberForge7NamespaceKind::World, "survival")
            .expect("create EF7 world namespace");

        const EF7_PLAYERS: usize = 2_000;
        let player_batch = (0..EF7_PLAYERS).map(|id| {
            let mut uuid = [0u8; 16];
            uuid[..8].copy_from_slice(&(id as u64).to_be_bytes());
            uuid[8..].copy_from_slice(&((id as u64).wrapping_mul(0x9e37_79b9)).to_be_bytes());
            (1u64, id as u64 + 1, uuid, vec![(id % 251) as u8; 512])
        }).collect::<Vec<_>>();
        let player_batch_start = Instant::now();
        ef7.apply_player_batch(&player_batch, &[]).expect("atomic EF7 player batch");
        let player_batch_elapsed = player_batch_start.elapsed();

        let player_lookup_start = Instant::now();
        for id in (0..EF7_PLAYERS).step_by(10) {
            let mut uuid = [0u8; 16];
            uuid[..8].copy_from_slice(&(id as u64).to_be_bytes());
            uuid[8..].copy_from_slice(&((id as u64).wrapping_mul(0x9e37_79b9)).to_be_bytes());
            assert!(ef7.get_player(1, uuid).expect("EF7 UUID lookup").is_some());
        }
        let player_lookup_elapsed = player_lookup_start.elapsed();

        const EF7_PROFILE_COUNT: usize = 128;
        let profile_start = Instant::now();
        for id in 0..EF7_PROFILE_COUNT {
            let mut uuid = [0u8; 16];
            uuid[..8].copy_from_slice(&((10_000 + id) as u64).to_be_bytes());
            uuid[8..].copy_from_slice(&((20_000 + id) as u64).to_be_bytes());
            let profile = EmberForge7PlayerProfile {
                uuid,
                username: format!("Player_{id}"),
                first_joined_ms: 1_700_000_000_000,
                last_seen_ms: 1_700_000_100_000,
                playtime_ticks: (id as u64) * 20,
                custom_data: vec![(id % 251) as u8; 1_024],
            };
            ef7.put_player_profile(1, 10_000 + id as u64, &profile)
                .expect("write typed EF7 player profile");
        }
        let profile_write_elapsed = profile_start.elapsed();
        let name_search_start = Instant::now();
        let matches = ef7.search_player_profiles_by_name(1, "player_1", 100)
            .expect("EF7 player name prefix search");
        assert!(!matches.is_empty());
        let name_search_elapsed = name_search_start.elapsed();

        const EF7_CHUNKS: usize = 128;
        let chunk_write_start = Instant::now();
        for id in 0..EF7_CHUNKS {
            let x = (id as i32 % 16) - 8;
            let z = (id as i32 / 16) - 4;
            ef7.put_chunk(2, x, z, &vec![(id % 251) as u8; 8 * 1024])
                .expect("write EF7 world chunk");
        }
        let chunk_write_elapsed = chunk_write_start.elapsed();
        let chunk_scan_start = Instant::now();
        let chunks = ef7.scan_chunks(2, -8, -4, 7, 3, EF7_CHUNKS)
            .expect("scan EF7 chunk rectangle");
        assert_eq!(chunks.len(), EF7_CHUNKS);

        println!("ef7_player_batch_rows={EF7_PLAYERS}");
        println!("ef7_player_batch_elapsed_ms={}", player_batch_elapsed.as_millis());
        println!("ef7_player_uuid_lookups={}", EF7_PLAYERS / 10);
        println!("ef7_player_uuid_lookup_elapsed_ms={}", player_lookup_elapsed.as_millis());
        println!("ef7_typed_profiles={EF7_PROFILE_COUNT}");
        println!("ef7_profile_write_elapsed_ms={}", profile_write_elapsed.as_millis());
        println!("ef7_profile_name_search_elapsed_ms={}", name_search_elapsed.as_millis());
        println!("ef7_world_chunks={EF7_CHUNKS}");
        println!("ef7_chunk_write_elapsed_ms={}", chunk_write_elapsed.as_millis());
        println!("ef7_chunk_scan_elapsed_ms={}", chunk_scan_start.elapsed().as_millis());
        println!("ef7_page_cache={:?}", ef7.stats());
    }
    for suffix in [".ef7.lock", ".ef7.pages", ".ef7.wal"] {
        let mut path = PathBuf::from(ef7_path.as_os_str());
        path.as_mut_os_string().push(suffix);
        let _ = fs::remove_file(path);
    }

}
