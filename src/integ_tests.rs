//! Integration tests for coordinator and worker behavior
//!
//! These tests use SQLite in-memory databases and real CSV files to test
//! end to end scenarios of the loader.

#[cfg(test)]
mod tests {
    use crate::{
        coordination::{
            Coordinator, DsqlConfig, FileFormat, LoadConfigBuilder,
            coordinator::LoadResult,
            manifest::{
                ChunkResultFile, ChunkStatus, LocalManifestStorage, ManifestStorage, OnConflict,
                ParquetConfig,
            },
        },
        db::{
            Pool, SchemaInferrer,
            pool::{PoolArgsBuilder, PoolConnection, pool as build_dsql_pool},
        },
        formats::{
            DelimitedConfig, FileReader, delimited::reader::GenericDelimitedReader,
            parquet::GenericParquetReader, pgdump::find_copy_block,
        },
        io::{ByteReader, LocalFileByteReader},
        runner::{
            ExportArgs, Format, LoadArgs, MigrateArgs, VerifyMode, run_export, run_load,
            run_migrate,
        },
    };
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::collections::{HashMap, HashSet};
    use std::io::Write;
    use std::process::Command;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::fs::{self, File};
    use tokio::io::AsyncWriteExt;

    // ============ Test Helpers ============

    /// Helper to create a standard test CSV file with id,name,value,amount columns
    async fn create_test_csv(dir: &TempDir, filename: &str, num_rows: usize) -> String {
        let path = dir.path().join(filename);
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"id,name,value,amount\n").await.unwrap();
        for i in 0..num_rows {
            let line = format!("{},name_{},{}.5,{}\n", i, i, i, i * 10);
            file.write_all(line.as_bytes()).await.unwrap();
        }
        file.flush().await.unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Helper to create a test CSV file with custom content (rows include header)
    async fn create_csv_with_content(dir: &TempDir, filename: &str, content: &[&str]) -> String {
        let path = dir.path().join(filename);
        let mut file = File::create(&path).await.unwrap();
        for line in content {
            file.write_all(line.as_bytes()).await.unwrap();
        }
        file.flush().await.unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Helper to create a standard test TSV file with id,name,value,amount columns (tab-separated)
    async fn create_test_tsv(dir: &TempDir, filename: &str, num_rows: usize) -> String {
        let path = dir.path().join(filename);
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"id\tname\tvalue\tamount\n").await.unwrap();
        for i in 0..num_rows {
            let line = format!("{}\tname_{}\t{}.5\t{}\n", i, i, i, i * 10);
            file.write_all(line.as_bytes()).await.unwrap();
        }
        file.flush().await.unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Helper to create a SQLite pool and table
    async fn setup_sqlite_table(table_name: &str, columns: &str) -> Pool {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let sql = format!("CREATE TABLE {} ({})", table_name, columns);
            sqlx::query(&sql).execute(&mut **sqlite_conn).await.unwrap();
        }
        pool
    }

    /// Helper that builds a CSV `DelimitedConfig` with `has_header: true`.
    ///
    /// Most tests in this file create header-bearing fixtures via
    /// `create_test_csv` / `create_csv_with_content`, but the library default
    /// for `DelimitedConfig::csv()` is `has_header: false` (matches Postgres
    /// `COPY FROM` HEADER default). This helper makes the test intent explicit.
    fn csv_with_header() -> DelimitedConfig {
        DelimitedConfig {
            has_header: true,
            ..DelimitedConfig::csv()
        }
    }

    /// Helper that builds a TSV `DelimitedConfig` with `has_header: true`.
    fn tsv_with_header() -> DelimitedConfig {
        DelimitedConfig {
            has_header: true,
            ..DelimitedConfig::tsv()
        }
    }

    /// Helper to run a basic CSV load test with defaults
    async fn run_csv_load(
        pool: &Pool,
        table_name: &str,
        csv_path: &str,
        worker_count: usize,
        chunk_size: u64,
    ) -> LoadResult {
        let byte_reader = LocalFileByteReader::new(csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path.to_string())
            .target_table(table_name.to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(worker_count)
            .chunk_size_bytes(chunk_size)
            .batch_size(10)
            .batch_concurrency(2)
            .create_table_if_missing(true)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .debug(true) // Enable debug for tests to verify verbose output
            .build()
            .unwrap();

        coordinator.run_load(&config).await.unwrap()
    }

    /// Helper to run a basic TSV load test with defaults
    async fn run_tsv_load(
        pool: &Pool,
        table_name: &str,
        tsv_path: &str,
        worker_count: usize,
        chunk_size: u64,
    ) -> LoadResult {
        let byte_reader = LocalFileByteReader::new(tsv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, tsv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(tsv_path.to_string())
            .target_table(table_name.to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(worker_count)
            .chunk_size_bytes(chunk_size)
            .batch_size(10)
            .batch_concurrency(2)
            .create_table_if_missing(true)
            .file_format(FileFormat::Tsv(tsv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .build()
            .unwrap();

        coordinator.run_load(&config).await.unwrap()
    }

    /// Helper to query table row count
    async fn get_table_count(pool: &Pool, table_name: &str) -> i64 {
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let sql = format!("SELECT COUNT(*) FROM {}", table_name);
            let (count,): (i64,) = sqlx::query_as(&sql)
                .fetch_one(&mut **sqlite_conn)
                .await
                .unwrap();
            return count;
        }
        0
    }

    /// True if `table_name` exists in the SQLite test DB (sqlite_master lookup).
    async fn sqlite_table_exists(pool: &Pool, table_name: &str) -> bool {
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let (n,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?",
            )
            .bind(table_name)
            .fetch_one(&mut **sqlite_conn)
            .await
            .unwrap();
            return n > 0;
        }
        false
    }

    /// Build a `LoadArgs` for a CSV load against `pool`, with the atomic and
    /// if-not-exists toggles exposed (everything else defaulted).
    fn atomic_load_args(
        pool: &Pool,
        table: &str,
        csv_path: &str,
        atomic: bool,
        if_not_exists: bool,
    ) -> LoadArgs {
        LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path.to_string(),
            target_table: table.to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 1_000_000,
            batch_size: 10,
            batch_concurrency: 1,
            create_table_if_missing: if_not_exists,
            atomic,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        }
    }

    #[tokio::test]
    async fn test_atomic_requires_if_not_exists() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "ok.csv", 5).await;
        let pool =
            setup_sqlite_table("atomic_pre", "id TEXT, name TEXT, value TEXT, amount TEXT").await;

        let args = atomic_load_args(&pool, "atomic_pre", &csv_path, true, false);
        let err = run_load(args).await.unwrap_err().to_string();
        assert!(err.contains("--atomic requires --if-not-exists"), "{err}");
    }

    #[tokio::test]
    async fn test_atomic_refuses_existing_table() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "ok.csv", 5).await;
        // Pre-existing table with a row: --atomic must never drop it.
        let pool = setup_sqlite_table(
            "atomic_exists",
            "id TEXT, name TEXT, value TEXT, amount TEXT",
        )
        .await;
        pool.execute_query("INSERT INTO atomic_exists VALUES ('x','x','x','x')")
            .await
            .unwrap();

        let args = atomic_load_args(&pool, "atomic_exists", &csv_path, true, true);
        let err = run_load(args).await.unwrap_err().to_string();
        assert!(err.contains("refuses to load into existing table"), "{err}");
        assert_eq!(get_table_count(&pool, "atomic_exists").await, 1);
    }

    #[tokio::test]
    async fn test_atomic_drops_table_on_failure() {
        let temp_dir = TempDir::new().unwrap();
        // Inconsistent column counts force a deterministic partial failure.
        let csv_path = create_csv_with_content(
            &temp_dir,
            "bad.csv",
            &[
                "id,name,value\n",
                "1,alice,100\n",
                "2,bob\n", // missing column → fails
                "3,charlie,300\n",
                "4,diana,400,extra\n", // extra column → fails
            ],
        )
        .await;
        let pool = Pool::sqlite_in_memory().await.unwrap();

        let args = atomic_load_args(&pool, "atomic_rollback", &csv_path, true, true);
        let err = run_load(args).await.unwrap_err().to_string();
        assert!(err.contains("rolled back"), "{err}");
        assert!(
            !sqlite_table_exists(&pool, "atomic_rollback").await,
            "atomic load must DROP the table it created on failure"
        );
    }

    #[tokio::test]
    async fn test_atomic_keeps_table_on_success() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "ok.csv", 20).await;
        let pool = Pool::sqlite_in_memory().await.unwrap();

        let args = atomic_load_args(&pool, "atomic_success", &csv_path, true, true);
        let result = run_load(args).await.unwrap();
        assert_eq!(result.records_failed, 0);
        assert!(sqlite_table_exists(&pool, "atomic_success").await);
        assert_eq!(get_table_count(&pool, "atomic_success").await, 20);
    }

    // ============ Tests ============

    #[tokio::test]
    async fn test_basic_load_single_worker() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "test.csv", 100).await;
        let pool =
            setup_sqlite_table("test_table", "id TEXT, name TEXT, value TEXT, amount TEXT").await;

        let result = run_csv_load(&pool, "test_table", &csv_path, 1, 1000).await;

        assert!(result.chunks_processed > 0);
        assert_eq!(result.records_loaded, 100);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_multiple_workers_and_chunk_distribution() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "test.csv", 500).await;
        let pool =
            setup_sqlite_table("test_table", "id TEXT, name TEXT, value TEXT, amount TEXT").await;

        let result = run_csv_load(&pool, "test_table", &csv_path, 4, 800).await;

        assert!(result.chunks_processed > 1, "Should create multiple chunks");
        assert_eq!(result.records_loaded, 500);
        assert_eq!(result.records_failed, 0);

        // Verify work was distributed across multiple workers
        let unique_workers: HashSet<_> =
            result.chunk_results.iter().map(|r| &r.worker_id).collect();
        assert!(
            unique_workers.len() >= 2,
            "Expected at least 2 workers, got {}",
            unique_workers.len()
        );
    }

    #[tokio::test]
    async fn test_parquet_load() {
        let temp_dir = TempDir::new().unwrap();

        // Create a test Parquet file
        let parquet_path = temp_dir.path().join("test.parquet");
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]);

        let file = std::fs::File::create(&parquet_path).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(50)
            .build();

        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).unwrap();

        // Write 100 rows
        let num_rows = 100;
        let id_array = Int32Array::from_iter_values(0..num_rows);
        let name_array = StringArray::from_iter((0..num_rows).map(|i| Some(format!("name_{}", i))));
        let value_array = Float64Array::from_iter_values((0..num_rows).map(|i| (i as f64) * 1.5));

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let parquet_path_str = parquet_path.to_str().unwrap().to_string();

        // Set up test environment
        let pool = Pool::sqlite_in_memory().await.unwrap();

        // Manually create the SQLite table with correct types
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let create_table_sql = "CREATE TABLE test_parquet (id INTEGER, name TEXT, value REAL)";
            sqlx::query(create_table_sql)
                .execute(&mut **sqlite_conn)
                .await
                .unwrap();
        }

        let byte_reader = LocalFileByteReader::new(&parquet_path_str);
        let parquet_reader = GenericParquetReader::new(byte_reader).await.unwrap();
        let file_reader: Arc<dyn FileReader> = Arc::new(parquet_reader);

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let schema_inferrer = SchemaInferrer { has_header: true };

        let coordinator = Coordinator::new(manifest_storage, file_reader, schema_inferrer, pool);

        let config = LoadConfigBuilder::default()
            .source_uri(parquet_path_str)
            .target_table("test_parquet".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(2)
            .chunk_size_bytes(500)
            .batch_size(20)
            .batch_concurrency(2)
            .create_table_if_missing(false) // Table already created
            .file_format(FileFormat::Parquet(ParquetConfig::default()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();

        // Verify results
        assert!(result.chunks_processed > 0);
        assert_eq!(result.records_loaded, 100);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_tsv_load() {
        let temp_dir = TempDir::new().unwrap();
        let tsv_path = create_test_tsv(&temp_dir, "test.tsv", 100).await;
        let pool =
            setup_sqlite_table("test_tsv", "id TEXT, name TEXT, value TEXT, amount TEXT").await;

        let result = run_tsv_load(&pool, "test_tsv", &tsv_path, 2, 1000).await;

        // Verify results
        assert!(result.chunks_processed > 0);
        assert_eq!(result.records_loaded, 100);
        assert_eq!(result.records_failed, 0);

        // Verify data was correctly parsed (tab-separated values)
        let count = get_table_count(&pool, "test_tsv").await;
        assert_eq!(count, 100);
    }

    #[tokio::test]
    async fn test_unique_constraint_conflict_handling() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "duplicates.csv",
            &[
                "id,name,value\n",
                "1,first,100\n",
                "2,second,200\n",
                "1,duplicate,300\n", // Duplicate id=1
                "3,third,400\n",
            ],
        )
        .await;

        let pool = setup_sqlite_table(
            "test_unique",
            "id INTEGER PRIMARY KEY, name TEXT, value INTEGER",
        )
        .await;

        // Verify that the table has unique constraints
        assert!(
            pool.has_unique_constraints("public", "test_unique")
                .await
                .unwrap()
        );

        let result = run_csv_load(&pool, "test_unique", &csv_path, 1, 1000).await;

        assert_eq!(result.records_failed, 0, "Should have no failed records");
        assert_eq!(
            get_table_count(&pool, "test_unique").await,
            3,
            "Should have exactly 3 unique records"
        );

        // Verify first occurrence of duplicate key is kept
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let row: (i64, String, i64) =
                sqlx::query_as("SELECT id, name, value FROM test_unique WHERE id = 1")
                    .fetch_one(&mut **sqlite_conn)
                    .await
                    .unwrap();
            assert_eq!(row.1, "first", "First record should be kept");
            assert_eq!(row.2, 100, "First value should be kept");
        }
    }

    #[tokio::test]
    async fn test_datetime_formats_load() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "datetime.csv",
            &[
                "id,iso_date,us_date,euro_date,timestamp_space,timestamp_t\n",
                "1,2024-01-15,01/15/2024,15-01-2024,2024-01-15 10:30:00,2024-01-15T10:30:00\n",
                "2,2024-02-29,02/29/2024,29-02-2024,2024-02-29 09:15:22,2024-02-29T09:15:22\n",
                "3,2024-12-31,12/31/2024,31-12-2024,2024-12-31 23:59:59,2024-12-31T23:59:59\n",
            ],
        )
        .await;

        // Note: SQLite doesn't have native DATE/TIMESTAMP types, so we use TEXT
        // This matches how SQLite would interpret DATE/TIMESTAMP columns in practice
        let pool = setup_sqlite_table(
            "test_datetime",
            "id INTEGER, iso_date TEXT, us_date TEXT, euro_date TEXT, timestamp_space TEXT, timestamp_t TEXT",
        )
        .await;

        let result = run_csv_load(&pool, "test_datetime", &csv_path, 2, 1000).await;

        assert_eq!(result.records_loaded, 3);
        assert_eq!(result.records_failed, 0);
        assert_eq!(get_table_count(&pool, "test_datetime").await, 3);

        // Verify data format is preserved and queryable
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let row1: (String, String) =
                sqlx::query_as("SELECT iso_date, timestamp_space FROM test_datetime WHERE id = 1")
                    .fetch_one(&mut **sqlite_conn)
                    .await
                    .unwrap();
            assert_eq!(row1.0, "2024-01-15");
            assert_eq!(row1.1, "2024-01-15 10:30:00");

            // Verify leap year date loaded correctly
            let leap: (String,) = sqlx::query_as("SELECT iso_date FROM test_datetime WHERE id = 2")
                .fetch_one(&mut **sqlite_conn)
                .await
                .unwrap();
            assert_eq!(leap.0, "2024-02-29");
        }
    }

    // ============ Runner API Tests ============

    #[tokio::test]
    async fn test_runner_basic_csv_load() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "test.csv", 100).await;
        let pool = setup_sqlite_table(
            "runner_test_table",
            "id TEXT, name TEXT, value TEXT, amount TEXT",
        )
        .await;

        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path,
            target_table: "runner_test_table".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 2,
            chunk_size_bytes: 1000,
            batch_size: 10,
            batch_concurrency: 2,
            create_table_if_missing: false, // Table already created
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: true,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };

        let result = run_load(args).await.unwrap();

        assert!(result.chunks_processed > 0);
        assert_eq!(result.records_loaded, 100);
        assert_eq!(result.records_failed, 0);
        assert_eq!(get_table_count(&pool, "runner_test_table").await, 100);
    }

    #[tokio::test]
    async fn test_runner_parquet_load() {
        let temp_dir = TempDir::new().unwrap();

        // Create a test Parquet file
        let parquet_path = temp_dir.path().join("runner_test.parquet");
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]);

        let file = std::fs::File::create(&parquet_path).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(25)
            .build();

        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).unwrap();

        // Write 50 rows
        let num_rows = 50;
        let id_array = Int32Array::from_iter_values(0..num_rows);
        let name_array = StringArray::from_iter((0..num_rows).map(|i| Some(format!("name_{}", i))));
        let value_array = Float64Array::from_iter_values((0..num_rows).map(|i| (i as f64) * 2.5));

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Set up SQLite pool and table
        let pool = Pool::sqlite_in_memory().await.unwrap();
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let create_table_sql =
                "CREATE TABLE runner_parquet (id INTEGER, name TEXT, value REAL)";
            sqlx::query(create_table_sql)
                .execute(&mut **sqlite_conn)
                .await
                .unwrap();
        }

        let parquet_path_str = parquet_path.to_str().unwrap().to_string();

        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: parquet_path_str,
            target_table: "runner_parquet".to_string(),
            schema: "public".to_string(),
            format: Format::Parquet,
            worker_count: 2,
            chunk_size_bytes: 500,
            batch_size: 20,
            batch_concurrency: 2,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: true,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: None,
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };

        let result = run_load(args).await.unwrap();

        // Verify results
        assert!(result.chunks_processed > 0);
        assert_eq!(result.records_loaded, 50);
        assert_eq!(result.records_failed, 0);
        assert_eq!(get_table_count(&pool, "runner_parquet").await, 50);
    }

    #[tokio::test]
    async fn test_table_does_not_exist_error() {
        // This test verifies that:
        // 1. When a table doesn't exist and --if-not-exists is not set, the loader fails immediately
        // 2. The error message provides a helpful hint to use --if-not-exists

        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "test.csv", 10).await;

        // Create a pool but DO NOT create the table
        let pool = Pool::sqlite_in_memory().await.unwrap();

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool,
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path.to_string())
            .target_table("nonexistent_table".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false) // This is the key - not creating the table
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .build()
            .unwrap();

        // Attempt to run the load and expect it to fail
        let result = coordinator.run_load(&config).await;

        assert!(result.is_err(), "Load should fail when table doesn't exist");

        let error = result.unwrap_err();
        let error_msg = error.to_string();

        // Verify error message contains the table name
        assert!(
            error_msg.contains("nonexistent_table"),
            "Error should mention the table name. Got: {}",
            error_msg
        );

        // Verify error message contains the helpful hint
        assert!(
            error_msg.contains("--if-not-exists"),
            "Error should suggest using --if-not-exists flag. Got: {}",
            error_msg
        );

        // Verify the error message indicates the table doesn't exist
        assert!(
            error_msg.contains("does not exist") || error_msg.contains("not found"),
            "Error should clearly state the table doesn't exist. Got: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_manifest_persisted_on_errors() {
        // This test verifies that when errors occur and no manifest_dir is specified,
        // the temp directory is persisted for debugging

        let temp_dir = TempDir::new().unwrap();

        // Create table with CHECK constraint that will cause failures
        let pool = Pool::sqlite_in_memory().await.unwrap();
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let sql =
                "CREATE TABLE test_persist_manifest (id INTEGER, value INTEGER CHECK(value > 0))";
            sqlx::query(sql).execute(&mut **sqlite_conn).await.unwrap();
        }

        // Create CSV with values that will fail
        let csv_path = create_csv_with_content(
            &temp_dir,
            "invalid.csv",
            &[
                "id,value\n",
                "1,-100\n", // Violates CHECK constraint
            ],
        )
        .await;

        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path,
            target_table: "test_persist_manifest".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 1000,
            batch_size: 10,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None, // Don't specify manifest dir - this is key
            quiet: true,
            debug: true,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool),
        };

        let result = run_load(args).await.unwrap();

        // Verify errors occurred
        assert!(result.records_failed > 0, "Should have failed records");

        // Verify manifest directory was persisted
        assert!(
            result.persisted_manifest_dir.is_some(),
            "Manifest directory should be persisted when errors occur"
        );

        let manifest_path = result.persisted_manifest_dir.as_ref().unwrap();

        // Verify the directory exists
        assert!(
            manifest_path.exists(),
            "Persisted manifest directory should exist: {:?}",
            manifest_path
        );

        // Verify we can read the manifest file
        let manifest_file = manifest_path
            .join("jobs")
            .join(&result.job_id)
            .join("manifest.json");
        assert!(
            manifest_file.exists(),
            "Manifest file should exist: {:?}",
            manifest_file
        );

        // Verify we can read chunk result files with errors
        let chunk_result_file = manifest_path
            .join("jobs")
            .join(&result.job_id)
            .join("chunks")
            .join("0000.result");
        assert!(
            chunk_result_file.exists(),
            "Chunk result file should exist: {:?}",
            chunk_result_file
        );

        // Read and verify the result file contains error information
        let result_content = std::fs::read_to_string(&chunk_result_file).unwrap();
        let result_json: serde_json::Value = serde_json::from_str(&result_content).unwrap();

        assert!(
            result_json["errors"].as_array().is_some(),
            "Result should contain errors array"
        );
        assert!(
            !result_json["errors"].as_array().unwrap().is_empty(),
            "Errors array should not be empty"
        );
        assert_eq!(
            result_json["status"].as_str().unwrap(),
            "failed",
            "Status should be 'failed'"
        );

        // Clean up the persisted directory
        std::fs::remove_dir_all(manifest_path).unwrap();
    }

    #[tokio::test]
    async fn test_failed_record_error_reporting() {
        // This test verifies that:
        // 1. Failed records are tracked accurately
        // 2. Error messages contain helpful batch context
        // 3. Chunk results show correct status and counts

        let temp_dir = TempDir::new().unwrap();

        // Create table with CHECK constraint that will cause failures
        let pool = Pool::sqlite_in_memory().await.unwrap();
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            // Create table with CHECK constraint requiring value > 0
            let sql = "CREATE TABLE test_check_constraint (id INTEGER, name TEXT, value INTEGER CHECK(value > 0))";
            sqlx::query(sql).execute(&mut **sqlite_conn).await.unwrap();
        }

        // Create CSV with values that violate the CHECK constraint
        let csv_path = create_csv_with_content(
            &temp_dir,
            "invalid_values.csv",
            &[
                "id,name,value\n",
                "1,Alice,-10\n",   // Negative value violates CHECK
                "2,Bob,-20\n",     // Negative value violates CHECK
                "3,Charlie,-30\n", // Negative value violates CHECK
            ],
        )
        .await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let coordinator = Coordinator::new(
            manifest_storage.clone(),
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path.to_string())
            .target_table("test_check_constraint".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(3) // All 3 records in one batch
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .debug(true) // Enable debug mode to get verbose output
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();

        // Verify that failures were properly tracked
        assert_eq!(result.chunks_processed, 1, "Should process 1 chunk");
        assert_eq!(
            result.records_loaded, 0,
            "Should load 0 records (batch failed)"
        );
        assert_eq!(
            result.records_failed, 3,
            "Should report 3 failed records (entire batch)"
        );

        // Verify no data was actually inserted
        assert_eq!(get_table_count(&pool, "test_check_constraint").await, 0);

        // Verify chunk result contains detailed error information
        let chunk_result = &result.chunk_results[0];
        assert_eq!(chunk_result.status, ChunkStatus::Failed);
        assert_eq!(
            chunk_result.records_failed, 3,
            "Chunk should show 3 failed records"
        );
        assert_eq!(chunk_result.records_loaded, 0);
        assert!(!chunk_result.errors.is_empty(), "Should have error records");

        // Check that error message contains helpful batch context
        let error_msg = &chunk_result.errors[0].error_message;
        assert!(
            error_msg.contains("Batch context"),
            "Error should contain batch context. Got: {}",
            error_msg
        );
        assert!(
            error_msg.contains("Batch size: 3 records"),
            "Error should show batch size. Got: {}",
            error_msg
        );
        assert!(
            error_msg.contains("First record sample:"),
            "Error should show first record sample. Got: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_schema_qualified_table_load() {
        // This test verifies schema-qualified table operations
        // Note: SQLite doesn't support schemas, so we simulate with table name prefix

        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "test.csv", 50).await;

        // Create table simulating schema prefix (sales_orders instead of sales.orders)
        let pool = setup_sqlite_table(
            "sales_orders", // Simulates sales.orders
            "id TEXT, name TEXT, value TEXT, amount TEXT",
        )
        .await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let schema_inferrer = SchemaInferrer { has_header: true };
        let coordinator =
            Coordinator::new(manifest_storage, file_reader, schema_inferrer, pool.clone());

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path.to_string())
            .target_table("orders".to_string())
            .schema("sales".to_string()) // Non-public schema
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(2)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(2)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();

        assert_eq!(result.records_loaded, 50);
        assert_eq!(result.records_failed, 0);

        // Verify data was loaded to the correct table
        assert_eq!(get_table_count(&pool, "sales_orders").await, 50);
    }

    #[tokio::test]
    async fn test_default_public_schema() {
        // Verify that "public" schema works (default behavior)
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "test.csv", 20).await;

        let pool =
            setup_sqlite_table("test_table", "id TEXT, name TEXT, value TEXT, amount TEXT").await;

        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path,
            target_table: "test_table".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 1000,
            batch_size: 10,
            batch_concurrency: 2,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: true,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };

        let result = run_load(args).await.unwrap();

        assert_eq!(result.records_loaded, 20);
        assert_eq!(result.records_failed, 0);
        assert_eq!(get_table_count(&pool, "test_table").await, 20);
    }

    #[tokio::test]
    async fn test_schema_qualified_ddl_generation() {
        // Test that generated DDL includes schema qualification
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "test.csv", 10).await;

        let pool = Pool::sqlite_in_memory().await.unwrap();

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let schema_inferrer = SchemaInferrer { has_header: true };
        let coordinator = Coordinator::new(manifest_storage, file_reader, schema_inferrer, pool);

        // Use a simulated schema for SQLite (analytics_metrics instead of analytics.metrics)
        let config = LoadConfigBuilder::default()
            .source_uri(csv_path.to_string())
            .target_table("metrics".to_string())
            .schema("analytics".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(true)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();

        assert_eq!(result.records_loaded, 10);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_resume_incomplete_job() {
        // Create test CSV data
        let temp_dir = TempDir::new().unwrap();
        let csv_path = temp_dir.path().join("test.csv").display().to_string();

        // Create CSV with 50 records
        let mut csv_content = String::from("id,name,value\n");
        for i in 1..=50 {
            csv_content.push_str(&format!("{},name{},{}\n", i, i, i * 10));
        }
        fs::write(&csv_path, csv_content).await.unwrap();

        // Set up SQLite table with primary key for idempotency
        let pool =
            setup_sqlite_table("test_resume", "id TEXT PRIMARY KEY, name TEXT, value TEXT").await;

        // Create persistent manifest directory
        let manifest_dir = TempDir::new().unwrap();
        let manifest_path = manifest_dir.path().to_path_buf();

        // First load - complete successfully
        let args1 = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path.clone(),
            target_table: "test_resume".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 500, // Small chunks to create multiple chunks
            batch_size: 10,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: Some(manifest_path.clone()),
            quiet: true,
            debug: true,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };

        let result1 = run_load(args1).await.unwrap();
        let job_id = result1.job_id.clone();

        // Verify first load completed
        assert_eq!(result1.records_loaded, 50);
        assert!(result1.chunks_processed > 1, "Should have multiple chunks");

        // Count records after first load
        let mut conn = pool.acquire().await.unwrap();
        let count1 = if let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_resume")
                .fetch_one(&mut **sqlite_conn)
                .await
                .unwrap();
            count
        } else {
            panic!("Expected SQLite connection");
        };
        assert_eq!(count1, 50);

        // ===== CAPTURE MANIFEST STATE AFTER FIRST LOAD =====
        let chunks_dir = manifest_path.join("jobs").join(&job_id).join("chunks");

        // Read and verify all chunk result files
        let mut first_load_results = Vec::new();
        for chunk_id in 0..result1.chunks_processed {
            let result_path = chunks_dir.join(format!("{:04}.result", chunk_id));

            // Verify result file exists
            assert!(
                result_path.exists(),
                "Chunk {} result file should exist after first load",
                chunk_id
            );

            // Read and parse result file
            let content = fs::read_to_string(&result_path).await.unwrap();
            let result: ChunkResultFile = serde_json::from_str(&content).unwrap();

            // Verify chunk succeeded
            assert_eq!(
                result.status,
                ChunkStatus::Success,
                "Chunk {} should have Success status",
                chunk_id
            );
            assert_eq!(result.records_failed, 0);
            assert!(result.records_loaded > 0);

            first_load_results.push((chunk_id, result));
        }

        // Verify all chunks have claim files
        for chunk_id in 0..result1.chunks_processed {
            let claim_path = chunks_dir.join(format!("{:04}.claim", chunk_id));
            assert!(
                claim_path.exists(),
                "Chunk {} claim file should exist after first load",
                chunk_id
            );
        }

        // Second load - resume the same job (should do nothing since already complete)
        let args2 = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path.clone(),
            target_table: "test_resume".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 500,
            batch_size: 10,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: Some(manifest_path.clone()),
            quiet: true,
            debug: true,
            column_mappings: HashMap::new(),
            resume_job_id: Some(job_id.clone()),
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };

        let result2 = run_load(args2).await.unwrap();

        // Verify resume used same job_id
        assert_eq!(result2.job_id, job_id);

        // ===== VERIFY MANIFEST STATE AFTER RESUME =====
        // Verify result files are unchanged (proves chunks weren't re-executed)
        for (chunk_id, original_result) in &first_load_results {
            let result_path = chunks_dir.join(format!("{:04}.result", chunk_id));

            assert!(
                result_path.exists(),
                "Chunk {} result file should still exist after resume",
                chunk_id
            );

            // Read and verify content is identical
            let content = fs::read_to_string(&result_path).await.unwrap();
            let current_result: ChunkResultFile = serde_json::from_str(&content).unwrap();

            // Compare key fields - if timestamps match, file wasn't rewritten
            assert_eq!(current_result.chunk_id, original_result.chunk_id);
            assert_eq!(current_result.status, original_result.status);
            assert_eq!(
                current_result.records_loaded,
                original_result.records_loaded
            );
            assert_eq!(current_result.worker_id, original_result.worker_id);
            assert_eq!(
                current_result.started_at, original_result.started_at,
                "Chunk {} result file was rewritten (started_at changed)",
                chunk_id
            );
            assert_eq!(
                current_result.completed_at, original_result.completed_at,
                "Chunk {} result file was rewritten (completed_at changed)",
                chunk_id
            );
        }

        // Verify claim files still exist
        for chunk_id in 0..result1.chunks_processed {
            let claim_path = chunks_dir.join(format!("{:04}.claim", chunk_id));
            assert!(
                claim_path.exists(),
                "Chunk {} claim file should still exist after resume",
                chunk_id
            );
        }

        // Count records after resume - should still be 50 (no duplicates)
        let mut conn = pool.acquire().await.unwrap();
        let count2 = if let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_resume")
                .fetch_one(&mut **sqlite_conn)
                .await
                .unwrap();
            count
        } else {
            panic!("Expected SQLite connection");
        };
        assert_eq!(count2, 50, "Resume should not create duplicates");

        // All chunks should still be claimed (no new work done)
        assert_eq!(
            result2.chunks_processed, result1.chunks_processed,
            "Resume should process same number of chunks (no new work)"
        );
    }

    #[tokio::test]
    async fn test_resume_retries_failed_chunks() {
        // Create test CSV data - mix of valid and invalid values
        let temp_dir = TempDir::new().unwrap();
        let csv_path = temp_dir.path().join("test.csv").display().to_string();

        // Create CSV with 40 records - some will fail due to CHECK constraint
        let mut csv_content = String::from("id,name,value\n");
        for i in 1..=40 {
            csv_content.push_str(&format!("{},name{},{}\n", i, i, i * 10));
        }
        fs::write(&csv_path, csv_content).await.unwrap();

        // Set up SQLite table with CHECK constraint that will fail for values > 300
        let pool = setup_sqlite_table(
            "test_resume_failures",
            "id TEXT PRIMARY KEY, name TEXT, value INTEGER CHECK(value <= 300)",
        )
        .await;

        // Create persistent manifest directory
        let manifest_dir = TempDir::new().unwrap();
        let manifest_path = manifest_dir.path().to_path_buf();

        // First load - will have failures for records with value > 300 (ids 31-40)
        let args1 = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path.clone(),
            target_table: "test_resume_failures".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 200, // Small chunks to ensure multiple chunks
            batch_size: 5,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: Some(manifest_path.clone()),
            quiet: true,
            debug: true,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };

        // First load now fails hard when chunks are left without result
        // files (single-worker exits on first batch failure → trailing
        // chunks unprocessed). Pre-fix this returned a green LoadResult
        // with silent drops; post-fix the operator gets a load-incomplete
        // error pointing at --resume-job-id. Recover the job_id by
        // listing the manifest dir.
        let err = run_load(args1)
            .await
            .expect_err("first load must fail hard when worker exits with chunks unprocessed");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Load incomplete"),
            "expected load-incomplete error, got: {msg}"
        );
        let job_id = {
            let jobs_dir = manifest_path.join("jobs");
            let mut entries = fs::read_dir(&jobs_dir).await.unwrap();
            let mut found = None;
            while let Some(entry) = entries.next_entry().await.unwrap() {
                if entry.file_type().await.unwrap().is_dir() {
                    found = Some(entry.file_name().to_string_lossy().into_owned());
                    break;
                }
            }
            found.expect("jobs dir must contain a job subdir")
        };

        // Count records after first load - should only have records with value <= 300
        let mut conn = pool.acquire().await.unwrap();
        let count1 = if let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_resume_failures")
                .fetch_one(&mut **sqlite_conn)
                .await
                .unwrap();
            count
        } else {
            panic!("Expected SQLite connection");
        };
        println!("Records in DB after first load: {}", count1);
        assert!(
            count1 < 40,
            "Should have fewer than 40 records due to failures"
        );
        assert!(count1 <= 30, "Should have at most 30 records (ids 1-30)");

        // Fetch existing records before dropping table
        let mut existing_records = Vec::new();
        if let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
            let rows: Vec<(String, String, i64)> =
                sqlx::query_as("SELECT id, name, value FROM test_resume_failures")
                    .fetch_all(&mut **sqlite_conn)
                    .await
                    .unwrap();
            existing_records = rows;
        }
        drop(conn);

        // Now drop and recreate table WITHOUT the CHECK constraint
        let mut conn = pool.acquire().await.unwrap();
        if let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
            sqlx::query("DROP TABLE test_resume_failures")
                .execute(&mut **sqlite_conn)
                .await
                .unwrap();
            sqlx::query(
                "CREATE TABLE test_resume_failures (id TEXT PRIMARY KEY, name TEXT, value INTEGER)",
            )
            .execute(&mut **sqlite_conn)
            .await
            .unwrap();

            // Re-insert the successfully loaded records
            for (id, name, value) in existing_records {
                sqlx::query("INSERT INTO test_resume_failures VALUES (?, ?, ?)")
                    .bind(id)
                    .bind(name)
                    .bind(value)
                    .execute(&mut **sqlite_conn)
                    .await
                    .unwrap();
            }
        }
        drop(conn);

        // Resume the job - failed chunks should be retried without the constraint
        let args2 = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path.clone(),
            target_table: "test_resume_failures".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 200,
            batch_size: 5,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: Some(manifest_path.clone()),
            quiet: true,
            debug: true,
            column_mappings: HashMap::new(),
            resume_job_id: Some(job_id.clone()),
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };

        let result2 = run_load(args2).await.unwrap();

        // Verify resume used same job_id
        assert_eq!(result2.job_id, job_id);

        println!(
            "Second load: loaded={}, failed={}",
            result2.records_loaded, result2.records_failed
        );

        // Verify resume retried the failed chunks and succeeded
        assert_eq!(result2.records_failed, 0, "Resume should have no failures");
        assert!(
            result2.records_loaded > 0,
            "Should have loaded some records on resume"
        );

        // Count records after resume - should have more than before
        let mut conn = pool.acquire().await.unwrap();
        let count2 = if let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_resume_failures")
                .fetch_one(&mut **sqlite_conn)
                .await
                .unwrap();
            count
        } else {
            panic!("Expected SQLite connection");
        };

        assert_eq!(count2, 40);
    }

    #[tokio::test]
    async fn test_on_conflict_do_update_with_primary_key() {
        let temp_dir = TempDir::new().unwrap();

        // Create initial CSV with 3 rows
        let csv_path1 = create_csv_with_content(
            &temp_dir,
            "initial.csv",
            &[
                "id,name,value\n",
                "1,Alice,100\n",
                "2,Bob,200\n",
                "3,Charlie,300\n",
            ],
        )
        .await;

        // Setup table with primary key on id
        let pool =
            setup_sqlite_table("test_upsert", "id TEXT PRIMARY KEY, name TEXT, value TEXT").await;

        // Load initial data with do-nothing mode
        let byte_reader = LocalFileByteReader::new(&csv_path1);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let coordinator = Coordinator::new(
            manifest_storage.clone(),
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path1.clone())
            .target_table("test_upsert".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .build()
            .unwrap();

        let result1 = coordinator.run_load(&config).await.unwrap();
        assert_eq!(result1.records_loaded, 3);

        // Create second CSV with overlapping IDs but different values
        let csv_path2 = create_csv_with_content(
            &temp_dir,
            "update.csv",
            &[
                "id,name,value\n",
                "2,Bob_Updated,250\n",
                "3,Charlie_Updated,350\n",
                "4,David,400\n",
            ],
        )
        .await;

        // Load with do-update mode (upsert)
        let byte_reader2 = LocalFileByteReader::new(&csv_path2);
        let file_reader2: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader2, csv_with_header()));

        let coordinator2 = Coordinator::new(
            manifest_storage,
            file_reader2,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config2 = LoadConfigBuilder::default()
            .source_uri(csv_path2)
            .target_table("test_upsert".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoUpdate)
            .build()
            .unwrap();

        let result2 = coordinator2.run_load(&config2).await.unwrap();
        assert_eq!(
            result2.records_loaded, 3,
            "All 3 records should be processed"
        );

        // Verify final state: should have 4 rows total, with updates applied
        let count = get_table_count(&pool, "test_upsert").await;
        assert_eq!(
            count, 4,
            "Should have 4 total rows (1 original + 2 updated + 1 new)"
        );

        // Verify Bob and Charlie were updated
        let mut conn = pool.acquire().await.unwrap();
        if let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
            let (bob_name, bob_value): (String, String) =
                sqlx::query_as("SELECT name, value FROM test_upsert WHERE id = '2'")
                    .fetch_one(&mut **sqlite_conn)
                    .await
                    .unwrap();
            assert_eq!(bob_name, "Bob_Updated");
            assert_eq!(bob_value, "250");

            let (charlie_name,): (String,) =
                sqlx::query_as("SELECT name FROM test_upsert WHERE id = '3'")
                    .fetch_one(&mut **sqlite_conn)
                    .await
                    .unwrap();
            assert_eq!(charlie_name, "Charlie_Updated");
        }
    }

    #[tokio::test]
    async fn test_on_conflict_error_mode() {
        let temp_dir = TempDir::new().unwrap();

        // Create initial CSV
        let csv_path1 = create_csv_with_content(
            &temp_dir,
            "initial.csv",
            &["id,name\n", "1,Alice\n", "2,Bob\n"],
        )
        .await;

        let pool = setup_sqlite_table("test_error_mode", "id TEXT PRIMARY KEY, name TEXT").await;

        // Load initial data
        let byte_reader = LocalFileByteReader::new(&csv_path1);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let coordinator = Coordinator::new(
            manifest_storage.clone(),
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path1)
            .target_table("test_error_mode".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::Error)
            .build()
            .unwrap();

        let result1 = coordinator.run_load(&config).await.unwrap();
        assert_eq!(result1.records_loaded, 2);

        // Try to load duplicate data with Error mode - should fail
        let csv_path2 = create_csv_with_content(
            &temp_dir,
            "duplicate.csv",
            &["id,name\n", "1,Alice_Duplicate\n"],
        )
        .await;

        let byte_reader2 = LocalFileByteReader::new(&csv_path2);
        let file_reader2: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader2, csv_with_header()));

        let coordinator2 = Coordinator::new(
            manifest_storage,
            file_reader2,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config2 = LoadConfigBuilder::default()
            .source_uri(csv_path2)
            .target_table("test_error_mode".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::Error)
            .build()
            .unwrap();

        let result2 = coordinator2.run_load(&config2).await.unwrap();

        // Should have failures due to constraint violation
        assert_eq!(
            result2.records_failed, 1,
            "Duplicate should fail with Error mode"
        );
        assert_eq!(result2.records_loaded, 0);
    }

    #[tokio::test]
    async fn test_on_conflict_do_update_without_constraints_fails() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path =
            create_csv_with_content(&temp_dir, "test.csv", &["id,name\n", "1,Alice\n"]).await;

        // Table WITHOUT any unique constraints
        let pool = setup_sqlite_table("test_no_constraints", "id TEXT, name TEXT").await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_no_constraints".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoUpdate)
            .build()
            .unwrap();

        // Should fail with clear error message
        let result = coordinator.run_load(&config).await;
        assert!(
            result.is_err(),
            "DoUpdate mode should fail without unique constraints"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("do-update mode requires table")
                && err_msg.contains("unique constraint"),
            "Error message should explain the requirement: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_error_chain_formatting_in_batch_errors() {
        // This test verifies that error chains are properly formatted
        // when batch operations fail

        let temp_dir = TempDir::new().unwrap();

        // Create CSV with data that will cause a CHECK constraint violation
        let csv_path = create_csv_with_content(
            &temp_dir,
            "constraint_violation.csv",
            &[
                "id,value\n",
                "1,-100\n", // Negative value violates CHECK constraint
            ],
        )
        .await;

        // Create table with CHECK constraint
        let pool = Pool::sqlite_in_memory().await.unwrap();
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let sql = "CREATE TABLE test_error_chain (id INTEGER, value INTEGER CHECK(value > 0))";
            sqlx::query(sql).execute(&mut **sqlite_conn).await.unwrap();
        }

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_error_chain".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .debug(true) // Enable debug mode to verify verbose output
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();

        // Should have failures
        assert_eq!(result.records_failed, 1, "Should have 1 failed record");
        assert_eq!(result.records_loaded, 0, "Should load 0 records");

        // Verify error message structure
        let chunk_result = &result.chunk_results[0];
        assert!(!chunk_result.errors.is_empty(), "Should have error records");

        let error_msg = &chunk_result.errors[0].error_message;

        // Verify error message contains the database error
        assert!(
            error_msg.contains("Database error:"),
            "Error should start with 'Database error:'. Got: {}",
            error_msg
        );

        // Verify error message contains batch context
        assert!(
            error_msg.contains("Batch context"),
            "Error should contain batch context. Got: {}",
            error_msg
        );

        // Verify error message contains first record sample
        assert!(
            error_msg.contains("First record sample:"),
            "Error should show first record sample. Got: {}",
            error_msg
        );

        // Verify the error message shows the problematic value
        assert!(
            error_msg.contains("-100") || error_msg.contains("[1, -100]"),
            "Error should show the problematic value. Got: {}",
            error_msg
        );

        // Verify parameter limit hint is NOT present (this is a CHECK constraint error, not parameter limit)
        assert!(
            !error_msg
                .contains("Hint: This error is caused by exceeding the database parameter limit"),
            "Parameter limit hint should not appear for CHECK constraint errors. Got: {}",
            error_msg
        );
    }

    #[tokio::test]
    async fn test_csv_config_all_parameters() {
        // Comprehensive test for all 4 CSV configuration parameters:
        // delimiter, quote, escape, and has_header
        let temp_dir = TempDir::new().unwrap();

        // Create a custom-formatted file:
        // - Pipe-delimited (|)
        // - Single quote as quote character (')
        // - Backslash as escape character (\)
        // - No header row
        let csv_path = create_csv_with_content(
            &temp_dir,
            "custom_format.txt",
            &[
                "1|'Alice\\'s Data'|100\n",
                "2|'Bob said: \\'Hi\\''|200\n",
                "3|'Charlie, Jr.'|300\n",
                "4|'Dave | likes | pipes | '|400",
            ],
        )
        .await;

        let pool = setup_sqlite_table("test_all_params", "col1 TEXT, col2 TEXT, col3 TEXT").await;

        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path,
            target_table: "test_all_params".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 1000,
            batch_size: 10,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: Some("|".to_string()),
            quote: Some("'".to_string()),
            escape: Some("\\".to_string()),
            has_header: Some(false),
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };

        let result = run_load(args).await.unwrap();

        assert_eq!(result.records_loaded, 4, "Should load all 4 records");
        assert_eq!(result.records_failed, 0, "Should have no failures");
        assert_eq!(get_table_count(&pool, "test_all_params").await, 4);

        // Verify data was parsed correctly with all custom parameters
        let mut conn = pool.acquire().await.unwrap();
        if let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
            // Verify first record - escape character in quoted field
            let (col1, col2, col3): (String, String, String) =
                sqlx::query_as("SELECT col1, col2, col3 FROM test_all_params WHERE col1 = '1'")
                    .fetch_one(&mut **sqlite_conn)
                    .await
                    .unwrap();
            assert_eq!(col1, "1");
            assert_eq!(col2, "Alice's Data", "Escaped quotes should be unescaped");
            assert_eq!(col3, "100");

            // Verify second record - multiple escaped quotes
            let (col2_2,): (String,) =
                sqlx::query_as("SELECT col2 FROM test_all_params WHERE col1 = '2'")
                    .fetch_one(&mut **sqlite_conn)
                    .await
                    .unwrap();
            assert_eq!(
                col2_2, "Bob said: 'Hi'",
                "Multiple escaped quotes should be handled"
            );

            // Verify third record - comma inside quoted field (shouldn't be confused with delimiter)
            let (col2_3,): (String,) =
                sqlx::query_as("SELECT col2 FROM test_all_params WHERE col1 = '3'")
                    .fetch_one(&mut **sqlite_conn)
                    .await
                    .unwrap();
            assert_eq!(
                col2_3, "Charlie, Jr.",
                "Comma in quoted field should be preserved"
            );

            // Verify fourth record - delimiter (pipe) inside quoted field
            let (col2_4,): (String,) =
                sqlx::query_as("SELECT col2 FROM test_all_params WHERE col1 = '4'")
                    .fetch_one(&mut **sqlite_conn)
                    .await
                    .unwrap();
            assert_eq!(
                col2_4, "Dave | likes | pipes | ",
                "Delimiter (pipe) inside quoted field should be preserved and not treated as field separator"
            );
        }
    }

    #[tokio::test]
    async fn test_parse_errors_detected_for_inconsistent_columns() {
        // Verify that records with mismatched column counts are reported as parse errors
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "bad_columns.csv",
            &[
                "id,name,value\n",
                "1,alice,100\n",
                "2,bob\n", // missing column
                "3,charlie,300\n",
                "4,diana,400,extra\n", // extra column
                "5,eve,500\n",
            ],
        )
        .await;

        let pool = setup_sqlite_table("test_parse_errors", "col1 TEXT, col2 TEXT, col3 TEXT").await;

        let result = run_csv_load_with_opts(
            &pool,
            "test_parse_errors",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(
            result.records_loaded, 3,
            "Should load only the 3 records with correct column count"
        );
        assert_eq!(
            result.records_failed, 2,
            "Should report 2 failed records (one missing column, one extra)"
        );
        assert_eq!(get_table_count(&pool, "test_parse_errors").await, 3);
    }

    #[tokio::test]
    async fn test_parse_errors_detected_for_csv_errors() {
        // Verify that unclosed quotes result in fewer records loaded
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "malformed.csv",
            &[
                "id,name,value\n",
                "1,alice,100\n",
                "2,\"unclosed quote,200\n",
                "3,charlie,300\n",
                "4,diana,400\n",
            ],
        )
        .await;

        let pool =
            setup_sqlite_table("test_csv_parse_errors", "col1 TEXT, col2 TEXT, col3 TEXT").await;

        let result = run_csv_load_with_opts(
            &pool,
            "test_csv_parse_errors",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert!(
            result.records_loaded < 4,
            "Should load fewer records than the 4 data rows due to unclosed quote. Got: {}",
            result.records_loaded
        );
    }

    // ============ RFC 4180 Compliance Tests ============

    /// Helper to run a CSV load through the runner API with optional delimited config overrides
    async fn run_csv_load_with_opts(
        pool: &Pool,
        table_name: &str,
        csv_path: &str,
        delimiter: Option<String>,
        quote: Option<String>,
        escape: Option<String>,
        has_header: Option<bool>,
    ) -> crate::runner::LoadResult {
        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path.to_string(),
            target_table: table_name.to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 10_000_000,
            batch_size: 100,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter,
            quote,
            escape,
            has_header,
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };
        run_load(args).await.unwrap()
    }

    /// Helper to create a CSV file with NO header row (just data).
    async fn create_headerless_csv(dir: &TempDir, filename: &str, num_rows: usize) -> String {
        let path = dir.path().join(filename);
        let mut file = File::create(&path).await.unwrap();
        for i in 0..num_rows {
            let line = format!("{},name_{},{}.5,{}\n", i, i, i, i * 10);
            file.write_all(line.as_bytes()).await.unwrap();
        }
        file.flush().await.unwrap();
        path.to_str().unwrap().to_string()
    }

    async fn create_headerless_tsv(dir: &TempDir, filename: &str, num_rows: usize) -> String {
        let path = dir.path().join(filename);
        let mut file = File::create(&path).await.unwrap();
        for i in 0..num_rows {
            let line = format!("{}\tname_{}\t{}.5\t{}\n", i, i, i, i * 10);
            file.write_all(line.as_bytes()).await.unwrap();
        }
        file.flush().await.unwrap();
        path.to_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_headerless_csv_loads_all_rows_by_default() {
        // Reproduces issue #28: a CSV with no header row must not silently
        // drop the first data row when the user passes no header-related flags.
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_headerless_csv(&temp_dir, "headerless.csv", 1000).await;

        let pool = setup_sqlite_table(
            "headerless",
            "id INTEGER, name TEXT, value REAL, amount INTEGER",
        )
        .await;

        let result = run_csv_load_with_opts(
            &pool,
            "headerless",
            &csv_path,
            None,
            None,
            None,
            /* has_header */ None,
        )
        .await;

        assert_eq!(
            result.records_loaded, 1000,
            "All 1000 data rows must load when CSV has no header (issue #28)"
        );
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_csv_with_header_loads_all_rows_when_header_flag_set() {
        // Verifies the new --header CLI flag (which sets has_header: Some(true))
        // correctly skips the header row and loads every data row.
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "with_header.csv", 1000).await;

        let pool = setup_sqlite_table(
            "with_header",
            "id INTEGER, name TEXT, value REAL, amount INTEGER",
        )
        .await;

        let result = run_csv_load_with_opts(
            &pool,
            "with_header",
            &csv_path,
            None,
            None,
            None,
            /* has_header */ Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 1000);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_headerless_tsv_loads_all_rows_by_default() {
        // Symmetric coverage to test_headerless_csv_loads_all_rows_by_default:
        // a TSV with no header row must not silently drop the first data row
        // when the user passes no header-related flags.
        let temp_dir = TempDir::new().unwrap();
        let tsv_path = create_headerless_tsv(&temp_dir, "headerless.tsv", 1000).await;

        let pool = setup_sqlite_table(
            "headerless_tsv",
            "id INTEGER, name TEXT, value REAL, amount INTEGER",
        )
        .await;

        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: tsv_path,
            target_table: "headerless_tsv".to_string(),
            schema: "public".to_string(),
            format: Format::Tsv,
            worker_count: 1,
            chunk_size_bytes: 10_000_000,
            batch_size: 100,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: None,
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };
        let result = run_load(args).await.unwrap();

        assert_eq!(
            result.records_loaded, 1000,
            "All 1000 data rows must load when TSV has no header (issue #28 symmetric case)"
        );
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_tsv_with_header_loads_all_rows_when_header_flag_set() {
        // Symmetric to test_csv_with_header_loads_all_rows_when_header_flag_set:
        // a header-bearing TSV with `has_header: Some(true)` must skip the
        // header and load every data row.
        let temp_dir = TempDir::new().unwrap();
        let tsv_path = create_test_tsv(&temp_dir, "with_header.tsv", 1000).await;

        let pool = setup_sqlite_table(
            "tsv_with_header",
            "id INTEGER, name TEXT, value REAL, amount INTEGER",
        )
        .await;

        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: tsv_path,
            target_table: "tsv_with_header".to_string(),
            schema: "public".to_string(),
            format: Format::Tsv,
            worker_count: 1,
            chunk_size_bytes: 10_000_000,
            batch_size: 100,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool.clone()),
        };
        let result = run_load(args).await.unwrap();

        assert_eq!(result.records_loaded, 1000);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_header_csv_without_header_flag_eats_header_as_data() {
        // Symmetric mirror of issue #28: a header-bearing CSV loaded WITHOUT
        // `--header` under the new 3.0.0 default (`has_header: None` → false)
        // treats the header line as a data row. With a TEXT schema the row
        // gets silently inserted (records_loaded = data_rows + 1), which is
        // the failure mode the post-load `--header` advisory exists to flag.
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "header_no_flag.csv", 1000).await;

        // TEXT-only schema so the header line ("id,name,value,amount") parses
        // as a valid row instead of failing on type coercion.
        let pool = setup_sqlite_table(
            "header_no_flag",
            "id TEXT, name TEXT, value TEXT, amount TEXT",
        )
        .await;

        let result = run_csv_load_with_opts(
            &pool,
            "header_no_flag",
            &csv_path,
            None,
            None,
            None,
            /* has_header */ None,
        )
        .await;

        assert_eq!(
            result.records_loaded, 1001,
            "header line should be loaded as a 1001st data row when --header is not set"
        );
        assert_eq!(result.records_failed, 0);
    }

    /// Library-API validation: `has_header: Some(_)` is meaningless with Parquet
    /// and must be rejected by `run_load` before any I/O. Mirrors the CLI's
    /// `validate_delimited_options` so library consumers don't get silent drops.
    #[tokio::test]
    async fn test_run_load_rejects_has_header_with_parquet() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: "/dev/null".to_string(),
            target_table: "t".to_string(),
            schema: "public".to_string(),
            format: Format::Parquet,
            worker_count: 1,
            chunk_size_bytes: 10_000_000,
            batch_size: 100,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            verify: VerifyMode::Off,
            test_pool: Some(pool),
        };
        let err = run_load(args).await.expect_err("must reject");
        let msg = format!("{}", err);
        assert!(
            msg.contains("has_header") && msg.contains("Parquet"),
            "error message must name the offending option and format: {msg}"
        );
    }

    /// Regression: worker `current_line` must start at 1 for headerless CSVs.
    ///
    /// Pre-fix, `has_header` was derived from the format variant rather than the
    /// manifest's `DelimitedConfig.has_header`, so headerless CSV/TSV loads
    /// reported error line numbers off by one.
    #[tokio::test]
    async fn test_headerless_csv_error_line_numbers_are_one_based() {
        let temp_dir = TempDir::new().unwrap();
        // Headerless CSV with a duplicate id; first row is line 1.
        let csv_path = create_csv_with_content(
            &temp_dir,
            "dup_headerless.csv",
            &["1,Alice\n", "1,Alice_Duplicate\n"],
        )
        .await;

        let pool =
            setup_sqlite_table("test_dup_headerless", "id TEXT PRIMARY KEY, name TEXT").await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::csv(),
        ));
        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: false },
            pool.clone(),
        );

        // Use OnConflict::Error so the duplicate triggers a batch_error
        // (which reports `line_number = line_offset` derived from current_line).
        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_dup_headerless".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::Error)
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();
        assert_eq!(result.records_failed, 2, "Both rows fail as a batch");

        let batch_errors: Vec<_> = result
            .chunk_results
            .iter()
            .flat_map(|r| r.errors.iter())
            .filter(|e| e.error_type == "batch_error")
            .collect();
        assert_eq!(batch_errors.len(), 1);
        assert_eq!(
            batch_errors[0].line_number, 1,
            "Headerless CSV must report line_offset starting at 1, not 2"
        );
    }

    #[tokio::test]
    async fn test_rfc4180_crlf_line_endings() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "crlf.csv",
            &["id,name\r\n", "1,alice\r\n", "2,bob\r\n"],
        )
        .await;

        let pool = setup_sqlite_table("test_crlf", "col1 TEXT, col2 TEXT").await;
        let result =
            run_csv_load_with_opts(&pool, "test_crlf", &csv_path, None, None, None, Some(true))
                .await;

        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_rfc4180_no_trailing_newline() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "no_trailing.csv",
            &["id,name\n", "1,alice\n", "2,bob"],
        )
        .await;

        let pool = setup_sqlite_table("test_no_trailing", "col1 TEXT, col2 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_no_trailing",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_rfc4180_spaces_preserved() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "spaces.csv",
            &["id,name\n", "1, alice \n", "2, bob \n"],
        )
        .await;

        let pool = setup_sqlite_table("test_spaces", "col1 TEXT, col2 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_spaces",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_rfc4180_quoted_fields() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "quoted.csv",
            &["id,name\n", "1,\"alice\"\n", "2,\"bob\"\n"],
        )
        .await;

        let pool = setup_sqlite_table("test_quoted", "col1 TEXT, col2 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_quoted",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_rfc4180_embedded_newline_in_quoted_field() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "embedded_newline.csv",
            &["id,name\n", "1,\"line1\nline2\"\n", "2,bob\n"],
        )
        .await;

        let pool = setup_sqlite_table("test_embedded_nl", "col1 TEXT, col2 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_embedded_nl",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_rfc4180_embedded_comma_in_quoted_field() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "embedded_comma.csv",
            &["id,name\n", "1,\"last, first\"\n", "2,bob\n"],
        )
        .await;

        let pool = setup_sqlite_table("test_embedded_comma", "col1 TEXT, col2 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_embedded_comma",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_rfc4180_doubled_quote_escape() {
        // RFC 4180 rule 7: double-quote inside quoted field escaped by doubling
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "doubled_quote.csv",
            &["id,name\n", "1,\"she said \"\"hello\"\"\"\n", "2,bob\n"],
        )
        .await;

        let pool = setup_sqlite_table("test_doubled_quote", "col1 TEXT, col2 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_doubled_quote",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);
    }

    // ============ Real-World Edge Case Tests ============

    #[tokio::test]
    async fn test_backslash_escape_with_flag() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "backslash.csv",
            &[
                "id,keyword\n",
                "1,\"normal\"\n",
                "2,\"marc-\\\"pete\\\"-mitscher\"\n",
                "3,\"clean\"\n",
            ],
        )
        .await;

        let pool = setup_sqlite_table("test_backslash", "col1 TEXT, col2 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_backslash",
            &csv_path,
            None,
            None,
            Some("\\".to_string()),
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 3);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_empty_fields() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "empty_fields.csv",
            &["id,name,value\n", "1,,100\n", "2,bob,\n", "3,,\n"],
        )
        .await;

        let pool = setup_sqlite_table("test_empty_fields", "col1 TEXT, col2 TEXT, col3 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_empty_fields",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 3);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_no_header_mode() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "no_header.csv",
            &["1,alice,100\n", "2,bob,200\n"],
        )
        .await;

        let pool = setup_sqlite_table("test_no_header", "col1 TEXT, col2 TEXT, col3 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_no_header",
            &csv_path,
            None,
            None,
            None,
            Some(false),
        )
        .await;

        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_unicode_content() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "unicode.csv",
            &[
                "id,name\n",
                "1,\"café résumé\"\n",
                "2,\"日本語\"\n",
                "3,\"emoji 🎉\"\n",
            ],
        )
        .await;

        let pool = setup_sqlite_table("test_unicode", "col1 TEXT, col2 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_unicode",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 3);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_mixed_line_endings() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "mixed_endings.csv",
            &["id,name\n", "1,alice\r\n", "2,bob\n", "3,charlie\r\n"],
        )
        .await;

        let pool = setup_sqlite_table("test_mixed_endings", "col1 TEXT, col2 TEXT").await;
        let result = run_csv_load_with_opts(
            &pool,
            "test_mixed_endings",
            &csv_path,
            None,
            None,
            None,
            Some(true),
        )
        .await;

        assert_eq!(result.records_loaded, 3);
        assert_eq!(result.records_failed, 0);
    }

    // ============ --exclude-columns integration tests ============

    /// End-to-end: CSV has all columns including the excluded PK. Loader skips
    /// the excluded position and lets the DB apply DEFAULT. Mirrors the customer
    /// scenario (UUID PK with gen_random_uuid() default).
    #[tokio::test]
    async fn test_exclude_columns_skip_mode_end_to_end() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "with_pk.csv",
            &[
                "pk_id,name,email\n",
                "1,Alice,alice@ex.com\n",
                "2,Bob,bob@ex.com\n",
                "3,Charlie,charlie@ex.com\n",
            ],
        )
        .await;

        // pk_id gets a server-generated default; customer's table uses
        // `DEFAULT (lower(hex(randomblob(16))))` as a SQLite-compatible analog
        // to `DEFAULT gen_random_uuid()`.
        let pool = setup_sqlite_table(
            "test_exclude",
            "pk_id TEXT DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY, name TEXT, email TEXT",
        )
        .await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_exclude".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();
        assert_eq!(result.records_loaded, 3);
        assert_eq!(result.records_failed, 0);
        assert_eq!(get_table_count(&pool, "test_exclude").await, 3);

        // Verify: the CSV's pk_id values were NOT inserted; DB-generated defaults
        // were used. The CSV had pk_id = 1/2/3; those should not appear.
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let (count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM test_exclude WHERE pk_id IN ('1','2','3')")
                    .fetch_one(&mut **sqlite_conn)
                    .await
                    .unwrap();
            assert_eq!(
                count, 0,
                "pk_id values from CSV should NOT appear; DB should have generated defaults"
            );
        }
    }

    /// Parallel workers + multi-chunk load with `--exclude-columns`: the same
    /// invariant as `test_multiple_workers_and_chunk_distribution` but with a
    /// DB-defaulted PK column being skipped. Exercises the concurrent-chunk
    /// path that production actually uses.
    #[tokio::test]
    async fn test_exclude_columns_parallel_workers() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = temp_dir.path().join("parallel.csv");
        let mut file = File::create(&csv_path).await.unwrap();
        file.write_all(b"pk_id,name,email\n").await.unwrap();
        for i in 0..500 {
            let line = format!("{},name_{},user_{}@ex.com\n", i, i, i);
            file.write_all(line.as_bytes()).await.unwrap();
        }
        file.flush().await.unwrap();
        let csv_path = csv_path.to_str().unwrap().to_string();

        let pool = setup_sqlite_table(
            "test_exclude_parallel",
            "pk_id TEXT DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY, name TEXT, email TEXT",
        )
        .await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_exclude_parallel".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(4)
            .chunk_size_bytes(800)
            .batch_size(50)
            .batch_concurrency(2)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();

        assert!(result.chunks_processed > 1, "expected multiple chunks");
        assert_eq!(result.records_loaded, 500);
        assert_eq!(result.records_failed, 0);
        assert_eq!(get_table_count(&pool, "test_exclude_parallel").await, 500);

        let unique_workers: HashSet<_> =
            result.chunk_results.iter().map(|r| &r.worker_id).collect();
        assert!(
            unique_workers.len() >= 2,
            "expected at least 2 workers, got {}",
            unique_workers.len()
        );

        // CSV pk_ids were 0..500; none should appear because the DB default generated them.
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let (count,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM test_exclude_parallel WHERE pk_id IN ('0','1','2','3','499')",
            )
            .fetch_one(&mut **sqlite_conn)
            .await
            .unwrap();
            assert_eq!(
                count, 0,
                "CSV pk_id values must not appear; DB-generated defaults should be used"
            );
        }
    }

    /// Parquet source + `--exclude-columns`: parquet emits records in the file's
    /// column order, so this confirms the positional skipping logic works when the
    /// reader is a Parquet reader (not just CSV).
    #[tokio::test]
    async fn test_exclude_columns_parquet() {
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("exclude.parquet");
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("pk_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]);

        let file = std::fs::File::create(&parquet_path).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(50)
            .build();
        let mut writer =
            ArrowWriter::try_new(file, Arc::new(arrow_schema.clone()), Some(props)).unwrap();

        let num_rows: i32 = 100;
        let pk_array = StringArray::from_iter((0..num_rows).map(|i| Some(format!("csv_pk_{}", i))));
        let name_array = StringArray::from_iter((0..num_rows).map(|i| Some(format!("name_{}", i))));
        let value_array = Float64Array::from_iter_values((0..num_rows).map(|i| (i as f64) * 1.5));

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(pk_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let parquet_path_str = parquet_path.to_str().unwrap().to_string();

        let pool = setup_sqlite_table(
            "test_exclude_parquet",
            "pk_id TEXT DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY, name TEXT, value REAL",
        )
        .await;

        let byte_reader = LocalFileByteReader::new(&parquet_path_str);
        let parquet_reader = GenericParquetReader::new(byte_reader).await.unwrap();
        let file_reader: Arc<dyn FileReader> = Arc::new(parquet_reader);

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(parquet_path_str)
            .target_table("test_exclude_parquet".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(2)
            .chunk_size_bytes(500)
            .batch_size(20)
            .batch_concurrency(2)
            .create_table_if_missing(false)
            .file_format(FileFormat::Parquet(ParquetConfig::default()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();

        assert_eq!(result.records_loaded, 100);
        assert_eq!(result.records_failed, 0);
        assert_eq!(get_table_count(&pool, "test_exclude_parquet").await, 100);

        // None of the parquet-sourced pk_id values should land in the DB.
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn
        {
            let (count,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM test_exclude_parquet WHERE pk_id LIKE 'csv_pk_%'",
            )
            .fetch_one(&mut **sqlite_conn)
            .await
            .unwrap();
            assert_eq!(
                count, 0,
                "Parquet pk_id values must not appear; DB default should have replaced them"
            );
        }
    }

    /// Field-count mismatch: the CSV is missing a column. Records should be
    /// dropped and counted as failures with one aggregated ErrorRecord.
    #[tokio::test]
    async fn test_exclude_columns_field_count_mismatch_is_aggregated() {
        let temp_dir = TempDir::new().unwrap();
        // CSV is missing pk_id — should fail because excluded still requires full schema
        let csv_path = create_csv_with_content(
            &temp_dir,
            "missing_pk.csv",
            &["name,email\n", "Alice,alice@ex.com\n", "Bob,bob@ex.com\n"],
        )
        .await;

        let pool = setup_sqlite_table(
            "test_exclude_mismatch",
            "pk_id TEXT DEFAULT 'x' PRIMARY KEY, name TEXT, email TEXT",
        )
        .await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_exclude_mismatch".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();
        assert_eq!(result.records_loaded, 0);
        assert_eq!(result.records_failed, 2);
        assert_eq!(get_table_count(&pool, "test_exclude_mismatch").await, 0);

        // Error aggregation: exactly one ErrorRecord of type field_count_mismatch with count 2
        let field_count_errors: Vec<_> = result
            .chunk_results
            .iter()
            .flat_map(|r| r.errors.iter())
            .filter(|e| e.error_type == "field_count_mismatch")
            .collect();
        assert_eq!(field_count_errors.len(), 1);
        let msg = &field_count_errors[0].error_message;
        assert!(msg.contains("2 record(s)"), "msg: {}", msg);
        assert!(
            msg.contains("first mismatch at chunk record index") && msg.contains("had 2 fields"),
            "msg: {}",
            msg
        );
        assert_eq!(field_count_errors[0].line_number, 0);
    }

    /// Invalid column name: validator rejects before loading starts.
    #[tokio::test]
    async fn test_exclude_columns_unknown_name_errors() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path =
            create_csv_with_content(&temp_dir, "ok.csv", &["id,name\n", "1,Alice\n"]).await;

        let pool = setup_sqlite_table("test_unknown", "id TEXT PRIMARY KEY, name TEXT").await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_unknown".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["does_not_exist".to_string()])
            .build()
            .unwrap();

        let err = coordinator.run_load(&config).await.unwrap_err().to_string();
        assert!(
            err.contains("Column 'does_not_exist' not found"),
            "error should identify the unknown column, got: {}",
            err
        );
    }

    /// Combination: --exclude-columns + --column-map on disjoint columns should work.
    /// Exercises both features together: pk_id is excluded (DB applies DEFAULT), and
    /// the SQLite column `full_name` is renamed to itself to cover the rename code path.
    /// A non-trivial rename is not possible here without also mutating the SQLite schema,
    /// so this asserts only that the combined pipeline loads both rows successfully.
    /// Rename-after-exclude ordering is pinned by the unit test
    /// `exclude_then_rename_ordering_applies_rename_to_remaining_columns` in coordinator.rs.
    #[tokio::test]
    async fn test_exclude_columns_plus_column_map() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "combo.csv",
            &[
                "pk_id,full_name,email\n",
                "1,Alice,alice@ex.com\n",
                "2,Bob,bob@ex.com\n",
            ],
        )
        .await;

        let pool = setup_sqlite_table(
            "test_combo",
            "pk_id TEXT DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY, full_name TEXT, email TEXT",
        )
        .await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let mut mappings = HashMap::new();
        mappings.insert("full_name".to_string(), "full_name".to_string());

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_combo".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(mappings)
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();
        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);
        assert_eq!(get_table_count(&pool, "test_combo").await, 2);
    }

    /// Conflict: --column-map targets an excluded column — validator must reject.
    #[tokio::test]
    async fn test_exclude_columns_rename_conflict_errors() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path =
            create_csv_with_content(&temp_dir, "conflict.csv", &["pk_id,name\n", "1,Alice\n"])
                .await;

        let pool = setup_sqlite_table(
            "test_rename_conflict",
            "pk_id TEXT DEFAULT 'x' PRIMARY KEY, name TEXT",
        )
        .await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let mut mappings = HashMap::new();
        mappings.insert("pk_id".to_string(), "new_pk".to_string());

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_rename_conflict".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(mappings)
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let err = coordinator.run_load(&config).await.unwrap_err().to_string();
        assert!(
            err.contains("cannot be both excluded and renamed"),
            "error should call out exclusion/rename conflict, got: {}",
            err
        );
    }

    /// --if-not-exists + --exclude-columns must be rejected at setup.
    #[tokio::test]
    async fn test_exclude_columns_with_if_not_exists_errors() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path =
            create_csv_with_content(&temp_dir, "create.csv", &["id,name\n", "1,Alice\n"]).await;

        let pool = Pool::sqlite_in_memory().await.unwrap();

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("never_created".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(true)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["id".to_string()])
            .build()
            .unwrap();

        let err = coordinator.run_load(&config).await.unwrap_err().to_string();
        assert!(
            err.contains("not supported with --if-not-exists"),
            "error should reject the combination, got: {}",
            err
        );
    }

    /// DO UPDATE with all conflict columns excluded must be rejected.
    #[tokio::test]
    async fn test_exclude_columns_all_conflict_cols_excluded_do_update_errors() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path =
            create_csv_with_content(&temp_dir, "do_update.csv", &["pk_id,name\n", "1,Alice\n"])
                .await;

        // pk_id is the only unique column; excluding it should fail under do-update
        let pool = setup_sqlite_table(
            "test_all_excluded_upsert",
            "pk_id TEXT PRIMARY KEY, name TEXT",
        )
        .await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let coordinator = Coordinator::new(
            manifest_storage,
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_all_excluded_upsert".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoUpdate)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let err = coordinator.run_load(&config).await.unwrap_err().to_string();
        assert!(
            err.contains("All conflict columns are excluded"),
            "error should call out the broken conflict target, got: {}",
            err
        );
    }

    /// Resume with matching --exclude-columns must succeed; mismatched must fail.
    #[tokio::test]
    async fn test_exclude_columns_resume_compatibility_check() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "resume.csv",
            &["pk_id,name\n", "1,Alice\n", "2,Bob\n", "3,Charlie\n"],
        )
        .await;

        let pool = setup_sqlite_table(
            "test_resume_excl",
            "pk_id TEXT DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY, name TEXT",
        )
        .await;

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let coordinator = Coordinator::new(
            manifest_storage.clone(),
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path.clone())
            .target_table("test_resume_excl".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();
        assert_eq!(result.records_loaded, 3);
        let job_id = result.job_id;

        // Resume with a DIFFERENT exclude list must fail
        let byte_reader2 = LocalFileByteReader::new(&csv_path);
        let file_reader2: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader2, csv_with_header()));
        let coordinator2 = Coordinator::new(
            manifest_storage.clone(),
            file_reader2,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );
        let bad_config = LoadConfigBuilder::default()
            .source_uri(csv_path.clone())
            .target_table("test_resume_excl".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["name".to_string()])
            .resume_job_id(Some(job_id.clone()))
            .build()
            .unwrap();

        let err = coordinator2
            .run_load(&bad_config)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("--exclude-columns mismatch"),
            "expected resume mismatch error, got: {}",
            err
        );
    }

    /// Resume without --exclude-columns must inherit the manifest's exclusion set.
    /// Uses an order-reversed explicit list to also verify the sort-before-compare path.
    #[tokio::test]
    async fn test_exclude_columns_resume_inherits_from_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_csv_with_content(
            &temp_dir,
            "inherit.csv",
            &["pk_id,extra,name\n", "1,x,Alice\n", "2,y,Bob\n"],
        )
        .await;

        let pool = setup_sqlite_table(
            "test_resume_inherit",
            "pk_id TEXT DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY, \
             extra TEXT DEFAULT 'def', name TEXT",
        )
        .await;

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let coordinator = Coordinator::new(
            manifest_storage.clone(),
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path.clone())
            .target_table("test_resume_inherit".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string(), "extra".to_string()])
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();
        assert_eq!(result.records_loaded, 2);
        let job_id = result.job_id;

        // Resume with exclude_columns omitted entirely should inherit from manifest.
        let byte_reader2 = LocalFileByteReader::new(&csv_path);
        let file_reader2: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader2, csv_with_header()));
        let coordinator2 = Coordinator::new(
            manifest_storage.clone(),
            file_reader2,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );
        let resume_omitted = LoadConfigBuilder::default()
            .source_uri(csv_path.clone())
            .target_table("test_resume_inherit".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .resume_job_id(Some(job_id.clone()))
            .build()
            .unwrap();

        coordinator2
            .run_load(&resume_omitted)
            .await
            .expect("resume without exclude_columns must succeed using manifest value");
        assert_eq!(
            get_table_count(&pool, "test_resume_inherit").await,
            2,
            "resume must not re-insert rows"
        );

        // Resume with the same exclude list in reversed order must also succeed
        // (sort-before-compare makes the check order-independent).
        let byte_reader3 = LocalFileByteReader::new(&csv_path);
        let file_reader3: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader3, csv_with_header()));
        let coordinator3 = Coordinator::new(
            manifest_storage,
            file_reader3,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );
        let resume_reordered = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_resume_inherit".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["extra".to_string(), "pk_id".to_string()])
            .resume_job_id(Some(job_id))
            .build()
            .unwrap();

        coordinator3
            .run_load(&resume_reordered)
            .await
            .expect("reordered exclude_columns must be accepted on resume");
        assert_eq!(
            get_table_count(&pool, "test_resume_inherit").await,
            2,
            "reordered-resume must not re-insert rows"
        );
    }

    /// Manifest integrity: resuming with a corrupted manifest where
    /// excluded_columns and excluded_positions disagree in length must be rejected
    /// before any worker starts.
    #[tokio::test]
    async fn test_exclude_columns_resume_rejects_corrupted_manifest_parity() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path =
            create_csv_with_content(&temp_dir, "corrupt.csv", &["pk_id,name\n", "1,Alice\n"]).await;
        let pool = setup_sqlite_table(
            "test_parity",
            "pk_id TEXT DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY, name TEXT",
        )
        .await;

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let coordinator = Coordinator::new(
            manifest_storage.clone(),
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path.clone())
            .target_table("test_parity".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();
        let job_id = result.job_id;

        // Corrupt the manifest: keep excluded_columns but drop excluded_positions.
        let manifest_path = manifest_dir
            .path()
            .join("jobs")
            .join(&job_id)
            .join("manifest.json");
        let raw = std::fs::read_to_string(&manifest_path).unwrap();
        let mut mf: serde_json::Value = serde_json::from_str(&raw).unwrap();
        mf["table"]["excluded_positions"] = serde_json::json!([]);
        std::fs::write(&manifest_path, serde_json::to_string(&mf).unwrap()).unwrap();

        let byte_reader2 = LocalFileByteReader::new(&csv_path);
        let file_reader2: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader2, csv_with_header()));
        let coordinator2 = Coordinator::new(
            manifest_storage,
            file_reader2,
            SchemaInferrer { has_header: true },
            pool,
        );
        let resume_config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_parity".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .resume_job_id(Some(job_id))
            .build()
            .unwrap();

        let err = coordinator2
            .run_load(&resume_config)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("manifest exclusion fields disagree"),
            "expected parity error, got: {}",
            err
        );
    }

    /// Manifest integrity: an out-of-range `excluded_positions` entry must be
    /// rejected on resume even when the parity check passes.
    #[tokio::test]
    async fn test_exclude_columns_resume_rejects_out_of_range_position() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path =
            create_csv_with_content(&temp_dir, "oor.csv", &["pk_id,name\n", "1,Alice\n"]).await;
        let pool = setup_sqlite_table(
            "test_oor",
            "pk_id TEXT DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY, name TEXT",
        )
        .await;

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn ManifestStorage> =
            Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
        let coordinator = Coordinator::new(
            manifest_storage.clone(),
            file_reader,
            SchemaInferrer { has_header: true },
            pool.clone(),
        );

        let config = LoadConfigBuilder::default()
            .source_uri(csv_path.clone())
            .target_table("test_oor".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .exclude_columns(vec!["pk_id".to_string()])
            .build()
            .unwrap();

        let job_id = coordinator.run_load(&config).await.unwrap().job_id;

        // Parity intact (len == 1), but position 99 is out of range.
        let manifest_path = manifest_dir
            .path()
            .join("jobs")
            .join(&job_id)
            .join("manifest.json");
        let raw = std::fs::read_to_string(&manifest_path).unwrap();
        let mut mf: serde_json::Value = serde_json::from_str(&raw).unwrap();
        mf["table"]["excluded_positions"] = serde_json::json!([99]);
        std::fs::write(&manifest_path, serde_json::to_string(&mf).unwrap()).unwrap();

        let byte_reader2 = LocalFileByteReader::new(&csv_path);
        let file_reader2: Arc<dyn FileReader> =
            Arc::new(GenericDelimitedReader::new(byte_reader2, csv_with_header()));
        let coordinator2 = Coordinator::new(
            manifest_storage,
            file_reader2,
            SchemaInferrer { has_header: true },
            pool,
        );
        let resume_config = LoadConfigBuilder::default()
            .source_uri(csv_path)
            .target_table("test_oor".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            })
            .worker_count(1)
            .chunk_size_bytes(1000)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::Csv(csv_with_header()))
            .column_mappings(HashMap::new())
            .quiet(true)
            .on_conflict(OnConflict::DoNothing)
            .resume_job_id(Some(job_id))
            .build()
            .unwrap();

        let err = coordinator2
            .run_load(&resume_config)
            .await
            .expect_err("resume must fail when excluded_positions has an out-of-range entry")
            .to_string();
        // Assert on the offending position value (the one stable diagnostic) rather
        // than exact phrasing, which clap/anyhow may re-format.
        assert!(
            err.contains("99"),
            "expected error to name the offending position, got: {}",
            err
        );
    }

    /// Manifest integrity: duplicate / non-strictly-increasing `excluded_positions`
    /// entries must be rejected on resume. These cannot arise from
    /// `compute_excluded_positions`, but a hand-edited manifest can produce them
    /// and would otherwise cause silent field-mapping misalignment.
    #[tokio::test]
    async fn test_exclude_columns_resume_rejects_non_strictly_increasing_positions() {
        async fn run_corruption_case(
            table_suffix: &str,
            corrupted_positions: serde_json::Value,
        ) -> String {
            let temp_dir = TempDir::new().unwrap();
            let csv_path = create_csv_with_content(
                &temp_dir,
                "nsi.csv",
                &["pk_id,extra,name\n", "1,x,Alice\n"],
            )
            .await;
            let table = format!("test_nsi_{}", table_suffix);
            let pool = setup_sqlite_table(
                &table,
                "pk_id TEXT DEFAULT (lower(hex(randomblob(16)))) PRIMARY KEY, \
                 extra TEXT DEFAULT 'def', name TEXT",
            )
            .await;

            let manifest_dir = TempDir::new().unwrap();
            let manifest_storage: Arc<dyn ManifestStorage> =
                Arc::new(LocalManifestStorage::new(manifest_dir.path().to_path_buf()));
            let byte_reader = LocalFileByteReader::new(&csv_path);
            let file_reader: Arc<dyn FileReader> =
                Arc::new(GenericDelimitedReader::new(byte_reader, csv_with_header()));
            let coordinator = Coordinator::new(
                manifest_storage.clone(),
                file_reader,
                SchemaInferrer { has_header: true },
                pool.clone(),
            );

            // Common builder: the two call sites below differ only in whether they
            // set exclude_columns (initial) or resume_job_id (resume).
            let base_builder = |csv: String, table: String| {
                LoadConfigBuilder::default()
                    .source_uri(csv)
                    .target_table(table)
                    .schema("public".to_string())
                    .dsql_config(DsqlConfig {
                        endpoint: "test".to_string(),
                        region: "us-west-2".to_string(),
                        username: "test".to_string(),
                    })
                    .worker_count(1)
                    .chunk_size_bytes(1000)
                    .batch_size(10)
                    .batch_concurrency(1)
                    .create_table_if_missing(false)
                    .file_format(FileFormat::Csv(csv_with_header()))
                    .column_mappings(HashMap::new())
                    .quiet(true)
                    .on_conflict(OnConflict::DoNothing)
                    .clone()
            };

            let config = base_builder(csv_path.clone(), table.clone())
                .exclude_columns(vec!["pk_id".to_string(), "extra".to_string()])
                .build()
                .unwrap();

            let job_id = coordinator
                .run_load(&config)
                .await
                .expect("initial load must succeed so we have a manifest to corrupt")
                .job_id;

            // Parity preserved (len == 2) but positions violate strictly-increasing.
            let manifest_path = manifest_dir
                .path()
                .join("jobs")
                .join(&job_id)
                .join("manifest.json");
            let raw = std::fs::read_to_string(&manifest_path).unwrap();
            let mut mf: serde_json::Value = serde_json::from_str(&raw).unwrap();
            mf["table"]["excluded_positions"] = corrupted_positions;
            std::fs::write(&manifest_path, serde_json::to_string(&mf).unwrap()).unwrap();

            let byte_reader2 = LocalFileByteReader::new(&csv_path);
            let file_reader2: Arc<dyn FileReader> =
                Arc::new(GenericDelimitedReader::new(byte_reader2, csv_with_header()));
            let coordinator2 = Coordinator::new(
                manifest_storage,
                file_reader2,
                SchemaInferrer { has_header: true },
                pool,
            );
            let resume_config = base_builder(csv_path, table)
                .resume_job_id(Some(job_id))
                .build()
                .unwrap();

            coordinator2
                .run_load(&resume_config)
                .await
                .expect_err("resume must fail on non-strictly-increasing excluded_positions")
                .to_string()
        }

        // Duplicate positions: [0, 0] — must be rejected (not strictly increasing).
        let err = run_corruption_case("dup", serde_json::json!([0, 0])).await;
        assert!(
            err.contains("not strictly increasing"),
            "expected not-strictly-increasing error for duplicates, got: {}",
            err
        );

        // Non-sorted positions: [1, 0] — must be rejected (not strictly increasing).
        let err = run_corruption_case("unsorted", serde_json::json!([1, 0])).await;
        assert!(
            err.contains("not strictly increasing"),
            "expected not-strictly-increasing error for unsorted, got: {}",
            err
        );
    }

    // ============ pg_dump integration tests ============

    fn pgdump_load_args(source_uri: String, target_table: &str, pool: Pool) -> LoadArgs {
        LoadArgs {
            endpoint: "ignored.dsql.us-east-1.on.aws".into(),
            region: "us-east-1".into(),
            username: "admin".into(),
            source_uri,
            target_table: target_table.into(),
            schema: "public".into(),
            format: Format::PgDump,
            worker_count: 1,
            chunk_size_bytes: 1024 * 1024,
            batch_size: 100,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: OnConflict::DoNothing,
            verify: VerifyMode::Count,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: None,
            test_pool: Some(pool),
        }
    }

    #[tokio::test]
    async fn pgdump_loads_via_run_load() -> anyhow::Result<()> {
        // pg_dump-shaped fixture: real tab bytes between fields, COPY header
        // followed by data lines, terminated by a literal `\.` line.
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "-- PostgreSQL database dump")?;
        writeln!(f, "SET statement_timeout = 0;")?;
        writeln!(f)?;
        writeln!(f, "COPY public.things (id, name, note) FROM stdin;")?;
        writeln!(f, "1\twidget\t\\N")?;
        writeln!(f, "2\tgizmo\thas\\ttab")?;
        writeln!(f, "3\tgadget\thello")?;
        writeln!(f, "\\.")?;
        writeln!(f)?;
        f.flush()?;

        let pool = setup_sqlite_table("things", "id INTEGER, name TEXT, note TEXT").await;
        let args = pgdump_load_args(
            f.path().to_string_lossy().into_owned(),
            "things",
            pool.clone(),
        );

        let result = run_load(args).await?;
        assert_eq!(result.records_loaded, 3);
        assert_eq!(result.records_failed, 0);

        // `note` for row 1 was `\N` → loaded as SQL NULL. Bind as
        // `Option<String>` so the sqlx decode path doesn't error on
        // UnexpectedNullError before assertions run.
        #[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
        struct ThingRow {
            id: i64,
            name: String,
            note: Option<String>,
        }

        let mut conn = pool.acquire().await?;
        let PoolConnection::Sqlite(ref mut sqlite_conn) = conn else {
            panic!("expected sqlite pool connection in test");
        };
        let rows: Vec<ThingRow> = sqlx::query_as("SELECT id, name, note FROM things ORDER BY id")
            .fetch_all(&mut **sqlite_conn)
            .await?;
        assert_eq!(
            rows[0],
            ThingRow {
                id: 1,
                name: "widget".into(),
                note: None
            }
        );
        assert_eq!(
            rows[1],
            ThingRow {
                id: 2,
                name: "gizmo".into(),
                note: Some("has\ttab".into())
            }
        );
        assert_eq!(
            rows[2],
            ThingRow {
                id: 3,
                name: "gadget".into(),
                note: Some("hello".into())
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn pgdump_errors_when_table_not_in_dump() -> anyhow::Result<()> {
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "COPY public.other (a) FROM stdin;")?;
        writeln!(f, "1")?;
        writeln!(f, "\\.")?;
        f.flush()?;

        let pool = setup_sqlite_table("missing", "a INTEGER").await;
        let args = pgdump_load_args(f.path().to_string_lossy().into_owned(), "missing", pool);

        let err = run_load(args).await.unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("public.missing") || msg.contains("no `COPY"),
            "expected error to name the missing block, got: {msg}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn pgdump_real_fixture_loads() -> anyhow::Result<()> {
        let pool =
            setup_sqlite_table("pg_loader_test_things", "id INTEGER, name TEXT, note TEXT").await;

        let args = pgdump_load_args(
            "tests/fixtures/pgdump_simple.sql".into(),
            "pg_loader_test_things",
            pool,
        );

        let result = run_load(args).await?;
        assert_eq!(result.records_loaded, 3);
        Ok(())
    }

    #[tokio::test]
    async fn pgdump_reorders_columns_when_target_order_differs() -> anyhow::Result<()> {
        // pg_dump emits (id, name, note); target table column order is shuffled.
        // The loader must reorder by name so values land in the right columns.
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "COPY public.things (id, name, note) FROM stdin;")?;
        writeln!(f, "1\twidget\thello")?;
        writeln!(f, "2\tgizmo\tworld")?;
        writeln!(f, "\\.")?;
        f.flush()?;

        let pool = setup_sqlite_table("things", "name TEXT, id INTEGER, note TEXT").await;
        let args = pgdump_load_args(
            f.path().to_string_lossy().into_owned(),
            "things",
            pool.clone(),
        );

        let result = run_load(args).await?;
        assert_eq!(result.records_loaded, 2);
        assert_eq!(result.records_failed, 0);

        // Verify the values landed in the named columns, not positionally.
        // Without reordering, `1` would have ended up in `name` and `widget`
        // in `id` (which would then fail or silently coerce on SQLite).
        #[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
        struct ThingRow {
            id: i64,
            name: String,
            note: Option<String>,
        }
        let mut conn = pool.acquire().await?;
        let PoolConnection::Sqlite(ref mut sqlite_conn) = conn else {
            panic!("expected sqlite pool connection in test");
        };
        let rows: Vec<ThingRow> = sqlx::query_as("SELECT id, name, note FROM things ORDER BY id")
            .fetch_all(&mut **sqlite_conn)
            .await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            ThingRow {
                id: 1,
                name: "widget".into(),
                note: Some("hello".into())
            }
        );
        assert_eq!(
            rows[1],
            ThingRow {
                id: 2,
                name: "gizmo".into(),
                note: Some("world".into())
            }
        );
        Ok(())
    }

    /// End-to-end fidelity test: PG → real `pg_dump` → loader → PG, verified
    /// with `EXCEPT` in both directions so any decode bug (BYTEA hex escapes,
    /// JSONB whitespace canonicalization, TIMESTAMPTZ precision, NULL vs '',
    /// UTF-8 multi-byte) shows up as a non-empty diff. Loading into the same
    /// Postgres instance under a different table name lets PG do the type-
    /// aware comparison — SQLite would silently coerce mismatches.
    ///
    /// Skipped (returns Ok with a printed message) when `PGDUMP_E2E_SOURCE_URL`
    /// is not set, so `cargo test` works locally without a Postgres available.
    /// CI sets it via the `postgres` service container.
    #[tokio::test]
    async fn pgdump_real_binary_round_trip_pg_to_pg() -> anyhow::Result<()> {
        let Some(source_pg_url) = std::env::var("PGDUMP_E2E_SOURCE_URL")
            .ok()
            .filter(|v| !v.is_empty())
        else {
            eprintln!(
                "skipping pgdump_real_binary_round_trip_pg_to_pg: \
                 PGDUMP_E2E_SOURCE_URL not set"
            );
            return Ok(());
        };

        let pg_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&source_pg_url)
            .await?;

        // Unique src/dst names so concurrent CI runs don't collide.
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let src = format!("pg_loader_src_{suffix}");
        let dst = format!("pg_loader_dst_{suffix}");

        // 1. SOURCE table with full type variety. Each row exercises a
        // distinct decode class.
        sqlx::query(&format!(
            "CREATE TABLE {src} (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                note TEXT,
                blob BYTEA,
                payload JSONB,
                ts TIMESTAMPTZ
            )"
        ))
        .execute(&pg_pool)
        .await?;

        // Pre-create DST with the same column SET but in DIFFERENT order than
        // pg_dump emits — exercises name-based reorder against real pg_dump
        // output. pg_dump emits columns in attnum (creation) order.
        sqlx::query(&format!(
            "CREATE TABLE {dst} (
                ts TIMESTAMPTZ,
                payload JSONB,
                blob BYTEA,
                note TEXT,
                name TEXT NOT NULL,
                id BIGINT PRIMARY KEY
            )"
        ))
        .execute(&pg_pool)
        .await?;

        // NOTE: empty TEXT (`note = ''`) and empty BYTEA (`'\x'`) are
        // intentionally omitted from this round-trip seed. The pg_dump
        // reader preserves the `\N` vs empty-string distinction (see
        // `PgDumpReader` rustdoc and `read_chunk_distinguishes_null_from_empty_string`),
        // but this test pre-dates that fix and seeds only NULL for `note`;
        // adding an empty-string row here would broaden the round-trip
        // assertion shape and is tracked separately.
        sqlx::query(&format!(
            "INSERT INTO {src} (id, name, note, blob, payload, ts) VALUES
                (1, 'plain',         E'tab\\there',  E'\\\\xDEADBEEF', '{{\"a\":1}}'::jsonb,  '2024-01-15 12:34:56+00'),
                (2, 'unicode-naïve', E'two\\nlines', E'\\\\x00FF',     '[1,2,3,null]'::jsonb, '2024-06-30 23:59:59.123456+00'),
                (3, 'null-fields',    NULL,          NULL,             NULL,                  NULL)"
        ))
        .execute(&pg_pool)
        .await?;

        // 2. Dump src with the real pg_dump binary.
        let dump_dir = tempfile::tempdir()?;
        let dump_path = dump_dir.path().join("dump.sql");
        let status = Command::new("pg_dump")
            .args([
                "--data-only",
                "-Fp",
                "--table",
                &src,
                "--no-owner",
                "--no-privileges",
                &source_pg_url,
            ])
            .stdout(std::fs::File::create(&dump_path)?)
            .status()
            .map_err(|e| anyhow::anyhow!("failed to spawn pg_dump (is it on PATH?): {e}"))?;
        assert!(status.success(), "pg_dump exited with {status}");

        // 3. Re-target the dump's COPY block from `src` to `dst`. pg_dump
        // emits the source table name in the COPY header; rewrite that line
        // so the loader matches the dst block. Single-table workflow.
        let dump_text = std::fs::read_to_string(&dump_path)?;
        let rewritten =
            dump_text.replace(&format!("COPY public.{src}"), &format!("COPY public.{dst}"));
        let rewritten_path = dump_dir.path().join("rewritten.sql");
        std::fs::write(&rewritten_path, rewritten)?;

        // 4. Load through the loader pipeline into the same PG instance.
        let pool = Pool::from_pg_pool(pg_pool.clone());
        let args = LoadArgs {
            endpoint: "ignored.dsql.us-east-1.on.aws".into(),
            region: "us-east-1".into(),
            username: "ignored".into(),
            source_uri: rewritten_path.to_string_lossy().into_owned(),
            target_table: dst.clone(),
            schema: "public".into(),
            format: Format::PgDump,
            worker_count: 1,
            chunk_size_bytes: 1024 * 1024,
            batch_size: 100,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: None,
            verify: VerifyMode::Off,
            test_pool: Some(pool),
        };

        let result = run_load(args).await?;
        assert_eq!(
            result.records_loaded, 3,
            "all 3 dumped rows must load; failed = {}",
            result.records_failed
        );
        assert_eq!(result.records_failed, 0);

        // 5. Type-aware fidelity check: PG itself compares src vs dst, so any
        // BYTEA/JSONB/TIMESTAMPTZ/NULL drift surfaces as a non-empty diff.
        // Run EXCEPT in both directions to catch missing AND extra rows.
        let (src_minus_dst,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM (
                 SELECT id, name, note, blob, payload, ts FROM {src}
                 EXCEPT
                 SELECT id, name, note, blob, payload, ts FROM {dst}
             ) diff"
        ))
        .fetch_one(&pg_pool)
        .await?;
        let (dst_minus_src,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM (
                 SELECT id, name, note, blob, payload, ts FROM {dst}
                 EXCEPT
                 SELECT id, name, note, blob, payload, ts FROM {src}
             ) diff"
        ))
        .fetch_one(&pg_pool)
        .await?;

        // If either is non-zero, dump the diff so the failure is debuggable.
        // Project all columns to text so the panic message shows which value
        // diverged (BYTEA as hex, JSONB and TIMESTAMPTZ via ::text).
        if src_minus_dst != 0 || dst_minus_src != 0 {
            type DiffRow = (
                i64,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
            );
            let projection = "id, name, note, encode(blob, 'hex'), \
                              payload::text, ts::text";
            let in_src: Vec<DiffRow> = sqlx::query_as(&format!(
                "SELECT {projection} FROM {src} \
                 EXCEPT \
                 SELECT {projection} FROM {dst} ORDER BY id"
            ))
            .fetch_all(&pg_pool)
            .await
            .unwrap_or_default();
            let in_dst: Vec<DiffRow> = sqlx::query_as(&format!(
                "SELECT {projection} FROM {dst} \
                 EXCEPT \
                 SELECT {projection} FROM {src} ORDER BY id"
            ))
            .fetch_all(&pg_pool)
            .await
            .unwrap_or_default();
            panic!(
                "round-trip mismatch:\n  in src not dst ({src_minus_dst}): {in_src:#?}\n  in dst not src ({dst_minus_src}): {in_dst:#?}"
            );
        }

        let (dst_count,): (i64,) = sqlx::query_as(&format!("SELECT COUNT(*)::BIGINT FROM {dst}"))
            .fetch_one(&pg_pool)
            .await?;
        assert_eq!(dst_count, 3, "dst must contain exactly 3 rows post-load");

        // 6. Cleanup. Best-effort; CI tears the container down anyway.
        let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {src}, {dst}"))
            .execute(&pg_pool)
            .await;
        Ok(())
    }

    /// Customer-shape E2E: seed PG with one construct per dsql-lint rule
    /// the migrate flow rewrites, plus every data-fidelity case the COPY
    /// decoder must preserve, then `pg_dump` → `run_migrate` → real DSQL
    /// cluster and assert the destination matches the source byte-for-byte.
    /// `#[ignore]` — needs PG + DSQL cluster + IAM. CI's `e2e-dsql` job
    /// opts in via `--ignored`.
    #[tokio::test]
    #[ignore]
    async fn pgdump_migrate_real_full_dump_collapses_serial_strips_fk() -> anyhow::Result<()> {
        let source_pg_url = std::env::var("PGDUMP_E2E_SOURCE_URL")
            .map_err(|_| anyhow::anyhow!("PGDUMP_E2E_SOURCE_URL must be set"))?;
        let dsql_endpoint = std::env::var("LOADER_DSQL_E2E_ENDPOINT").map_err(|_| {
            anyhow::anyhow!(
                "LOADER_DSQL_E2E_ENDPOINT must be set (e.g. <cluster>.dsql.us-east-1.on.aws)"
            )
        })?;
        let dsql_region =
            std::env::var("LOADER_DSQL_E2E_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        anyhow::ensure!(!source_pg_url.is_empty(), "PGDUMP_E2E_SOURCE_URL is empty");
        anyhow::ensure!(
            !dsql_endpoint.is_empty(),
            "LOADER_DSQL_E2E_ENDPOINT is empty"
        );

        let pg_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&source_pg_url)
            .await?;

        // UUID suffix so parallel runs on a shared cluster don't collide.
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let users_src = format!("e2e_users_{suffix}");
        let events_src = format!("e2e_events_{suffix}");

        // SCHEMA — one construct per rewrite rule:
        //   users.id        SERIAL PRIMARY KEY    → serial_sequence_idiom
        //   users.email     UNIQUE inline         → alter_add_unique_collapse
        //   events.id       SERIAL PRIMARY KEY    → serial_sequence_idiom
        //                                           + alter_add_primary_key_collapse
        //   events.user_id  REFERENCES            → foreign_key (removed)
        //   events.payload  JSONB                 (preserved — DSQL native)
        //   events_label_idx CREATE INDEX … USING btree
        //                                         → index_async + index_using
        //   events.note     NOT NULL DEFAULT ''   (preserved as-is)
        sqlx::query(&format!(
            "CREATE TABLE {users_src} (
                id SERIAL PRIMARY KEY,
                email TEXT NOT NULL UNIQUE
            )"
        ))
        .execute(&pg_pool)
        .await?;
        sqlx::query(&format!(
            "CREATE TABLE {events_src} (
                id SERIAL PRIMARY KEY,
                label TEXT NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                user_id INTEGER REFERENCES {users_src}(id),
                payload JSONB,
                created_at TIMESTAMPTZ NOT NULL
            )"
        ))
        .execute(&pg_pool)
        .await?;
        sqlx::query(&format!(
            "CREATE INDEX {events_src}_label_idx ON {events_src}(label)"
        ))
        .execute(&pg_pool)
        .await?;

        sqlx::query(&format!(
            "INSERT INTO {users_src} (email) VALUES ('a@example.com'), ('b@example.com')"
        ))
        .execute(&pg_pool)
        .await?;

        // DATA — every row exercises a distinct decode/encode boundary the
        // pgdump reader+worker chain must preserve. The destination assertions
        // below check each `note` value byte-for-byte, so a regression in any
        // single decode arm fails this test loudly.
        //
        // Row 1: plain text + JSON object + microsecond TS.
        // Row 2: empty `note` in NOT NULL DEFAULT '' col — pins the
        //        commit-07c0196 fix that `\N` ≠ `""` end-to-end.
        // Row 3: tab inside `note` — encoded as `\t` in COPY.
        // Row 4: newline inside `note` — encoded as `\n` in COPY; must NOT
        //        split rows. JSON null literal (distinct from SQL NULL).
        // Row 5: multi-byte UTF-8 in `note`.
        // Row 6: literal backslash in `note` (encoded `\\` in COPY).
        // Row 7: literal `\N` text inside `note` — encoded `\\N` in COPY,
        //        MUST NOT be misread as the NULL sentinel.
        // Row 8: NULL `note` is impossible (NOT NULL); NULL `user_id`,
        //        `payload`, AND a microsecond TS at year boundary.
        sqlx::query(&format!(
            "INSERT INTO {events_src} (label, note, user_id, payload, created_at) VALUES
                ('alpha',   'first',                  1,    '{{\"k\":\"v\"}}'::jsonb,                  '2024-01-15 12:34:56.789012+00'),
                ('beta',    '',                       2,    '[1,2,3,null]'::jsonb,                     '2024-06-30 23:59:59.123456+00'),
                ('gamma',   E'tab\\there',            1,    '{{\"nested\":{{\"a\":[true,false]}}}}'::jsonb, '2024-03-15 09:30:00.000001+00'),
                ('delta',   E'two\\nlines',           2,    'null'::jsonb,                              '2025-02-28 12:00:00+00'),
                ('epsilon', 'unicode-naïve-café',     1,    '\"plain string\"'::jsonb,                  '2025-07-04 18:00:00+00'),
                ('zeta',    E'back\\\\slash',         2,    '42'::jsonb,                                '2024-12-31 23:59:59.999999+00'),
                ('eta',     E'null-\\\\N-marker',     1,    '[]'::jsonb,                                '2025-01-01 00:00:00+00'),
                ('theta',   'no-fk',                  NULL, NULL,                                       '2025-12-31 00:00:00+00')"
        ))
        .execute(&pg_pool)
        .await?;

        // Full dump (no --data-only) — embeds DDL.
        let dump_dir = tempfile::tempdir()?;
        let dump_path = dump_dir.path().join("dump.sql");
        let status = Command::new("pg_dump")
            .args([
                "-Fp",
                "--table",
                &users_src,
                "--table",
                &events_src,
                "--no-owner",
                "--no-privileges",
                &source_pg_url,
            ])
            .stdout(std::fs::File::create(&dump_path)?)
            .status()
            .map_err(|e| anyhow::anyhow!("failed to spawn pg_dump (is it on PATH?): {e}"))?;
        assert!(status.success(), "pg_dump exited with {status}");

        // `test_pool: None` → production DSQL IAM-auth path.
        let args = MigrateArgs {
            endpoint: dsql_endpoint.clone(),
            region: dsql_region.clone(),
            username: "admin".to_string(),
            source_uri: format!("file://{}", dump_path.display()),
            schema: "public".to_string(),
            dry_run: false,
            worker_count: 1,
            batch_size: 100,
            batch_concurrency: 1,
            chunk_size_bytes: 1024 * 1024,
            on_conflict: OnConflict::Error,
            // Exercise the deepest tier end-to-end: `Full` runs L1+L2 +
            // the affirmative schema check + L3 per-row value verification.
            // The assertions below pin `Match` for both tables on a fresh
            // cluster, proving the recast-compare canonicalizes correctly
            // against real DSQL (the claim SQLite tests cannot make).
            verify: VerifyMode::Full,
            quiet: true,
            debug: false,
            test_pool: None,
        };

        let report = run_migrate(args).await?;

        // ── Stage 1: dsql-lint diagnostic shape ────────────────────────
        assert!(
            report.ddl_unfixable.is_empty(),
            "no unfixable diagnostics expected on this fixture, got: {:?}",
            report.ddl_unfixable
        );
        let rule_count = |r: &str| report.ddl_changes.iter().filter(|d| d.rule == r).count();
        assert_eq!(
            rule_count("serial_sequence_idiom"),
            2,
            "expected 2 serial_sequence_idiom (users.id + events.id), got: {:?}",
            report.ddl_changes
        );
        assert!(
            rule_count("foreign_key") >= 1,
            "FK should be reported as auto-removed, got: {:?}",
            report.ddl_changes
        );
        assert!(
            rule_count("alter_add_unique_collapse") >= 1,
            "UNIQUE should fold via alter_add_unique_collapse, got: {:?}",
            report.ddl_changes
        );
        // DSQL supports JSONB natively (dsql-lint >=0.2.6 dropped the
        // JSONB→JSON rewrite), so the json_type rule must NOT fire — the
        // column is preserved as jsonb (asserted on the destination below).
        assert_eq!(
            rule_count("json_type"),
            0,
            "JSONB must be preserved (no json_type rewrite), got: {:?}",
            report.ddl_changes
        );
        assert!(
            rule_count("index_async") >= 1,
            "CREATE INDEX → CREATE INDEX ASYNC should fire, got: {:?}",
            report.ddl_changes
        );

        // Separate pool for assertions (run_migrate owns its pool).
        let dsql_pool = build_dsql_pool(
            PoolArgsBuilder::default()
                .endpoint(&dsql_endpoint)
                .region(&dsql_region)
                .username("admin")
                .build()?,
        )
        .await?;

        // ── Stage 1.5: per-table verification verdict (verify=Full) ─────
        // L1+L2+L3 all ran; every loaded table must come back as Match —
        // proving the server-side recast-compare canonicalizes the seeded
        // values (timestamptz µs / jsonb / \N) on real DSQL. The
        // affirmative schema check must also be populated.
        for t in &report.tables {
            let v = t.verify.as_ref().unwrap_or_else(|| {
                panic!(
                    "verify must be Some on {}.{} under verify=Full",
                    t.schema, t.table
                )
            });
            assert_eq!(
                v.verdict,
                crate::runner::VerifyVerdict::Match,
                "{}.{} expected Match, got {:?} (source_rows={:?}, loaded={}, failed={}, target_counts={:?}, l3_details={:?})",
                t.schema,
                t.table,
                v.verdict,
                v.source_rows,
                v.records_loaded,
                v.records_failed,
                v.target_counts,
                v.l3_details,
            );
            assert!(
                v.source_rows.is_some(),
                "pgdump must produce exact source_rows; got None for {}.{}",
                t.schema,
                t.table,
            );
            let sc = v.schema_check.as_ref().unwrap_or_else(|| {
                panic!(
                    "verify=Full must populate schema_check on {}.{}",
                    t.schema, t.table
                )
            });
            assert!(
                sc.columns_matched.is_some_and(|n| n > 0) && sc.pk_present,
                "{}.{}: expected columns_matched>0 + pk_present, got {sc:?}",
                t.schema,
                t.table,
            );
        }

        // ── Stage 2: schema shape on the destination ───────────────────
        // SERIAL → identity on both PK columns.
        for (table, col) in [(&users_src, "id"), (&events_src, "id")] {
            let (is_identity,): (String,) = sqlx::query_as(&format!(
                "SELECT is_identity FROM information_schema.columns \
                 WHERE table_schema='public' AND table_name='{table}' AND column_name='{col}'"
            ))
            .fetch_one(&dsql_pool)
            .await?;
            assert_eq!(
                is_identity, "YES",
                "{table}.{col} must be is_identity=YES post-migrate"
            );
        }

        // FK auto-removed: zero FOREIGN KEY constraints on events.
        let (fk_count,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM information_schema.table_constraints \
             WHERE table_schema='public' AND table_name='{events_src}' \
             AND constraint_type='FOREIGN KEY'"
        ))
        .fetch_one(&dsql_pool)
        .await?;
        assert_eq!(fk_count, 0, "{events_src} must have 0 FK constraints");

        // Inline UNIQUE survives the pg_dump → ALTER → collapse round-trip.
        let (users_unique_count,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM information_schema.table_constraints \
             WHERE table_schema='public' AND table_name='{users_src}' \
             AND constraint_type='UNIQUE'"
        ))
        .fetch_one(&dsql_pool)
        .await?;
        assert_eq!(
            users_unique_count, 1,
            "{users_src} must have exactly 1 UNIQUE constraint"
        );

        // JSONB preserved: DSQL supports it natively, so the column stays
        // `jsonb` (dsql-lint >=0.2.6 no longer rewrites it to `json`).
        let (payload_type,): (String,) = sqlx::query_as(&format!(
            "SELECT data_type FROM information_schema.columns \
             WHERE table_schema='public' AND table_name='{events_src}' AND column_name='payload'"
        ))
        .fetch_one(&dsql_pool)
        .await?;
        assert_eq!(
            payload_type, "jsonb",
            "{events_src}.payload must be preserved as data_type='jsonb'"
        );

        // NOT NULL DEFAULT '' preserved on events.note.
        let (note_nullable, note_default): (String, Option<String>) = sqlx::query_as(&format!(
            "SELECT is_nullable, column_default FROM information_schema.columns \
             WHERE table_schema='public' AND table_name='{events_src}' AND column_name='note'"
        ))
        .fetch_one(&dsql_pool)
        .await?;
        assert_eq!(note_nullable, "NO", "events.note must remain NOT NULL");
        assert!(
            note_default.as_deref().is_some_and(|d| d.contains("''")),
            "events.note default must contain '', got {:?}",
            note_default
        );

        // ── Stage 3: data fidelity, byte-for-byte ──────────────────────
        // Decode each row at the destination and assert exact-equal against
        // what we seeded. A regression in any single COPY-decode arm
        // (escape, multi-byte, `\N` ambiguity, JSONB→JSON cast) shows up here.
        #[derive(Debug, sqlx::FromRow)]
        struct EventRow {
            label: String,
            note: String,
            user_id: Option<i32>,
            payload: Option<serde_json::Value>,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let rows: Vec<EventRow> = sqlx::query_as(&format!(
            "SELECT label, note, user_id, payload, created_at \
             FROM {events_src} ORDER BY label"
        ))
        .fetch_all(&dsql_pool)
        .await?;
        assert_eq!(rows.len(), 8, "all 8 events rows must round-trip");

        // Helper to find a row by label so the assertion list reads top-to-bottom.
        let find = |label: &str| {
            rows.iter()
                .find(|r| r.label == label)
                .unwrap_or_else(|| panic!("missing label={label}"))
        };

        // Row 1: plain.
        let r = find("alpha");
        assert_eq!(r.note, "first");
        assert_eq!(r.user_id, Some(1));
        assert_eq!(r.payload, Some(serde_json::json!({"k": "v"})));

        // Row 2: empty `note` in NOT NULL DEFAULT '' must stay empty (NOT NULL).
        // This is the iter-1 commit-07c0196 regression pin.
        let r = find("beta");
        assert_eq!(r.note, "", "empty `note` collapsed to NULL or got mangled");
        assert_eq!(r.payload, Some(serde_json::json!([1, 2, 3, null])));

        // Row 3: real tab byte inside `note`.
        let r = find("gamma");
        assert_eq!(r.note, "tab\there");
        assert_eq!(
            r.payload,
            Some(serde_json::json!({"nested": {"a": [true, false]}}))
        );

        // Row 4: real newline byte inside `note` — MUST NOT split rows.
        // JSON null literal preserved as Some(Value::Null).
        let r = find("delta");
        assert_eq!(r.note, "two\nlines");
        assert_eq!(r.payload, Some(serde_json::Value::Null));

        // Row 5: multi-byte UTF-8 in `note`.
        let r = find("epsilon");
        assert_eq!(r.note, "unicode-naïve-café");
        assert_eq!(
            r.payload,
            Some(serde_json::Value::String("plain string".to_string()))
        );

        // Row 6: single backslash in `note` (encoded `\\` in COPY).
        let r = find("zeta");
        assert_eq!(r.note, "back\\slash");
        assert_eq!(r.payload, Some(serde_json::json!(42)));

        // Row 7: literal text `null-\N-marker` — the `\N` here is data, not the
        // NULL sentinel. pg_dump emits this as `null-\\N-marker`; the loader's
        // `decode_field` must NOT collapse the embedded `\N` to NULL.
        let r = find("eta");
        assert_eq!(
            r.note, "null-\\N-marker",
            "literal `\\N` inside data was misread as NULL sentinel"
        );
        assert_eq!(r.payload, Some(serde_json::json!([])));

        // Row 8: NULL on user_id and payload preserved as SQL NULL.
        let r = find("theta");
        assert_eq!(r.note, "no-fk");
        assert!(
            r.user_id.is_none(),
            "user_id should be SQL NULL, got {:?}",
            r.user_id
        );
        assert!(
            r.payload.is_none(),
            "payload should be SQL NULL, got {:?}",
            r.payload
        );

        // TIMESTAMPTZ microsecond fidelity: pin two extreme cases.
        // alpha = 2024-01-15 12:34:56.789012 UTC (six-digit fractional).
        let alpha_ts = find("alpha").created_at;
        let expected_alpha = chrono::DateTime::parse_from_rfc3339("2024-01-15T12:34:56.789012Z")?
            .with_timezone(&chrono::Utc);
        assert_eq!(
            alpha_ts, expected_alpha,
            "TIMESTAMPTZ microsecond precision lost on alpha"
        );
        // zeta = end-of-year 2024 with .999999 microseconds.
        let zeta_ts = find("zeta").created_at;
        let expected_zeta = chrono::DateTime::parse_from_rfc3339("2024-12-31T23:59:59.999999Z")?
            .with_timezone(&chrono::Utc);
        assert_eq!(
            zeta_ts, expected_zeta,
            "TIMESTAMPTZ microsecond lost on zeta"
        );

        // Users data sanity (UNIQUE round-trip + identity column).
        #[derive(Debug, sqlx::FromRow)]
        struct UserRow {
            email: String,
        }
        let user_rows: Vec<UserRow> =
            sqlx::query_as(&format!("SELECT email FROM {users_src} ORDER BY email"))
                .fetch_all(&dsql_pool)
                .await?;
        let emails: Vec<&str> = user_rows.iter().map(|u| u.email.as_str()).collect();
        assert_eq!(emails, vec!["a@example.com", "b@example.com"]);

        // ── Stage 4: L3 catches a post-load divergence on real DSQL ─────
        // The migrate-time verify passed (Match). Now mutate one target
        // value out from under the loader and re-run L3 directly: it must
        // flag ValueMismatch and localize the offending PK — a value that
        // diverges from the source while counts stay intact.
        {
            // Find events row (label 'alpha') and corrupt its note. `id` is
            // BIGINT after the SERIAL→IDENTITY rewrite, so fetch as i64.
            let (alpha_id,): (i64,) = sqlx::query_as(&format!(
                "SELECT id FROM {events_src} WHERE label = 'alpha'"
            ))
            .fetch_one(&dsql_pool)
            .await?;
            dsql_pool
                .execute_query(&format!(
                    "UPDATE {events_src} SET note = 'CORRUPTED' WHERE id = {alpha_id}"
                ))
                .await?;

            let reader: Arc<dyn ByteReader> = Arc::new(LocalFileByteReader::new(&dump_path));
            let block = find_copy_block(&*reader, "public", &events_src).await?;
            let (outcome, details) =
                crate::verify::verify_table_values(&dsql_pool, reader, &block, 1024 * 1024).await?;

            assert_eq!(
                outcome,
                crate::verify::L3Outcome::Ran {
                    value_mismatches: 1,
                    rows_missing_at_target: 0
                },
                "post-load mutation must surface exactly one value mismatch",
            );
            assert_eq!(
                details.mismatch_pks,
                vec![alpha_id.to_string()],
                "L3 must localize the corrupted row's primary key",
            );
        }

        // ── Cleanup: best-effort (per-run CI cluster is torn down anyway).
        let _ = sqlx::query(&format!(
            "DROP TABLE IF EXISTS {events_src}, {users_src} CASCADE"
        ))
        .execute(&pg_pool)
        .await;
        let _ = dsql_pool
            .execute_query(&format!("DROP TABLE IF EXISTS {events_src}"))
            .await;
        let _ = dsql_pool
            .execute_query(&format!("DROP TABLE IF EXISTS {users_src}"))
            .await;
        Ok(())
    }

    /// Regression: `load --verify=full` into columns whose declared
    /// precision/scale is NARROWER than the source text must still verify as
    /// `Match`. The load coerces the value to the column's typmod (e.g.
    /// `numeric(10,2)` stores `1.005`→`1.01`, `timestamp(0)` drops sub-second);
    /// L3 must recast the source through the column's *parameterized* type so
    /// it rounds identically. Before the fix L3 recast through the bare type
    /// name (no scale), keeping `1.005`, and false-mismatched every such row.
    ///
    /// `#[ignore]` — needs a real DSQL cluster + IAM (CI `e2e-dsql`). DSQL is
    /// the type authority here; SQLite can't reproduce typmod coercion.
    #[tokio::test]
    #[ignore]
    async fn pgdump_load_verify_full_typmod_narrower_than_source_matches() -> anyhow::Result<()> {
        let dsql_endpoint = std::env::var("LOADER_DSQL_E2E_ENDPOINT").map_err(|_| {
            anyhow::anyhow!("LOADER_DSQL_E2E_ENDPOINT must be set (<cluster>.dsql.<region>.on.aws)")
        })?;
        let dsql_region =
            std::env::var("LOADER_DSQL_E2E_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        anyhow::ensure!(
            !dsql_endpoint.is_empty(),
            "LOADER_DSQL_E2E_ENDPOINT is empty"
        );

        let dsql_pool = build_dsql_pool(
            PoolArgsBuilder::default()
                .endpoint(&dsql_endpoint)
                .region(&dsql_region)
                .username("admin")
                .build()?,
        )
        .await?;

        // UUID-suffixed table so parallel runs on a shared cluster don't collide.
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let table = format!("e2e_typmod_{suffix}");

        // Narrow columns: scale-2 numeric and second-precision timestamps.
        dsql_pool
            .execute_query(&format!(
                "CREATE TABLE {table} (
                    id    int PRIMARY KEY,
                    amt   numeric(10,2),
                    ts0   timestamp(0),
                    tstz0 timestamptz(0)
                )"
            ))
            .await?;

        // Source COPY text is WIDER than every column's precision, so a
        // faithful load must coerce: 1.005→1.01, .6 seconds→rounded.
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "COPY public.{table} (id, amt, ts0, tstz0) FROM stdin;")?;
        writeln!(
            f,
            "1\t1.005\t2024-01-01 10:23:54.6\t2024-01-01 10:23:54.6+02"
        )?;
        writeln!(
            f,
            "2\t2.675\t2024-06-30 23:59:59.5\t2024-06-30 23:59:59.5+00"
        )?;
        writeln!(f, "\\.")?;
        f.flush()?;

        let mut args = pgdump_load_args(
            f.path().to_string_lossy().into_owned(),
            &table,
            dsql_pool.clone(),
        );
        args.verify = VerifyMode::Full;

        let result = run_load(args).await?;

        // The fix's payoff: a faithful narrowing load verifies clean. Before
        // the parameterized recast this was ValueMismatch(2).
        let v = result.verify.expect("verify=full must populate verify");
        assert_eq!(
            v.verdict,
            crate::verify::VerifyVerdict::Match,
            "narrowing load must verify as Match, got {:?} (l3_details={:?})",
            v.verdict,
            v.l3_details,
        );

        // Guard against a trivial pass: prove the column REALLY coerced the
        // value (so the recast genuinely had to round to match, not just
        // compare equal text). 1.005 must be stored as 1.01.
        let (stored_amt,): (String,) =
            sqlx::query_as(&format!("SELECT amt::text FROM {table} WHERE id = 1"))
                .fetch_one(&dsql_pool)
                .await?;
        assert_eq!(
            stored_amt, "1.01",
            "column must have rounded 1.005→1.01 (else the test isn't exercising typmod)"
        );

        let _ = dsql_pool
            .execute_query(&format!("DROP TABLE IF EXISTS {table}"))
            .await;
        Ok(())
    }

    /// Regression: `load --verify=full` on a table whose PRIMARY KEY is a type
    /// the target canonicalizes (numeric scale, uuid case) must verify as
    /// `Match`. L3 keys its row diff on the verbatim source PK text; if the
    /// query projected `CAST(pk AS TEXT)` (target-canonical) instead, a source
    /// PK `1.5` stored as `1.50` would miss the lookup and false-report
    /// `ValueRowMissingAtTarget` on a faithful load.
    ///
    /// `#[ignore]` — needs a real DSQL cluster + IAM (CI `e2e-dsql`). SQLite
    /// doesn't canonicalize numeric scale, so only DSQL exercises the gap.
    #[tokio::test]
    #[ignore]
    async fn pgdump_load_verify_full_canonicalizing_pk_matches() -> anyhow::Result<()> {
        let dsql_endpoint = std::env::var("LOADER_DSQL_E2E_ENDPOINT").map_err(|_| {
            anyhow::anyhow!("LOADER_DSQL_E2E_ENDPOINT must be set (<cluster>.dsql.<region>.on.aws)")
        })?;
        let dsql_region =
            std::env::var("LOADER_DSQL_E2E_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        anyhow::ensure!(
            !dsql_endpoint.is_empty(),
            "LOADER_DSQL_E2E_ENDPOINT is empty"
        );

        let dsql_pool = build_dsql_pool(
            PoolArgsBuilder::default()
                .endpoint(&dsql_endpoint)
                .region(&dsql_region)
                .username("admin")
                .build()?,
        )
        .await?;

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let table = format!("e2e_canonpk_{suffix}");

        // numeric(10,2) PK: source text `1.5` is stored canonical as `1.50`.
        dsql_pool
            .execute_query(&format!(
                "CREATE TABLE {table} (id numeric(10,2) PRIMARY KEY, note text)"
            ))
            .await?;

        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "COPY public.{table} (id, note) FROM stdin;")?;
        writeln!(f, "1.5\talpha")?;
        writeln!(f, "2.675\tbeta")?;
        writeln!(f, "\\.")?;
        f.flush()?;

        let mut args = pgdump_load_args(
            f.path().to_string_lossy().into_owned(),
            &table,
            dsql_pool.clone(),
        );
        args.verify = VerifyMode::Full;

        let result = run_load(args).await?;

        let v = result.verify.expect("verify=full must populate verify");
        assert_eq!(
            v.verdict,
            crate::verify::VerifyVerdict::Match,
            "canonicalizing PK must verify as Match, got {:?} (l3_details={:?})",
            v.verdict,
            v.l3_details,
        );

        // Guard against a trivial pass: prove the PK actually canonicalized
        // (so the source-text echo genuinely had to differ from the target's
        // CAST(pk AS TEXT)). Source `1.5` must be stored as `1.50`.
        let (stored_pk,): (String,) = sqlx::query_as(&format!(
            "SELECT id::text FROM {table} WHERE note = 'alpha'"
        ))
        .fetch_one(&dsql_pool)
        .await?;
        assert_eq!(
            stored_pk, "1.50",
            "PK must have canonicalized 1.5→1.50 (else the test isn't exercising the gap)"
        );

        let _ = dsql_pool
            .execute_query(&format!("DROP TABLE IF EXISTS {table}"))
            .await;
        Ok(())
    }

    /// True DSQL→DSQL round-trip: seed a real DSQL cluster with a DSQL-native
    /// identity table, `pg_dump` it THROUGH THE pgdump-proxy (stock pg_dump
    /// can't read DSQL directly), then `run_migrate` the resulting dump back
    /// into the same cluster and assert the DSQL-specific idioms survive the
    /// round-trip: the identity column re-lands as an identity (counter
    /// advanced by the dump's trailing `setval`), and the data matches.
    ///
    /// `#[ignore]` — needs a DSQL cluster + IAM + the proxy script + a pg_dump
    /// on PATH. CI's `e2e-dsql` job opts in via `--ignored`. Skips with a
    /// message when `LOADER_DSQL_E2E_ENDPOINT` or `DSQL_PGDUMP_PROXY` is unset.
    #[tokio::test]
    #[ignore]
    async fn pgdump_migrate_real_dsql_to_dsql_round_trip() -> anyhow::Result<()> {
        let Some(dsql_endpoint) = std::env::var("LOADER_DSQL_E2E_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty())
        else {
            eprintln!(
                "skipping pgdump_migrate_real_dsql_to_dsql_round_trip: \
                 LOADER_DSQL_E2E_ENDPOINT not set"
            );
            return Ok(());
        };
        let Some(proxy_script) = std::env::var("DSQL_PGDUMP_PROXY")
            .ok()
            .filter(|v| !v.is_empty())
        else {
            eprintln!(
                "skipping pgdump_migrate_real_dsql_to_dsql_round_trip: \
                 DSQL_PGDUMP_PROXY (path to pgdump-proxy script) not set"
            );
            return Ok(());
        };
        let dsql_region =
            std::env::var("LOADER_DSQL_E2E_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        // Pool for seeding + assertions (run_migrate owns its own pool).
        let dsql_pool = build_dsql_pool(
            PoolArgsBuilder::default()
                .endpoint(&dsql_endpoint)
                .region(&dsql_region)
                .username("admin")
                .build()?,
        )
        .await?;

        // UUID suffix so parallel runs on a shared cluster don't collide.
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let table = format!("e2e_dsql_{suffix}");

        // Seed a DSQL-native table: identity PK (explicit CACHE, required by
        // DSQL), inline UNIQUE, JSON column (DSQL stamps lz4 compression on it).
        // One DDL per statement — DSQL rejects multi-DDL transactions.
        dsql_pool
            .execute_query(&format!(
                "CREATE TABLE public.{table} (
                     id BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1) PRIMARY KEY,
                     email TEXT NOT NULL UNIQUE,
                     payload JSON
                 )"
            ))
            .await?;
        dsql_pool
            .execute_query(&format!(
                "INSERT INTO public.{table} (email, payload) \
                 VALUES ('a@example.com', '{{\"k\":1}}'), ('b@example.com', '[1,2,3]')"
            ))
            .await?;

        // Mint a fresh admin auth token for pg_dump's PGPASSWORD.
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(dsql_region.clone()))
            .load()
            .await;
        let token_config = aws_sdk_dsql::auth_token::Config::builder()
            .hostname(&dsql_endpoint)
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build auth-token config: {e}"))?;
        let token = aws_sdk_dsql::auth_token::AuthTokenGenerator::new(token_config)
            .db_connect_admin_auth_token(&sdk_config)
            .await
            .map_err(|e| anyhow::anyhow!("failed to mint DSQL auth token: {e}"))?;

        // Start the pgdump-proxy on a fixed local port; it bridges plaintext
        // localhost -> DSQL TLS and swallows the setup statements DSQL rejects.
        let proxy_port = "6543";
        let mut proxy = Command::new("python3")
            .args([
                proxy_script.as_str(),
                dsql_endpoint.as_str(),
                "--listen-port",
                proxy_port,
            ])
            .spawn()
            .map_err(|e| {
                anyhow::anyhow!("failed to spawn pgdump-proxy (is python3 on PATH?): {e}")
            })?;

        // The TempDir holding the dump must outlive run_migrate (which reads it),
        // so it lives in the test scope rather than the proxy block below — its
        // Drop then removes the dump on every exit path, including a later panic.
        let dump_dir = tempfile::tempdir()?;
        let dump_path = dump_dir.path().join("dsql_dump.sql");

        // pg_dump the DSQL cluster THROUGH the proxy. Wrapped so the proxy is
        // always killed (and reaped) even if a step fails.
        let dump_result: anyhow::Result<()> = async {
            // Wait for the proxy to accept connections instead of a fixed sleep:
            // poll-connect, and fail fast if the proxy died (e.g. port in use).
            let addr = format!("127.0.0.1:{proxy_port}");
            let mut ready = false;
            for _ in 0..50 {
                if let Some(status) = proxy.try_wait()? {
                    anyhow::bail!("pgdump-proxy exited before accepting connections: {status}");
                }
                if std::net::TcpStream::connect(&addr).is_ok() {
                    ready = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            anyhow::ensure!(ready, "pgdump-proxy did not start listening on {addr} in time");

            let status = Command::new("pg_dump")
                .args([
                    "-Fp",
                    "--no-owner",
                    "--no-privileges",
                    "--table",
                    &format!("public.{table}"),
                    &format!(
                        "host=127.0.0.1 port={proxy_port} dbname=postgres user=admin sslmode=disable"
                    ),
                ])
                .env("PGPASSWORD", token.as_str())
                .stdout(std::fs::File::create(&dump_path)?)
                .status()
                .map_err(|e| anyhow::anyhow!("failed to spawn pg_dump (is it on PATH?): {e}"))?;
            anyhow::ensure!(status.success(), "pg_dump through proxy exited with {status}");
            Ok(())
        }
        .await;
        // kill() only signals; wait() reaps so we don't leak a zombie / orphaned
        // listener on the fixed port for the rest of the test-binary's life.
        let _ = proxy.kill();
        let _ = proxy.wait();
        dump_result?;

        // Drop the source table so the migrate re-creates it from the dump's
        // DDL — a genuine round-trip, not a no-op against an existing table.
        dsql_pool
            .execute_query(&format!("DROP TABLE IF EXISTS public.{table}"))
            .await?;

        // Migrate the DSQL-sourced dump back into the cluster. `test_pool: None`
        // → production IAM path. verify=Count closes the loop per table.
        let report = run_migrate(MigrateArgs {
            endpoint: dsql_endpoint.clone(),
            region: dsql_region.clone(),
            username: "admin".to_string(),
            source_uri: format!("file://{}", dump_path.display()),
            schema: "public".to_string(),
            dry_run: false,
            worker_count: 1,
            batch_size: 100,
            batch_concurrency: 1,
            chunk_size_bytes: 1024 * 1024,
            on_conflict: crate::coordination::manifest::OnConflict::Error,
            verify: VerifyMode::Count,
            quiet: true,
            debug: false,
            test_pool: None,
        })
        .await?;
        // dump_path lives in dump_dir (TempDir); it auto-cleans on Drop.

        // ── The DSQL-native idioms must all have transformed cleanly. ───────
        assert!(
            report.ddl_unfixable.is_empty(),
            "DSQL-sourced dump must migrate with zero unfixable, got: {:?}",
            report.ddl_unfixable
        );
        let has_rule = |r: &str| report.ddl_changes.iter().any(|d| d.rule == r);
        assert!(
            has_rule("identity_add_generated_collapse"),
            "identity ALTER must collapse, got: {:?}",
            report.ddl_changes
        );
        assert!(
            has_rule("alter_column_set_compression_strip"),
            "SET COMPRESSION must be stripped, got: {:?}",
            report.ddl_changes
        );
        assert_eq!(report.tables.len(), 1, "exactly one table should load");
        assert_eq!(report.tables[0].records_loaded, 2);
        assert_eq!(report.tables[0].records_failed, 0);

        // The reads below run microseconds after the migrate's DDL committed,
        // so they can trip DSQL's optimistic-concurrency check (OC001, "schema
        // has been updated by another transaction") against the just-changed
        // catalog. Wrap them in the connector's `retry_on_occ` (SQLSTATE-based
        // detection + exponential backoff) rather than hand-rolling a retry.
        let occ = aurora_dsql_sqlx_connector::OCCRetryConfig::default();

        // ── The identity column re-lands as an identity on the destination. ─
        let (is_identity,): (String,) = aurora_dsql_sqlx_connector::retry_on_occ(&occ, || async {
            sqlx::query_as(&format!(
                "SELECT is_identity FROM information_schema.columns \
                     WHERE table_schema='public' AND table_name='{table}' AND column_name='id'"
            ))
            .fetch_one(&dsql_pool)
            .await
        })
        .await?;
        assert_eq!(is_identity, "YES", "{table}.id must be an identity column");

        // ── Data round-tripped byte-for-byte. ───────────────────────────────
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            email: String,
            payload: Option<serde_json::Value>,
        }
        let rows: Vec<Row> = aurora_dsql_sqlx_connector::retry_on_occ(&occ, || async {
            sqlx::query_as(&format!(
                "SELECT id, email, payload FROM public.{table} ORDER BY id"
            ))
            .fetch_all(&dsql_pool)
            .await
        })
        .await?;
        assert_eq!(rows.len(), 2);
        // Identity values round-trip (the loaded id column carries the source values).
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[0].email, "a@example.com");
        assert_eq!(rows[0].payload, Some(serde_json::json!({"k": 1})));
        assert_eq!(rows[1].id, 2);
        assert_eq!(rows[1].email, "b@example.com");
        assert_eq!(rows[1].payload, Some(serde_json::json!([1, 2, 3])));

        // ── Counter advanced: the dump's trailing setval landed on the inline
        // identity's implicit sequence, so the next insert gets id=3. ────────
        // Safe under retry: an OCC-conflicted INSERT does not commit, so a retry
        // does not double-insert.
        let (next_id,): (i64,) = aurora_dsql_sqlx_connector::retry_on_occ(&occ, || async {
            sqlx::query_as(&format!(
                "INSERT INTO public.{table} (email) VALUES ('c@example.com') RETURNING id"
            ))
            .fetch_one(&dsql_pool)
            .await
        })
        .await?;
        assert_eq!(
            next_id, 3,
            "identity counter must be advanced past the loaded max id by the dump's setval"
        );

        // ── Cleanup (best-effort; per-run CI cluster is torn down anyway). ───
        let _ = dsql_pool
            .execute_query(&format!("DROP TABLE IF EXISTS public.{table}"))
            .await;
        Ok(())
    }

    #[tokio::test]
    async fn pgdump_errors_on_column_set_mismatch() -> anyhow::Result<()> {
        // Target table has an extra column not present in the dump's COPY clause.
        // Reordering cannot rescue this — the sets diverge.
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "COPY public.things (id, name) FROM stdin;")?;
        writeln!(f, "1\twidget")?;
        writeln!(f, "\\.")?;
        f.flush()?;

        let pool = setup_sqlite_table("things", "id INTEGER, name TEXT, note TEXT NOT NULL").await;
        let args = pgdump_load_args(
            f.path().to_string_lossy().into_owned(),
            "things",
            pool.clone(),
        );

        let err = run_load(args).await.unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("column-set mismatch"),
            "expected column-set mismatch error, got: {msg}"
        );

        assert_eq!(
            get_table_count(&pool, "things").await,
            0,
            "column-set guard must reject before any rows are inserted"
        );
        Ok(())
    }

    // ============ Verify (source_rows + LoadResult.verify) ============

    /// pgdump: source_rows == record count.
    #[tokio::test]
    async fn pgdump_load_populates_source_rows_exact_count() -> anyhow::Result<()> {
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "COPY public.things (id, name) FROM stdin;")?;
        for i in 0..10 {
            writeln!(f, "{i}\titem{i}")?;
        }
        writeln!(f, "\\.")?;
        f.flush()?;

        let pool = setup_sqlite_table("things", "id INTEGER, name TEXT").await;
        let args = pgdump_load_args(
            f.path().to_string_lossy().into_owned(),
            "things",
            pool.clone(),
        );
        let result = run_load(args).await?;

        assert_eq!(result.source_rows, Some(10));
        assert_eq!(result.records_loaded, 10);
        assert_eq!(result.records_failed, 0);
        let v = result.verify.as_ref().unwrap();
        assert_eq!(v.verdict, crate::verify::VerifyVerdict::Match);
        assert_eq!(
            v.target_counts,
            Some(crate::verify::L2Counts { pre: 0, post: 10 })
        );
        Ok(())
    }

    /// parquet: source_rows == sum of row-group `num_rows`.
    #[tokio::test]
    async fn parquet_load_populates_source_rows_exact_count() {
        let temp_dir = TempDir::new().unwrap();
        let parquet_path = temp_dir.path().join("verify.parquet");
        let schema = ArrowSchema::new(vec![Field::new("id", DataType::Int32, false)]);
        let file = std::fs::File::create(&parquet_path).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(20)
            .build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).unwrap();
        let id_array = Int32Array::from_iter_values(0..50);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(id_array)]).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let pool = setup_sqlite_table("verify_pq", "id INTEGER").await;
        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: parquet_path.to_str().unwrap().to_string(),
            target_table: "verify_pq".to_string(),
            schema: "public".to_string(),
            format: Format::Parquet,
            worker_count: 1,
            chunk_size_bytes: 10_000_000,
            batch_size: 100,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            verify: VerifyMode::Count,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: None,
            test_pool: Some(pool.clone()),
        };
        let result = run_load(args).await.unwrap();
        assert_eq!(result.source_rows, Some(50));
        assert_eq!(result.records_loaded, 50);
        let v = result.verify.as_ref().unwrap();
        assert_eq!(v.verdict, crate::verify::VerifyVerdict::Match);
        assert_eq!(
            v.target_counts,
            Some(crate::verify::L2Counts { pre: 0, post: 50 })
        );
    }

    /// csv + verify=Count → SkippedNoExactSourceCount (no exact source count).
    #[tokio::test]
    async fn csv_load_with_verify_count_yields_skipped_verdict() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "verify.csv", 10).await;
        let pool =
            setup_sqlite_table("verify_csv", "id TEXT, name TEXT, value TEXT, amount TEXT").await;
        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path,
            target_table: "verify_csv".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 10_000_000,
            batch_size: 100,
            batch_concurrency: 1,
            create_table_if_missing: false,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            verify: VerifyMode::Count,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            test_pool: Some(pool.clone()),
        };
        let result = run_load(args).await.unwrap();
        assert_eq!(
            result.source_rows, None,
            "csv has no exact source-row count, so source_rows must be None"
        );
        let v = result
            .verify
            .as_ref()
            .expect("verify=Count must produce a VerifyOutcome");
        assert_eq!(
            v.verdict,
            crate::verify::VerifyVerdict::SkippedNoExactSourceCount,
            "csv with verify=Count must yield SkippedNoExactSourceCount"
        );
    }

    /// `--if-not-exists` + verify=Count against a populated target
    /// must skip L2 (else a re-run would report ExtraTarget). L1-only
    /// path → SkippedNoExactSourceCount for csv.
    #[tokio::test]
    async fn csv_load_with_if_not_exists_and_verify_count_skips_l2() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "verify.csv", 10).await;

        // Pre-populate: simulates a prior load + operator re-run under --if-not-exists.
        let pool =
            setup_sqlite_table("verify_idem", "id TEXT, name TEXT, value TEXT, amount TEXT").await;
        if let Ok(mut conn) = pool.acquire().await
            && let crate::db::pool::PoolConnection::Sqlite(ref mut c) = conn
        {
            sqlx::query("INSERT INTO verify_idem VALUES ('99', 'pre', '0.5', '0')")
                .execute(&mut **c)
                .await
                .unwrap();
        }

        let args = LoadArgs {
            endpoint: "test".to_string(),
            region: "us-west-2".to_string(),
            username: "test".to_string(),
            source_uri: csv_path,
            target_table: "verify_idem".to_string(),
            schema: "public".to_string(),
            format: Format::Csv,
            worker_count: 1,
            chunk_size_bytes: 10_000_000,
            batch_size: 100,
            batch_concurrency: 1,
            create_table_if_missing: true,
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            verify: VerifyMode::Count,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: Some(true),
            test_pool: Some(pool.clone()),
        };
        let result = run_load(args).await.unwrap();
        let v = result
            .verify
            .as_ref()
            .expect("verify=Count must produce a VerifyOutcome");
        // L2 skipped under --if-not-exists.
        assert_eq!(v.target_counts, None);
        // csv → L1 also skipped → SkippedNoExactSourceCount.
        assert_eq!(
            v.verdict,
            crate::verify::VerifyVerdict::SkippedNoExactSourceCount,
            "csv + --if-not-exists must yield SkippedNoExactSourceCount, not ExtraTarget"
        );
    }

    /// `validate_load_args` rejects `--if-not-exists` with pgdump
    /// today; this is a tripwire if that ever relaxes.
    #[tokio::test]
    async fn pgdump_with_if_not_exists_is_rejected_at_validation() -> anyhow::Result<()> {
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "COPY public.things (id, name) FROM stdin;")?;
        writeln!(f, "1\twidget")?;
        writeln!(f, "\\.")?;
        f.flush()?;

        let pool = setup_sqlite_table("things", "id INTEGER, name TEXT").await;
        let mut args = pgdump_load_args(f.path().to_string_lossy().into_owned(), "things", pool);
        args.create_table_if_missing = true;
        args.verify = VerifyMode::Count;
        let err = run_load(args).await.unwrap_err().to_string();
        assert!(
            err.contains("create_table_if_missing"),
            "pgdump + --if-not-exists must be rejected at validation; got: {err}"
        );
        Ok(())
    }

    /// Symmetric to migrate's `run_migrate_rejects_dump_identifiers_with_embedded_quote`:
    /// the load path must reject a COPY column name with `"` before INSERT runs.
    #[tokio::test]
    async fn pgdump_load_rejects_column_name_with_embedded_quote() -> anyhow::Result<()> {
        let mut f = tempfile::NamedTempFile::new()?;
        // `""` decodes to one `"` in the column name — would corrupt INSERT
        // identifier quoting if validation is skipped.
        writeln!(f, "COPY public.things (id, \"evil\"\"col\") FROM stdin;")?;
        writeln!(f, "1\twidget")?;
        writeln!(f, "\\.")?;
        f.flush()?;

        let pool = setup_sqlite_table("things", "id INTEGER, name TEXT").await;
        let args = pgdump_load_args(f.path().to_string_lossy().into_owned(), "things", pool);
        let err = run_load(args).await.unwrap_err().to_string();
        assert!(
            err.contains("pg_dump column identifier") && err.contains("unsafe character"),
            "expected validation error naming the column field; got: {err}"
        );
        Ok(())
    }

    /// Plain `load --verify=full` on pgdump runs L3 end-to-end (resolving
    /// the COPY block itself, unlike migrate) and reports a clean Match
    /// with the affirmative schema_check populated. Pins the load-path L3
    /// wiring, which is separate from the migrate orchestrator path.
    #[tokio::test]
    async fn pgdump_load_verify_full_matches_with_schema_check() -> anyhow::Result<()> {
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "COPY public.things (id, name) FROM stdin;")?;
        writeln!(f, "1\talpha")?;
        writeln!(f, "2\tbeta")?;
        writeln!(f, "\\.")?;
        f.flush()?;

        let pool = setup_sqlite_table("things", "id INTEGER PRIMARY KEY, name TEXT").await;
        let mut args = pgdump_load_args(f.path().to_string_lossy().into_owned(), "things", pool);
        args.verify = VerifyMode::Full;

        let result = run_load(args).await?;
        let v = result.verify.expect("verify=full must populate verify");
        assert_eq!(v.verdict, crate::verify::VerifyVerdict::Match);
        assert_eq!(
            v.schema_check,
            Some(crate::verify::SchemaCheck {
                columns_matched: Some(2),
                pk_present: true,
            })
        );
        Ok(())
    }

    /// End-to-end proof that the native `export` reader produces a dump the
    /// `migrate` flow reloads with full fidelity: seed PG with type variety
    /// (identity PK, varchar/numeric modifiers, bytea, jsonb, timestamptz,
    /// NULLs, COPY-special characters) plus a secondary index, `run_export`
    /// the table to a `.sql`, retarget it to a fresh table name, `run_migrate`
    /// it back into the same PG instance, and compare with `EXCEPT` in both
    /// directions so any DDL- or data-generation bug surfaces as a diff.
    ///
    /// `#[ignore]` — needs a real Postgres (CI's `e2e-dsql` job provides one via
    /// its `postgres` service and sets `PGDUMP_E2E_SOURCE_URL`; the job opts in
    /// via `--ignored`). Also self-skips with a message when the var is unset,
    /// so a stray `cargo test -- --ignored` locally still passes without a PG.
    #[tokio::test]
    #[ignore]
    async fn export_round_trip_pg_to_pg() -> anyhow::Result<()> {
        let Some(source_pg_url) = std::env::var("PGDUMP_E2E_SOURCE_URL")
            .ok()
            .filter(|v| !v.is_empty())
        else {
            eprintln!("skipping export_round_trip_pg_to_pg: PGDUMP_E2E_SOURCE_URL not set");
            return Ok(());
        };

        let pg_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&source_pg_url)
            .await?;

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let src = format!("export_src_{suffix}");
        let dst = format!("export_dst_{suffix}");

        // SOURCE: identity PK (exercises CACHE + BY DEFAULT downgrade),
        // varchar/numeric type modifiers, a column DEFAULT, and the full
        // data-fidelity matrix.
        //
        // No secondary index here: dsql-lint rewrites `CREATE INDEX` to
        // `CREATE INDEX ASYNC` (correct for DSQL), but this round-trip reloads
        // into stock Postgres, whose parser rejects `ASYNC`. Verbatim
        // pg_get_indexdef emission + the ASYNC/USING normalization is covered
        // by the dsql-lint unit/probe path; this test focuses on the schema +
        // data fidelity Postgres can verify.
        sqlx::query(&format!(
            "CREATE TABLE {src} (
                 id BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1) PRIMARY KEY,
                 name VARCHAR(50) NOT NULL,
                 bal NUMERIC(10,2),
                 status TEXT DEFAULT 'active',
                 note TEXT,
                 blob BYTEA,
                 payload JSONB,
                 ts TIMESTAMPTZ
             )"
        ))
        .execute(&pg_pool)
        .await?;

        // Each row hits a distinct decode class: embedded tab/newline,
        // backslash, the literal text "\N", bytea hex, jsonb, NULLs.
        sqlx::query(&format!(
            "INSERT INTO {src} (id, name, bal, status, note, blob, payload, ts) VALUES
                 (1, 'plain',   12.50, 'active',  E'tab\\there',  E'\\\\xDEADBEEF', '{{\"a\":1}}'::jsonb,  '2024-01-15 12:34:56+00'),
                 (2, 'naïve',   0.00, 'paused',  E'two\\nlines', E'\\\\x00FF',     '[1,2,3,null]'::jsonb, '2024-06-30 23:59:59.123456+00'),
                 (3, 'literal', NULL,  'active',  E'\\\\N text',    NULL,             NULL,                  NULL)"
        ))
        .execute(&pg_pool)
        .await?;

        // EXPORT through the native catalog reader into a temp .sql file.
        let dump_dir = tempfile::tempdir()?;
        let dump_path = dump_dir.path().join("export.sql");
        let pool = Pool::from_pg_pool(pg_pool.clone());
        let report = run_export(ExportArgs {
            endpoint: "ignored.dsql.us-east-1.on.aws".into(),
            region: "us-east-1".into(),
            username: "ignored".into(),
            schema: Some("public".into()),
            table: Some(src.clone()),
            output: Some(dump_path.clone()),
            test_pool: Some(pool),
        })
        .await?;
        assert_eq!(report.tables, vec![format!("public.{src}")]);

        // Retarget the dump from src to dst so we can reload into the same PG
        // and diff. All emitted identifiers are quoted, so the qualified
        // `"public"."src"` token is unambiguous.
        let dump_text = std::fs::read_to_string(&dump_path)?;
        let retargeted = dump_text
            .replace(
                &format!("\"public\".\"{src}\""),
                &format!("\"public\".\"{dst}\""),
            )
            // The identity setval references the source sequence by its
            // unquoted implicit name (`<table>_id_seq`); retarget it so the
            // continuation lands on the dst table's own sequence. (In a real
            // cross-cluster move no rewrite is needed — names are identical.)
            .replace(&format!("{src}_id_seq"), &format!("{dst}_id_seq"));
        let retargeted_path = dump_dir.path().join("retargeted.sql");
        std::fs::write(&retargeted_path, &retargeted)?;

        // MIGRATE the exported dump into the fresh table.
        let migrate_pool = Pool::from_pg_pool(pg_pool.clone());
        let migrate_report = run_migrate(MigrateArgs {
            endpoint: "ignored.dsql.us-east-1.on.aws".into(),
            region: "us-east-1".into(),
            username: "ignored".into(),
            source_uri: format!("file://{}", retargeted_path.display()),
            schema: "public".into(),
            dry_run: false,
            worker_count: 1,
            batch_size: 100,
            batch_concurrency: 1,
            chunk_size_bytes: 1024 * 1024,
            on_conflict: crate::runner::OnConflict::Error,
            verify: VerifyMode::Off,
            quiet: true,
            debug: false,
            test_pool: Some(migrate_pool),
        })
        .await?;
        assert!(
            migrate_report.ddl_unfixable.is_empty(),
            "exported dump must reload with zero unfixable diagnostics, got: {:?}",
            migrate_report.ddl_unfixable
        );
        assert_eq!(migrate_report.tables.len(), 1, "one table should load");
        assert_eq!(migrate_report.tables[0].records_failed, 0);

        // FIDELITY: PG compares src vs dst itself, both directions.
        let projection =
            "id, name, bal, status, note, encode(blob, 'hex'), payload::text, ts::text";
        let (src_minus_dst,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM (SELECT {projection} FROM {src} EXCEPT SELECT {projection} FROM {dst}) d"
        ))
        .fetch_one(&pg_pool)
        .await?;
        let (dst_minus_src,): (i64,) = sqlx::query_as(&format!(
            "SELECT COUNT(*) FROM (SELECT {projection} FROM {dst} EXCEPT SELECT {projection} FROM {src}) d"
        ))
        .fetch_one(&pg_pool)
        .await?;
        assert_eq!(
            (src_minus_dst, dst_minus_src),
            (0, 0),
            "round-trip mismatch: {src_minus_dst} rows in src not dst, {dst_minus_src} in dst not src"
        );

        // Identity continuation: a fresh insert into dst (no explicit id)
        // must not collide with the loaded ids (max was 3).
        sqlx::query(&format!("INSERT INTO {dst} (name) VALUES ('after')"))
            .execute(&pg_pool)
            .await?;
        let (max_id,): (i64,) = sqlx::query_as(&format!("SELECT MAX(id) FROM {dst}"))
            .fetch_one(&pg_pool)
            .await?;
        assert!(
            max_id > 3,
            "identity must continue past loaded rows, got {max_id}"
        );

        let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {src}, {dst}"))
            .execute(&pg_pool)
            .await;
        Ok(())
    }

    /// End-to-end DSQL → DSQL via the README workflow: `export` a real DSQL
    /// cluster to a `.sql`, **edit** the dump (the optional middle step the
    /// README highlights — here we widen `name` from `varchar(50)` to `text`,
    /// a column-type change DSQL cannot do with an in-place `ALTER COLUMN`),
    /// then `migrate` the edited file back into the same cluster under a new
    /// name. The edit keeps the column set identical, so the COPY block still
    /// matches the recreated table (the migrate loader requires the dump's COPY
    /// columns to exactly match the target — it has no per-column DEFAULT fill).
    /// Verifies the export reader, the edit-then-load path, and identity-counter
    /// continuation against a real cluster.
    ///
    /// `#[ignore]` — needs a real DSQL cluster + IAM (CI `e2e-dsql`).
    #[tokio::test]
    #[ignore]
    async fn export_then_migrate_dsql_to_dsql_with_edit() -> anyhow::Result<()> {
        let dsql_endpoint = std::env::var("LOADER_DSQL_E2E_ENDPOINT").map_err(|_| {
            anyhow::anyhow!("LOADER_DSQL_E2E_ENDPOINT must be set (<cluster>.dsql.<region>.on.aws)")
        })?;
        let dsql_region =
            std::env::var("LOADER_DSQL_E2E_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        anyhow::ensure!(
            !dsql_endpoint.is_empty(),
            "LOADER_DSQL_E2E_ENDPOINT is empty"
        );

        let dsql_pool = build_dsql_pool(
            PoolArgsBuilder::default()
                .endpoint(&dsql_endpoint)
                .region(&dsql_region)
                .username("admin")
                .build()?,
        )
        .await?;

        // UUID-suffixed names so parallel runs on a shared cluster don't collide.
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let src = format!("e2e_export_src_{suffix}");
        let dst = format!("e2e_export_dst_{suffix}");

        // SOURCE on the real cluster: identity PK (CACHE + counter
        // continuation), type-modifier columns, a default, and the data
        // matrix that stresses the COPY encode path (tab, NULL, bytea, jsonb).
        // One DDL per transaction (DSQL constraint), so issue them separately.
        dsql_pool
            .execute_query(&format!(
                "CREATE TABLE {src} (
                     id bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 1) PRIMARY KEY,
                     name varchar(50) NOT NULL,
                     bal numeric(10,2),
                     payload jsonb,
                     blob bytea
                 )"
            ))
            .await?;
        dsql_pool
            .execute_query(&format!(
                "INSERT INTO {src} (id, name, bal, payload, blob) VALUES
                     (1, 'plain',  12.50, '{{\"a\":1}}'::jsonb,  E'\\\\xDEADBEEF'),
                     (2, E'tab\\there', 0.00, '[1,2,null]'::jsonb, NULL),
                     (3, 'three', NULL, NULL, NULL)"
            ))
            .await?;

        // EXPORT the source cluster to a .sql (production IAM path, test_pool None).
        let dump_dir = tempfile::tempdir()?;
        let dump_path = dump_dir.path().join("export.sql");
        let report = run_export(ExportArgs {
            endpoint: dsql_endpoint.clone(),
            region: dsql_region.clone(),
            username: "admin".to_string(),
            schema: Some("public".to_string()),
            table: Some(src.clone()),
            output: Some(dump_path.clone()),
            test_pool: None,
        })
        .await?;
        assert_eq!(report.tables, vec![format!("public.{src}")]);

        // EDIT the dump — the README's optional middle step. Two edits:
        //   1. retarget src → dst so we can load alongside the source;
        //   2. widen `name` from `varchar(50)` to `text` — a column-type change
        //      DSQL refuses as an in-place `ALTER COLUMN`, which is the whole
        //      reason to export/edit/migrate. This keeps the column SET
        //      identical, so the COPY block still matches the recreated table
        //      (the migrate loader rejects a target column absent from the COPY
        //      block — it has no per-column DEFAULT fill).
        let dump_text = std::fs::read_to_string(&dump_path)?;
        let edited = dump_text
            .replace(
                &format!("\"public\".\"{src}\""),
                &format!("\"public\".\"{dst}\""),
            )
            .replace(&format!("{src}_id_seq"), &format!("{dst}_id_seq"))
            // Widen the type in place. format_type emits `character varying(50)`
            // for the source's varchar(50); rewrite that column to `text`.
            .replace("\"name\" character varying(50)", "\"name\" text");
        let edited_path = dump_dir.path().join("edited.sql");
        std::fs::write(&edited_path, &edited)?;
        // Guard: the widening edit must have actually matched (format_type's
        // exact output is the load-bearing assumption). A silent no-op replace
        // would make the rest of the test prove nothing.
        anyhow::ensure!(
            edited.contains("\"name\" text") && !edited.contains("character varying(50)"),
            "edit did not rewrite name's type; export DDL shape changed?\n{edited}"
        );

        // MIGRATE the edited dump into the destination table on the same cluster.
        let migrate_report = run_migrate(MigrateArgs {
            endpoint: dsql_endpoint.clone(),
            region: dsql_region.clone(),
            username: "admin".to_string(),
            source_uri: format!("file://{}", edited_path.display()),
            schema: "public".to_string(),
            dry_run: false,
            worker_count: 1,
            batch_size: 100,
            batch_concurrency: 1,
            chunk_size_bytes: 1024 * 1024,
            on_conflict: OnConflict::Error,
            verify: VerifyMode::Full,
            quiet: true,
            debug: false,
            test_pool: None,
        })
        .await?;
        assert!(
            migrate_report.ddl_unfixable.is_empty(),
            "exported+edited dump must migrate with zero unfixable, got: {:?}",
            migrate_report.ddl_unfixable
        );
        assert_eq!(migrate_report.tables.len(), 1);
        assert_eq!(migrate_report.tables[0].records_failed, 0);
        let v = migrate_report.tables[0]
            .verify
            .as_ref()
            .expect("verify=Full populates a verdict");
        assert_eq!(
            v.verdict,
            crate::runner::VerifyVerdict::Match,
            "destination must match source data; got {:?}",
            v.verdict
        );

        // The widening edit took effect: a name longer than the source's
        // varchar(50) now inserts cleanly, which the original type would have
        // rejected with "value too long". Proves the edited `text` type landed.
        let long_name = "x".repeat(100);
        dsql_pool
            .execute_query(&format!("INSERT INTO {dst} (name) VALUES ('{long_name}')"))
            .await
            .map_err(|e| {
                anyhow::anyhow!("widened `name text` column must accept a >50-char value: {e}")
            })?;

        // Identity continuation: an insert with no explicit id continues past
        // the loaded rows (max id was 3) rather than colliding.
        dsql_pool
            .execute_query(&format!("INSERT INTO {dst} (name) VALUES ('after')"))
            .await?;
        let (max_id,): (i64,) = sqlx::query_as(&format!("SELECT MAX(id) FROM {dst}"))
            .fetch_one(&dsql_pool)
            .await?;
        assert!(
            max_id > 3,
            "identity counter must continue past loaded rows, got {max_id}"
        );

        // Cleanup (best-effort; per-run CI cluster is torn down anyway).
        let _ = dsql_pool
            .execute_query(&format!("DROP TABLE IF EXISTS {src}"))
            .await;
        let _ = dsql_pool
            .execute_query(&format!("DROP TABLE IF EXISTS {dst}"))
            .await;
        Ok(())
    }
}
