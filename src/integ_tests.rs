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
            manifest::{ChunkResultFile, ChunkStatus, LocalManifestStorage},
        },
        db::{Pool, SchemaInferrer, pool::PoolConnection},
        formats::{DelimitedConfig, FileReader, delimited::reader::GenericDelimitedReader},
        io::LocalFileByteReader,
        runner::{Format, LoadArgs, run_load},
    };
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::fs::File;
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

    /// Helper to run a basic CSV load test with defaults
    async fn run_csv_load(
        pool: &Pool,
        table_name: &str,
        csv_path: &str,
        worker_count: usize,
        chunk_size: u64,
    ) -> LoadResult {
        let byte_reader = LocalFileByteReader::new(csv_path);
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::default(),
        ));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
            .quiet(true)
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
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::tsv(),
        ));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .file_format(FileFormat::Tsv(DelimitedConfig::tsv()))
            .column_mappings(std::collections::HashMap::new())
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
        let unique_workers: std::collections::HashSet<_> =
            result.chunk_results.iter().map(|r| &r.worker_id).collect();
        assert!(
            unique_workers.len() >= 2,
            "Expected at least 2 workers, got {}",
            unique_workers.len()
        );
    }

    #[tokio::test]
    async fn test_parquet_load() {
        use crate::coordination::manifest::ParquetConfig;
        use crate::formats::FileReader;
        use crate::formats::parquet::GenericParquetReader;
        use arrow::array::*;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let temp_dir = TempDir::new().unwrap();

        // Create a test Parquet file
        let parquet_path = temp_dir.path().join("test.parquet");
        let schema = Schema::new(vec![
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
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .column_mappings(std::collections::HashMap::new())
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
            manifest_dir: None,
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
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
        use arrow::array::*;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let temp_dir = TempDir::new().unwrap();

        // Create a test Parquet file
        let parquet_path = temp_dir.path().join("runner_test.parquet");
        let schema = Schema::new(vec![
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
            manifest_dir: None,
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
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
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::default(),
        ));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
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
            && let crate::db::pool::PoolConnection::Sqlite(ref mut sqlite_conn) = conn
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
            manifest_dir: None, // Don't specify manifest dir - this is key
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
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
            && let crate::db::pool::PoolConnection::Sqlite(ref mut sqlite_conn) = conn
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
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::default(),
        ));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
            .quiet(true)
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
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::csv(),
        ));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
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
            manifest_dir: None,
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
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
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::csv(),
        ));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
            .quiet(true)
            .build()
            .unwrap();

        let result = coordinator.run_load(&config).await.unwrap();

        assert_eq!(result.records_loaded, 10);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_resume_incomplete_job() {
        use crate::db::pool::PoolConnection;
        use tempfile::TempDir;
        use tokio::fs;

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
            manifest_dir: Some(manifest_path.clone()),
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
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
            manifest_dir: Some(manifest_path.clone()),
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            resume_job_id: Some(job_id.clone()),
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
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
        use crate::db::pool::PoolConnection;
        use tempfile::TempDir;
        use tokio::fs;

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
            manifest_dir: Some(manifest_path.clone()),
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            resume_job_id: None,
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
            test_pool: Some(pool.clone()),
        };

        let result1 = run_load(args1).await.unwrap();
        let job_id = result1.job_id.clone();

        // Verify first load had some successes and some failures
        println!(
            "First load: loaded={}, failed={}",
            result1.records_loaded, result1.records_failed
        );
        assert!(result1.records_loaded < 40, "Should have some failures");
        assert!(result1.records_failed > 0, "Should have failures");

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
            manifest_dir: Some(manifest_path.clone()),
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            resume_job_id: Some(job_id.clone()),
            on_conflict: crate::coordination::manifest::OnConflict::DoNothing,
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
        use crate::coordination::manifest::OnConflict;

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
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::csv(),
        ));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
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
        let file_reader2: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader2,
            DelimitedConfig::csv(),
        ));

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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
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
        use crate::coordination::manifest::OnConflict;

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
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::csv(),
        ));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
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
        let file_reader2: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader2,
            DelimitedConfig::csv(),
        ));

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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
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
        use crate::coordination::manifest::OnConflict;

        let temp_dir = TempDir::new().unwrap();
        let csv_path =
            create_csv_with_content(&temp_dir, "test.csv", &["id,name\n", "1,Alice\n"]).await;

        // Table WITHOUT any unique constraints
        let pool = setup_sqlite_table("test_no_constraints", "id TEXT, name TEXT").await;

        let byte_reader = LocalFileByteReader::new(&csv_path);
        let file_reader: Arc<dyn FileReader> = Arc::new(GenericDelimitedReader::new(
            byte_reader,
            DelimitedConfig::csv(),
        ));

        let manifest_dir = TempDir::new().unwrap();
        let manifest_storage: Arc<dyn crate::coordination::manifest::ManifestStorage> =
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
            .file_format(FileFormat::Csv(DelimitedConfig::csv()))
            .column_mappings(std::collections::HashMap::new())
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
}
