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
            manifest::{ChunkStatus, LocalManifestStorage},
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
}
