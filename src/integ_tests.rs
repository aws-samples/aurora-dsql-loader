//! Integration tests for coordinator and worker behavior
//!
//! These tests use SQLite in-memory databases and real CSV files to test
//! end to end scenarios of the loader.

#[cfg(test)]
mod tests {
    use crate::{
        coordination::coordinator::LoadResult,
        coordination::manifest::LocalManifestStorage,
        coordination::{Coordinator, DsqlConfig, FileFormat, LoadConfig},
        db::pool::PoolConnection,
        db::{Pool, SchemaInferrer},
        formats::delimited::reader::GenericDelimitedReader,
        formats::{DelimitedConfig, FileReader},
        io::LocalFileByteReader,
        runner::{Format, LoadArgs},
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
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
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
        partition_size: u64,
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

        let config = LoadConfig {
            source_uri: csv_path.to_string(),
            target_table: table_name.to_string(),
            dsql_config: DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            },
            worker_count,
            partition_size_bytes: partition_size,
            batch_size: 10,
            batch_concurrency: 2,
            create_table_if_missing: true,
            file_format: FileFormat::Csv(DelimitedConfig::csv()),
            column_mappings: std::collections::HashMap::new(),
            quiet: true,
        };

        coordinator.run_load(config).await.unwrap()
    }

    /// Helper to query table row count
    async fn get_table_count(pool: &Pool, table_name: &str) -> i64 {
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
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

        assert!(result.partitions_processed > 0);
        assert_eq!(result.records_loaded, 100);
        assert_eq!(result.records_failed, 0);
    }

    #[tokio::test]
    async fn test_multiple_workers_and_partition_distribution() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = create_test_csv(&temp_dir, "test.csv", 500).await;
        let pool =
            setup_sqlite_table("test_table", "id TEXT, name TEXT, value TEXT, amount TEXT").await;

        let result = run_csv_load(&pool, "test_table", &csv_path, 4, 800).await;

        assert!(
            result.partitions_processed > 1,
            "Should create multiple partitions"
        );
        assert_eq!(result.records_loaded, 500);
        assert_eq!(result.records_failed, 0);

        // Verify work was distributed across multiple workers
        let unique_workers: std::collections::HashSet<_> = result
            .partition_results
            .iter()
            .map(|r| &r.worker_id)
            .collect();
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
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
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

        let config = LoadConfig {
            source_uri: parquet_path_str,
            target_table: "test_parquet".to_string(),
            dsql_config: DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            },
            worker_count: 2,
            partition_size_bytes: 500,
            batch_size: 20,
            batch_concurrency: 2,
            create_table_if_missing: false, // Table already created
            file_format: FileFormat::Parquet(ParquetConfig::default()),
            column_mappings: std::collections::HashMap::new(),
            quiet: true,
        };

        let result = coordinator.run_load(config).await.unwrap();

        // Verify results
        assert!(result.partitions_processed > 0);
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
        assert!(pool.has_unique_constraints("test_unique").await.unwrap());

        let result = run_csv_load(&pool, "test_unique", &csv_path, 1, 1000).await;

        assert_eq!(result.records_failed, 0, "Should have no failed records");
        assert_eq!(
            get_table_count(&pool, "test_unique").await,
            3,
            "Should have exactly 3 unique records"
        );

        // Verify first occurrence of duplicate key is kept
        if let Ok(mut conn) = pool.acquire().await
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
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
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
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
            format: Format::Csv,
            worker_count: 2,
            partition_size_bytes: 1000,
            batch_size: 10,
            batch_concurrency: 2,
            create_table_if_missing: false, // Table already created
            manifest_dir: None,
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            test_pool: Some(pool.clone()),
        };

        let result = crate::runner::run_load(args).await.unwrap();

        assert!(result.partitions_processed > 0);
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
            && let PoolConnection::Sqlite(ref mut sqlite_conn) = conn {
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
            format: Format::Parquet,
            worker_count: 2,
            partition_size_bytes: 500,
            batch_size: 20,
            batch_concurrency: 2,
            create_table_if_missing: false,
            manifest_dir: None,
            quiet: true,
            column_mappings: std::collections::HashMap::new(),
            test_pool: Some(pool.clone()),
        };

        let result = crate::runner::run_load(args).await.unwrap();

        // Verify results
        assert!(result.partitions_processed > 0);
        assert_eq!(result.records_loaded, 50);
        assert_eq!(result.records_failed, 0);
        assert_eq!(get_table_count(&pool, "runner_parquet").await, 50);
    }

    #[tokio::test]
    async fn test_failed_record_error_reporting() {
        // This test verifies that:
        // 1. Failed records are tracked accurately
        // 2. Error messages contain helpful batch context
        // 3. Partition results show correct status and counts

        let temp_dir = TempDir::new().unwrap();

        // Create CSV with 4 columns
        let csv_path = create_csv_with_content(
            &temp_dir,
            "extra_columns.csv",
            &[
                "id,name,value,extra\n",
                "1,Alice,100,foo\n",
                "2,Bob,200,bar\n",
                "3,Charlie,300,baz\n",
            ],
        )
        .await;

        // Create table with only 3 columns - mismatch will cause failure
        let pool = setup_sqlite_table(
            "test_column_mismatch",
            "id INTEGER, name TEXT, value INTEGER",
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

        let config = LoadConfig {
            source_uri: csv_path.to_string(),
            target_table: "test_column_mismatch".to_string(),
            dsql_config: DsqlConfig {
                endpoint: "test".to_string(),
                region: "us-west-2".to_string(),
                username: "test".to_string(),
            },
            worker_count: 1,
            partition_size_bytes: 1000,
            batch_size: 3, // All 3 records in one batch
            batch_concurrency: 1,
            create_table_if_missing: false,
            file_format: FileFormat::Csv(DelimitedConfig::csv()),
            column_mappings: std::collections::HashMap::new(),
            quiet: true,
        };

        let result = coordinator.run_load(config).await.unwrap();

        // Verify that failures were properly tracked
        assert_eq!(result.partitions_processed, 1, "Should process 1 partition");
        assert_eq!(
            result.records_loaded, 0,
            "Should load 0 records (batch failed)"
        );
        assert_eq!(
            result.records_failed, 3,
            "Should report 3 failed records (entire batch)"
        );

        // Verify no data was actually inserted
        assert_eq!(get_table_count(&pool, "test_column_mismatch").await, 0);

        // Verify partition result contains detailed error information
        let partition_result = &result.partition_results[0];
        assert_eq!(partition_result.status, "failed");
        assert_eq!(
            partition_result.records_failed, 3,
            "Partition should show 3 failed records"
        );
        assert_eq!(partition_result.records_loaded, 0);
        assert!(
            !partition_result.errors.is_empty(),
            "Should have error records"
        );

        // Check that error message contains helpful batch context
        let error_msg = &partition_result.errors[0].error_message;
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
}
