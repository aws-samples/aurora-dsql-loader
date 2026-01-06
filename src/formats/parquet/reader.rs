//! Parquet file reader implementation.

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::sync::Arc;

use crate::formats::reader::{FileMetadata, FileReader, Partition, PartitionData};
use crate::io::ByteReader;

use super::adapter::ByteReaderAdapter;
use super::conversion::record_batch_to_records;

/// Information about a row group cached from Parquet metadata
#[derive(Debug, Clone)]
struct RowGroupInfo {
    index: usize,
    num_rows: u64,
    total_byte_size: u64,
}

/// Parquet file reader that works with any ByteReader implementation
pub struct GenericParquetReader<R: ByteReader> {
    reader: Arc<R>,
    row_groups: Vec<RowGroupInfo>,
}

impl<R: ByteReader + Clone + 'static> GenericParquetReader<R> {
    /// Create a new ParquetReader by reading the file metadata
    pub async fn new(reader: R) -> Result<Self> {
        // Create adapter for Arrow (using a clone of the reader for metadata reading)
        let adapter = ByteReaderAdapter::new(reader.clone())
            .await
            .context("Failed to create ByteReader adapter")?;

        // Read Parquet metadata from footer
        let builder = ParquetRecordBatchStreamBuilder::new(adapter)
            .await
            .context("Failed to read Parquet metadata")?;

        let metadata = builder.metadata().clone();

        // Extract row group information
        let mut row_groups = Vec::new();
        for (index, rg) in metadata.row_groups().iter().enumerate() {
            row_groups.push(RowGroupInfo {
                index,
                num_rows: rg.num_rows() as u64,
                total_byte_size: rg.total_byte_size() as u64,
            });
        }

        Ok(Self {
            reader: Arc::new(reader),
            row_groups,
        })
    }

    /// Read specific row groups and convert to Records
    async fn read_row_groups(&self, row_group_indices: &[usize]) -> Result<PartitionData> {
        // Create a new adapter for this read (clone the reader from Arc)
        let reader_clone = (*self.reader).clone();
        let adapter = ByteReaderAdapter::new(reader_clone)
            .await
            .context("Failed to create ByteReader adapter")?;

        // Build stream with only selected row groups
        let builder = ParquetRecordBatchStreamBuilder::new(adapter)
            .await
            .context("Failed to create Parquet stream builder")?;

        let stream = builder
            .with_row_groups(row_group_indices.to_vec())
            .build()
            .context("Failed to build Parquet stream")?;

        // Read all batches from stream and convert to Records
        let mut all_records = Vec::new();
        let mut bytes_read = 0u64;

        let mut stream = Box::pin(stream);
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to read record batch")?;

            // Estimate bytes read (Arrow doesn't expose exact byte count easily)
            bytes_read += estimate_batch_size(&batch);

            // Convert batch to records
            let records = record_batch_to_records(&batch)
                .context("Failed to convert RecordBatch to Records")?;
            all_records.extend(records);
        }

        Ok(PartitionData {
            records: all_records,
            bytes_read,
        })
    }
}

#[async_trait]
impl<R: ByteReader + Clone + 'static> FileReader for GenericParquetReader<R> {
    async fn metadata(&self) -> Result<FileMetadata> {
        let file_size = self.reader.size().await?;

        // Calculate total rows from row groups
        let total_rows: u64 = self.row_groups.iter().map(|rg| rg.num_rows).sum();

        Ok(FileMetadata {
            file_size_bytes: file_size,
            estimated_rows: Some(total_rows),
        })
    }

    async fn create_partitions(&self, target_size: u64) -> Result<Vec<Partition>> {
        let mut partitions = Vec::new();
        let mut partition_id = 0u32;

        let mut current_row_groups = Vec::new();
        let mut current_size = 0u64;
        let mut current_rows = 0u64;

        for rg in &self.row_groups {
            // Check if adding this row group would exceed target
            if !current_row_groups.is_empty() && current_size + rg.total_byte_size > target_size {
                // Finalize current partition
                // Use row group indices encoded in offsets
                let start_rg = current_row_groups[0];
                let end_rg = current_row_groups[current_row_groups.len() - 1] + 1;

                partitions.push(Partition {
                    partition_id,
                    start_offset: start_rg as u64,
                    end_offset: end_rg as u64,
                    estimated_rows: Some(current_rows),
                });

                partition_id += 1;
                current_row_groups.clear();
                current_size = 0;
                current_rows = 0;
            }

            current_row_groups.push(rg.index);
            current_size += rg.total_byte_size;
            current_rows += rg.num_rows;
        }

        // Add final partition if any row groups remain
        if !current_row_groups.is_empty() {
            let start_rg = current_row_groups[0];
            let end_rg = current_row_groups[current_row_groups.len() - 1] + 1;

            partitions.push(Partition {
                partition_id,
                start_offset: start_rg as u64,
                end_offset: end_rg as u64,
                estimated_rows: Some(current_rows),
            });
        }

        Ok(partitions)
    }

    async fn read_partition(&self, partition: &Partition) -> Result<PartitionData> {
        // Map partition offsets to row group indices
        // Offsets encode row group indices (start_offset = first RG, end_offset = last RG + 1)
        let start_rg = partition.start_offset as usize;
        let end_rg = partition.end_offset as usize;

        let row_group_indices: Vec<usize> = (start_rg..end_rg).collect();

        if row_group_indices.is_empty() {
            return Err(anyhow!(
                "No row groups found for partition {}",
                partition.partition_id
            ));
        }

        self.read_row_groups(&row_group_indices).await
    }
}

/// Estimate byte size of a RecordBatch (approximation)
fn estimate_batch_size(batch: &arrow::record_batch::RecordBatch) -> u64 {
    // Sum byte sizes of all arrays
    let mut size = 0u64;
    for column in batch.columns() {
        size += column.get_array_memory_size() as u64;
    }
    size
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::ByteReader;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    /// Mock ByteReader for local files (for testing)
    #[derive(Clone)]
    struct LocalFileReader {
        path: std::path::PathBuf,
    }

    #[async_trait]
    impl ByteReader for LocalFileReader {
        async fn size(&self) -> Result<u64> {
            let metadata = tokio::fs::metadata(&self.path).await?;
            Ok(metadata.len())
        }

        async fn read_range(&self, start: u64, end: u64) -> Result<Vec<u8>> {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            let mut file = tokio::fs::File::open(&self.path).await?;
            file.seek(std::io::SeekFrom::Start(start)).await?;
            let size = (end - start) as usize;
            let mut buffer = vec![0u8; size];
            file.read_exact(&mut buffer).await?;
            Ok(buffer)
        }
    }

    /// Helper to create a test Parquet file
    fn create_test_parquet_file(num_rows: usize, row_group_size: usize) -> NamedTempFile {
        let temp_file = NamedTempFile::new().unwrap();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]);

        let props = WriterProperties::builder()
            .set_max_row_group_size(row_group_size)
            .build();

        let file = std::fs::File::create(temp_file.path()).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).unwrap();

        // Write data in batches
        let batch_size = 100;
        for start in (0..num_rows).step_by(batch_size) {
            let end = std::cmp::min(start + batch_size, num_rows);

            let id_array = Int32Array::from_iter_values(start as i32..end as i32);
            let name_array =
                StringArray::from_iter_values((start..end).map(|i| format!("name_{}", i)));
            let value_array = Float64Array::from_iter_values((start..end).map(|i| i as f64 * 1.5));

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
        }

        writer.close().unwrap();
        temp_file
    }

    #[tokio::test]
    async fn test_parquet_reader_metadata() {
        let temp_file = create_test_parquet_file(100, 10000);
        let reader = LocalFileReader {
            path: temp_file.path().to_path_buf(),
        };

        let parquet_reader = GenericParquetReader::new(reader).await.unwrap();
        let metadata = parquet_reader.metadata().await.unwrap();

        assert!(metadata.file_size_bytes > 0);
        assert_eq!(metadata.estimated_rows, Some(100));
    }

    #[tokio::test]
    async fn test_parquet_reader_single_partition() {
        let temp_file = create_test_parquet_file(50, 10000);
        let reader = LocalFileReader {
            path: temp_file.path().to_path_buf(),
        };

        let parquet_reader = GenericParquetReader::new(reader).await.unwrap();

        // Large target size should create single partition
        let partitions = parquet_reader.create_partitions(1_000_000).await.unwrap();
        assert_eq!(partitions.len(), 1);

        let data = parquet_reader.read_partition(&partitions[0]).await.unwrap();
        assert_eq!(data.records.len(), 50);
        assert!(data.bytes_read > 0);

        // Check first record
        assert_eq!(data.records[0].fields[0], "0");
        assert_eq!(data.records[0].fields[1], "name_0");
        assert_eq!(data.records[0].fields[2], "0");
    }

    #[tokio::test]
    async fn test_parquet_reader_multiple_partitions() {
        // Create file with multiple row groups
        let temp_file = create_test_parquet_file(1000, 100); // ~10 row groups

        let reader = LocalFileReader {
            path: temp_file.path().to_path_buf(),
        };

        let parquet_reader = GenericParquetReader::new(reader).await.unwrap();

        // Small partition size should create multiple partitions
        let partitions = parquet_reader.create_partitions(5000).await.unwrap();
        assert!(partitions.len() > 1);

        // Read all partitions and verify total rows
        let mut total_rows = 0;
        for partition in &partitions {
            let data = parquet_reader.read_partition(partition).await.unwrap();
            total_rows += data.records.len();
        }

        assert_eq!(total_rows, 1000);
    }
}
