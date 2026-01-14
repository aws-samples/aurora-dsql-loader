use anyhow::{Result, anyhow};
use std::path::PathBuf;
use url::Url;

/// Represents a parsed source URI
#[derive(Debug, Clone)]
pub enum SourceUri {
    Local(PathBuf),
    S3 { bucket: String, key: String },
}

impl SourceUri {
    /// Parse a URI string into a SourceUri
    pub fn parse(uri: &str) -> Result<Self> {
        // Try parsing as URL first
        if let Ok(url) = Url::parse(uri) {
            match url.scheme() {
                "s3" => {
                    let bucket = url
                        .host_str()
                        .ok_or_else(|| anyhow!("S3 URI missing bucket: {}", uri))?
                        .to_string();

                    // Remove leading '/' from path
                    let key = url.path().trim_start_matches('/').to_string();

                    if key.is_empty() {
                        return Err(anyhow!("S3 URI missing key: {}", uri));
                    }

                    Ok(SourceUri::S3 { bucket, key })
                }
                "file" => {
                    let path = url
                        .to_file_path()
                        .map_err(|_| anyhow!("Invalid file:// URI: {}", uri))?;
                    Ok(SourceUri::Local(path))
                }
                scheme => Err(anyhow!("Unsupported URI scheme: {}", scheme)),
            }
        } else {
            // Treat as local file path
            Ok(SourceUri::Local(PathBuf::from(uri)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_uri() {
        let uri = SourceUri::parse("s3://my-bucket/path/to/file.csv").unwrap();
        match uri {
            SourceUri::S3 { bucket, key } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(key, "path/to/file.csv");
            }
            _ => panic!("Expected S3 URI"),
        }
    }

    #[test]
    fn test_parse_s3_uri_simple() {
        let uri = SourceUri::parse("s3://bucket/file.csv").unwrap();
        match uri {
            SourceUri::S3 { bucket, key } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "file.csv");
            }
            _ => panic!("Expected S3 URI"),
        }
    }

    #[test]
    fn test_parse_local_path() {
        let uri = SourceUri::parse("/data/file.csv").unwrap();
        assert!(matches!(uri, SourceUri::Local(_)));
    }

    #[test]
    fn test_parse_relative_path() {
        let uri = SourceUri::parse("data/file.csv").unwrap();
        assert!(matches!(uri, SourceUri::Local(_)));
    }

    #[test]
    fn test_parse_file_uri() {
        let uri = SourceUri::parse("file:///data/file.csv").unwrap();
        assert!(matches!(uri, SourceUri::Local(_)));
    }

    #[test]
    fn test_parse_s3_missing_bucket() {
        let result = SourceUri::parse("s3:///file.csv");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_missing_key() {
        let result = SourceUri::parse("s3://bucket/");
        assert!(result.is_err());
    }

    #[test]
    fn test_is_s3() {
        let s3_uri = SourceUri::parse("s3://bucket/key").unwrap();
        assert!(matches!(s3_uri, SourceUri::S3 { bucket: _, key: _ }));

        let local_uri = SourceUri::parse("/data/file.csv").unwrap();
        assert!(matches!(local_uri, SourceUri::Local(_)));
    }
}
