//! Connection pooling with aurora-dsql-sqlx-connector for automatic IAM token refresh.
use anyhow::Result;
use async_stream::try_stream;
use aurora_dsql_sqlx_connector::{DsqlConnectOptionsBuilder, pool as dsql_pool};
use aws_config::Region;
use derive_builder::Builder;
use either::Either;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::{Database, Describe, Error, Execute, Executor, postgres::PgConnection};
use std::time::Duration;

/// Inner pool variants
#[derive(Debug, Clone)]
enum PoolInner {
    Postgres(sqlx::PgPool),
    #[cfg(test)]
    Sqlite(sqlx::SqlitePool),
}

/// Connection that can be either Postgres or SQLite
pub enum PoolConnection {
    Postgres(sqlx::pool::PoolConnection<sqlx::Postgres>),
    #[cfg(test)]
    Sqlite(sqlx::pool::PoolConnection<sqlx::Sqlite>),
}

impl std::ops::Deref for PoolConnection {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        match self {
            PoolConnection::Postgres(conn) => conn,
            #[cfg(test)]
            PoolConnection::Sqlite(_) => panic!("Cannot deref SQLite connection as PgConnection"),
        }
    }
}

impl std::ops::DerefMut for PoolConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            PoolConnection::Postgres(conn) => conn,
            #[cfg(test)]
            PoolConnection::Sqlite(_) => panic!("Cannot deref SQLite connection as PgConnection"),
        }
    }
}

// Wrap pool implementations so that we can implement the `sqlx::Executor` trait.
#[derive(Debug, Clone)]
pub struct Pool {
    inner: PoolInner,
}

#[derive(Builder)]
pub struct PoolArgs {
    #[builder(setter(into))]
    endpoint: String,
    #[builder(setter(into))]
    region: String,
    #[builder(setter(into))]
    username: String,
    #[builder(default = "100")]
    min_idle: u32,
    #[builder(default = "5000")]
    max_pool_size: u32,
}

pub async fn pool(args: PoolArgs) -> anyhow::Result<Pool> {
    let PoolArgs {
        endpoint,
        region,
        username,
        min_idle,
        max_pool_size,
    } = args;

    let pg_options = sqlx::postgres::PgConnectOptions::new()
        .host(&endpoint)
        .username(&username)
        .database("postgres");

    let mut builder = DsqlConnectOptionsBuilder::default();
    builder.pg_connect_options(pg_options);
    builder.orm_prefix(Some("dsql-loader".to_string()));
    builder.region(Some(Region::new(region)));

    let config = builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build DSQL connection options: {}", e))?;

    let pool_options = sqlx::postgres::PgPoolOptions::new()
        .min_connections(min_idle)
        .max_connections(max_pool_size)
        .max_lifetime(Duration::from_secs(60 * 55));

    let sqlx_pool = dsql_pool::connect_with(&config, pool_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create DSQL connection pool: {}", e))?;

    Ok(Pool {
        inner: PoolInner::Postgres(sqlx_pool),
    })
}

impl Pool {
    /// Create an in-memory SQLite pool for testing
    #[cfg(test)]
    pub async fn sqlite_in_memory() -> Result<Self, sqlx::Error> {
        let sqlite_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(10)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect("sqlite::memory:")
            .await?;

        Ok(Pool {
            inner: PoolInner::Sqlite(sqlite_pool),
        })
    }

    pub async fn acquire(&self) -> Result<PoolConnection, sqlx::Error> {
        match &self.inner {
            PoolInner::Postgres(pool) => {
                let conn = pool.acquire().await?;
                Ok(PoolConnection::Postgres(conn))
            }
            #[cfg(test)]
            PoolInner::Sqlite(pool) => {
                let conn = pool.acquire().await?;
                Ok(PoolConnection::Sqlite(conn))
            }
        }
    }

    /// Execute a query (for DDL like CREATE TABLE) - works for both Postgres and SQLite
    pub async fn execute_query(&self, sql: &str) -> Result<(), sqlx::Error> {
        match &self.inner {
            PoolInner::Postgres(pool) => {
                let mut conn = pool.acquire().await?;
                sqlx::query(sql).execute(&mut *conn).await?;
                Ok(())
            }
            #[cfg(test)]
            PoolInner::Sqlite(pool) => {
                sqlx::query(sql).execute(pool).await?;
                Ok(())
            }
        }
    }

    /// Fetch all rows from a query with bind parameters - works for both Postgres and SQLite
    /// Accepts a slice of bind values for flexible parameter binding
    ///
    /// The generic type T must implement FromRow for both Postgres and SQLite row types
    pub async fn fetch_all_with_binds<T>(
        &self,
        sql: &str,
        bind_values: &[&str],
    ) -> Result<Vec<T>, sqlx::Error>
    where
        T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>
            + for<'r> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow>
            + Send
            + Unpin,
    {
        match &self.inner {
            PoolInner::Postgres(pool) => {
                let mut conn = pool.acquire().await?;
                let mut query = sqlx::query_as::<_, T>(sql);
                for value in bind_values {
                    query = query.bind(*value);
                }
                query.fetch_all(&mut *conn).await
            }
            #[cfg(test)]
            PoolInner::Sqlite(pool) => {
                // Convert Postgres-style $1, $2, ... to SQLite-style ?, ?, ...
                let mut sqlite_sql = sql.to_string();
                for i in (1..=bind_values.len()).rev() {
                    sqlite_sql = sqlite_sql.replace(&format!("${}", i), "?");
                }
                let mut query = sqlx::query_as::<_, T>(&sqlite_sql);
                for value in bind_values {
                    query = query.bind(*value);
                }
                query.fetch_all(pool).await
            }
        }
    }

    /// Check if this pool is using PostgreSQL (returns false for SQLite)
    pub fn is_postgres(&self) -> bool {
        matches!(&self.inner, PoolInner::Postgres(_))
    }

    /// Get the table identifier for the current database type
    /// - PostgreSQL: Returns (schema_name, table_name) as-is for qualified names
    /// - SQLite: Simulates schemas by prefixing table name (e.g., "sales_orders" for sales.orders)
    pub fn table_identifier(&self, schema_name: &str, table_name: &str) -> (String, String) {
        if self.is_postgres() {
            (schema_name.to_string(), table_name.to_string())
        } else {
            // SQLite: simulate schemas with table name prefix
            let sqlite_table = if schema_name != "public" {
                format!("{}_{}", schema_name, table_name)
            } else {
                table_name.to_string()
            };
            ("public".to_string(), sqlite_table)
        }
    }

    /// Get a fully qualified table name for SQL statements
    /// - PostgreSQL: Returns "schema"."table" for non-public, or "table" for public
    /// - SQLite: Returns "table" (possibly prefixed like "sales_orders")
    pub fn qualified_table_name(&self, schema_name: &str, table_name: &str) -> String {
        let (schema, table) = self.table_identifier(schema_name, table_name);

        if self.is_postgres() && schema != "public" {
            format!("\"{}\".\"{}\"", schema, table)
        } else {
            format!("\"{}\"", table)
        }
    }

    /// Check if a table has unique constraints (primary key or unique index)
    pub async fn has_unique_constraints(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool, sqlx::Error> {
        match &self.inner {
            PoolInner::Postgres(pool) => {
                let mut conn = pool.acquire().await?;

                // Check for primary key or unique constraints in pg_constraint
                let constraint_sql = r#"
                    SELECT COUNT(*) as count
                    FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    JOIN pg_namespace n ON t.relnamespace = n.oid
                    WHERE n.nspname = $1
                      AND t.relname = $2
                      AND (c.contype = 'p' OR c.contype = 'u')
                "#;

                let result: (i64,) = sqlx::query_as(constraint_sql)
                    .bind(schema_name)
                    .bind(table_name)
                    .fetch_one(&mut *conn)
                    .await?;

                if result.0 > 0 {
                    return Ok(true);
                }

                // Also check for unique indexes in pg_indexes
                let index_sql = r#"
                    SELECT COUNT(*) as count
                    FROM pg_indexes
                    WHERE schemaname = $1
                      AND tablename = $2
                      AND indexdef LIKE '%UNIQUE INDEX%'
                "#;

                let index_result: (i64,) = sqlx::query_as(index_sql)
                    .bind(schema_name)
                    .bind(table_name)
                    .fetch_one(&mut *conn)
                    .await?;

                Ok(index_result.0 > 0)
            }
            #[cfg(test)]
            PoolInner::Sqlite(pool) => {
                // SQLite doesn't support schemas - use table_identifier for consistent naming
                let (_, sqlite_table) = self.table_identifier(schema_name, table_name);

                // For SQLite, check both table info and indexes
                // First check if table has primary key
                let pragma_sql = format!("PRAGMA table_info({})", sqlite_table);
                let rows: Vec<(i32, String, String, i32, Option<String>, i32)> =
                    sqlx::query_as(&pragma_sql).fetch_all(pool).await?;

                // Check if any column is a primary key
                let has_pk = rows.iter().any(|row| row.5 > 0);
                if has_pk {
                    return Ok(true);
                }

                // Check for unique indexes
                let index_sql = format!("PRAGMA index_list({})", sqlite_table);
                let indexes: Vec<(i32, String, i32, String, i32)> =
                    sqlx::query_as(&index_sql).fetch_all(pool).await?;

                // Column 2 (index 2) is the 'unique' flag (1 = unique, 0 = not unique)
                let has_unique_index = indexes.iter().any(|idx| idx.2 == 1);

                Ok(has_unique_index)
            }
        }
    }

    /// Get columns involved in unique constraints (primary key first, then first unique constraint)
    ///
    /// Returns columns from the first constraint found, prioritizing primary keys over
    /// unique constraints. If a primary key exists, returns all its columns. Otherwise,
    /// returns all columns from the first unique constraint (ordered by constraint OID).
    pub async fn get_unique_constraint_columns(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Vec<String>, sqlx::Error> {
        match &self.inner {
            PoolInner::Postgres(pool) => {
                let mut conn = pool.acquire().await?;

                // Query for constraint columns, prioritizing primary key over unique constraints
                // Returns all columns from the first matching constraint
                let sql = r#"
                    SELECT a.attname, c.contype::text
                    FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    JOIN pg_namespace n ON t.relnamespace = n.oid
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
                    WHERE n.nspname = $1
                      AND t.relname = $2
                      AND c.contype IN ('p', 'u')
                    ORDER BY
                      CASE WHEN c.contype = 'p' THEN 0 ELSE 1 END,
                      c.oid,
                      a.attnum
                "#;

                let rows: Vec<(String, String)> = sqlx::query_as(sql)
                    .bind(schema_name)
                    .bind(table_name)
                    .fetch_all(&mut *conn)
                    .await?;

                // Take columns from the first constraint only (either primary key or first unique)
                if rows.is_empty() {
                    return Ok(Vec::new());
                }

                let first_constraint_type = rows[0].1.clone();
                let columns: Vec<String> = rows
                    .into_iter()
                    .take_while(|(_, contype)| contype == &first_constraint_type)
                    .map(|(col, _)| col)
                    .collect();

                Ok(columns)
            }
            #[cfg(test)]
            PoolInner::Sqlite(pool) => {
                // Query SQLite's pragma_table_info for constraint information
                let mut conn = pool.acquire().await?;

                // First, check for primary key columns in table_info
                let pk_sql = format!(
                    "SELECT name FROM pragma_table_info('{}') WHERE pk > 0 ORDER BY pk",
                    table_name
                );
                let pk_columns: Vec<(String,)> =
                    sqlx::query_as(&pk_sql).fetch_all(&mut *conn).await?;

                if !pk_columns.is_empty() {
                    return Ok(pk_columns.into_iter().map(|(col,)| col).collect());
                }

                // If no primary key, look for unique indexes
                let index_sql = format!(
                    "SELECT name FROM pragma_index_list('{}') WHERE [unique] = 1",
                    table_name
                );
                let indexes: Vec<(String,)> =
                    sqlx::query_as(&index_sql).fetch_all(&mut *conn).await?;

                // Get columns from first unique index
                if let Some((index_name,)) = indexes.first() {
                    let cols_sql = format!("SELECT name FROM pragma_index_info('{}')", index_name);
                    let cols: Vec<(String,)> =
                        sqlx::query_as(&cols_sql).fetch_all(&mut *conn).await?;
                    return Ok(cols.into_iter().map(|(col,)| col).collect());
                }

                Ok(Vec::new())
            }
        }
    }
}

impl Executor<'_> for &'_ Pool {
    type Database = sqlx::Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<sqlx::postgres::PgQueryResult, sqlx::postgres::PgRow>, Error>>
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        let pool = match &self.inner {
            PoolInner::Postgres(pool) => pool.clone(),
            #[cfg(test)]
            _ => panic!("Executor trait not supported for non-Postgres pools"),
        };

        Box::pin(try_stream! {
            let mut conn = pool.acquire().await?;
            let mut stream = conn.fetch_many(query);
            while let Some(item) = stream.try_next().await? {
                yield item;
            }
        })
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<sqlx::postgres::PgRow>, Error>>
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        let pool = match &self.inner {
            PoolInner::Postgres(pool) => pool.clone(),
            #[cfg(test)]
            _ => panic!("Executor trait not supported for non-Postgres pools"),
        };

        Box::pin(async move {
            let mut conn = pool.acquire().await?;
            conn.fetch_optional(query).await
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<sqlx::postgres::PgStatement<'q>, Error>> {
        let pool = match &self.inner {
            PoolInner::Postgres(pool) => pool.clone(),
            #[cfg(test)]
            _ => panic!("Executor trait not supported for non-Postgres pools"),
        };

        Box::pin(async move {
            let mut conn = pool.acquire().await?;
            conn.prepare_with(sql, parameters).await
        })
    }

    #[doc(hidden)]
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<sqlx::postgres::Postgres>, Error>> {
        let pool = match &self.inner {
            PoolInner::Postgres(pool) => pool.clone(),
            #[cfg(test)]
            _ => panic!("Executor trait not supported for non-Postgres pools"),
        };

        Box::pin(async move {
            let mut conn = pool.acquire().await?;
            conn.describe(sql).await
        })
    }
}
