//! Adapter between bb8 and the sqlx::Postgres driver.
use anyhow::{Context, Result, anyhow};
use async_stream::try_stream;
use aws_config::{BehaviorVersion, Region, default_provider::credentials::default_provider};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_dsql::auth_token::{AuthTokenGenerator, Config};
use aws_types::SdkConfig;
use derive_builder::Builder;
use either::Either;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use sqlx::{ConnectOptions, postgres::PgConnectOptions};
use sqlx::{Database, Describe, Error, Execute, Executor, postgres::PgConnection};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::config::{CONNECT_TIMEOUT, PING_TIMEOUT, TOKEN_VALIDITY_DURATION};

pub type Bb8Connection<'a> = bb8::PooledConnection<'a, ConnectionManager>;

/// Inner pool variants
#[derive(Debug, Clone)]
enum PoolInner {
    Postgres(bb8::Pool<ConnectionManager>),
    #[cfg(test)]
    Sqlite(sqlx::SqlitePool),
}

/// Connection that can be either Postgres or SQLite
pub enum PoolConnection {
    Postgres(Bb8Connection<'static>),
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
    region: Region,
    #[builder(setter(into))]
    username: String,
    #[builder(default = "2000")]
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
    let connect_options = sqlx::postgres::PgConnectOptions::new()
        .host(&endpoint)
        .username(&username)
        .database("postgres")
        .ssl_mode(sqlx::postgres::PgSslMode::VerifyFull)
        .to_owned();

    let auth = DsqlIamDbAuthTokenProvider::new(
        &endpoint,
        region.clone(),
        TokenType::from(username.as_ref()),
        SharedCredentialsProvider::new(default_provider().await),
    )
    .await
    .context("Failed to set up IAM authentication for DSQL cluster")?;

    let connector = Postgres::create_with_token_refresh(auth, connect_options).await?;
    let conn_manager = ConnectionManager::new(connector);

    let bb8_pool = bb8::Builder::new()
        .min_idle(min_idle)
        .max_size(max_pool_size)
        .max_lifetime(Duration::from_secs(60 * 55))
        .build(conn_manager)
        .await
        .context("Failed to create connection pool")?;

    Ok(Pool {
        inner: PoolInner::Postgres(bb8_pool),
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
                let conn = pool.get_owned().await.map_err(|e| match e {
                    bb8::RunError::User(e) => e,
                    bb8::RunError::TimedOut => sqlx::Error::PoolTimedOut,
                })?;
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
                let mut conn = pool.get().await.map_err(|e| match e {
                    bb8::RunError::User(e) => e,
                    bb8::RunError::TimedOut => sqlx::Error::PoolTimedOut,
                })?;
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

    /// Fetch all rows from a query with a single bind parameter - works for both Postgres and SQLite
    pub async fn fetch_all_with_bind(
        &self,
        sql: &str,
        bind_value: &str,
    ) -> Result<Vec<(String, String, String)>, sqlx::Error> {
        match &self.inner {
            PoolInner::Postgres(pool) => {
                let mut conn = pool.get().await.map_err(|e| match e {
                    bb8::RunError::User(e) => e,
                    bb8::RunError::TimedOut => sqlx::Error::PoolTimedOut,
                })?;
                let rows = sqlx::query_as::<_, (String, String, String)>(sql)
                    .bind(bind_value)
                    .fetch_all(&mut *conn)
                    .await?;
                Ok(rows)
            }
            #[cfg(test)]
            PoolInner::Sqlite(pool) => {
                // Convert Postgres-style $1 to SQLite-style ?
                let sqlite_sql = sql.replace("$1", "?");
                let rows = sqlx::query_as::<_, (String, String, String)>(&sqlite_sql)
                    .bind(bind_value)
                    .fetch_all(pool)
                    .await?;
                Ok(rows)
            }
        }
    }

    /// Check if this pool is using PostgreSQL (returns false for SQLite)
    pub fn is_postgres(&self) -> bool {
        matches!(&self.inner, PoolInner::Postgres(_))
    }

    /// Check if a table has unique constraints (primary key or unique index)
    pub async fn has_unique_constraints(&self, table_name: &str) -> Result<bool, sqlx::Error> {
        match &self.inner {
            PoolInner::Postgres(pool) => {
                let mut conn = pool.get().await.map_err(|e| match e {
                    bb8::RunError::User(e) => e,
                    bb8::RunError::TimedOut => sqlx::Error::PoolTimedOut,
                })?;

                // Check for primary key or unique constraints in pg_constraint
                let constraint_sql = r#"
                    SELECT COUNT(*) as count
                    FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    JOIN pg_namespace n ON t.relnamespace = n.oid
                    WHERE t.relname = $1
                    AND (c.contype = 'p' OR c.contype = 'u')
                "#;

                let result: (i64,) = sqlx::query_as(constraint_sql)
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
                    WHERE tablename = $1
                    AND indexdef LIKE '%UNIQUE INDEX%'
                "#;

                let index_result: (i64,) = sqlx::query_as(index_sql)
                    .bind(table_name)
                    .fetch_one(&mut *conn)
                    .await?;

                Ok(index_result.0 > 0)
            }
            #[cfg(test)]
            PoolInner::Sqlite(pool) => {
                // For SQLite, check both table info and indexes
                // First check if table has primary key
                let pragma_sql = format!("PRAGMA table_info({})", table_name);
                let rows: Vec<(i32, String, String, i32, Option<String>, i32)> =
                    sqlx::query_as(&pragma_sql).fetch_all(pool).await?;

                // Check if any column is a primary key
                let has_pk = rows.iter().any(|row| row.5 > 0);
                if has_pk {
                    return Ok(true);
                }

                // Check for unique indexes
                let index_sql = format!("PRAGMA index_list({})", table_name);
                let indexes: Vec<(i32, String, i32, String, i32)> =
                    sqlx::query_as(&index_sql).fetch_all(pool).await?;

                // Column 2 (index 2) is the 'unique' flag (1 = unique, 0 = not unique)
                let has_unique_index = indexes.iter().any(|idx| idx.2 == 1);

                Ok(has_unique_index)
            }
        }
    }
}

// Wrap `Arc<connector::Postgres>` so that we can implement the bb8::ManageConnection trait.
pub struct ConnectionManager {
    connector: Arc<Postgres>,
}

impl ConnectionManager {
    /// Create a new `ConnectionManager` with the specified connect options.
    pub fn new(connector: Arc<Postgres>) -> Self {
        Self { connector }
    }
}

impl bb8::ManageConnection for ConnectionManager {
    type Connection = PgConnection;
    type Error = sqlx::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.connector.connect().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        tokio::time::timeout(PING_TIMEOUT, sqlx::Connection::ping(conn))
            .await
            // Convert tokio timeouts into sqlx pool timeouts. bb8 will retry a different connection on ping failure.
            .map_err(|_| sqlx::Error::PoolTimedOut)
            // Make sure that we also look at the actual ping result
            .and_then(|result| result)?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        // sqlx::PgConnection provides no non-async way to check for closed/broken connections.
        false
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
            let mut conn = pool.get().await.map_err(|e| match e {
                bb8::RunError::User(e) => e,
                bb8::RunError::TimedOut => sqlx::Error::PoolTimedOut,
            })?;
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
            let mut conn = pool.get().await.map_err(|e| match e {
                bb8::RunError::User(e) => e,
                bb8::RunError::TimedOut => sqlx::Error::PoolTimedOut,
            })?;
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
            let mut conn = pool.get().await.map_err(|e| match e {
                bb8::RunError::User(e) => e,
                bb8::RunError::TimedOut => sqlx::Error::PoolTimedOut,
            })?;
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
            let mut conn = pool.get().await.map_err(|e| match e {
                bb8::RunError::User(e) => e,
                bb8::RunError::TimedOut => sqlx::Error::PoolTimedOut,
            })?;
            conn.describe(sql).await
        })
    }
}

pub struct Postgres {
    iam_auth_token_provider: DsqlIamDbAuthTokenProvider,
    connect_options: RwLock<Arc<PgConnectOptions>>,
}

impl Postgres {
    // NOTE: Ideally, we'd just generate a fresh token on every connection.
    // Unfortunately, `sqlx::PgConnectOptions` provides no way to update only
    // the password on every connection. The alternative that we use here uses
    // an `RwLock` to periodically update the connection options. One
    // alternative would be to just clone the connect options on each connection
    // and update the password. However, this seems expensive because connect
    // options include things like parsed SSL certs.
    pub async fn create_with_token_refresh(
        iam_auth_token_provider: DsqlIamDbAuthTokenProvider,
        connect_options: PgConnectOptions,
    ) -> Result<Arc<Self>> {
        let token = iam_auth_token_provider.generate_token().await?;
        let base_connect_options = connect_options.password(&token);
        let connector = Arc::new(Self {
            iam_auth_token_provider,
            connect_options: RwLock::new(Arc::new(base_connect_options.clone())),
        });
        connector
            .clone()
            .spawn_token_refresh(base_connect_options.get_host().to_string());
        Ok(connector)
    }

    async fn connect(&self) -> Result<sqlx::postgres::PgConnection, sqlx::Error> {
        let connect_options = self
            .connect_options
            .read()
            .await
            // Clone the Arc so we don't hold the RwLockReadGuard across an async await point
            .clone();

        let conn = tokio::time::timeout(CONNECT_TIMEOUT, connect_options.connect())
            .await
            .map_err(|_| sqlx::Error::PoolTimedOut)??;

        Ok(conn)
    }

    fn spawn_token_refresh(self: Arc<Postgres>, endpoint: String) {
        tracing::info!(endpoint, "spawn token refresh task");

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            tracing::info!(interval = ?interval.period(), "token refresh interval",);

            loop {
                interval.tick().await;
                tracing::debug!("refreshing credentials");

                let _ = self.update_token().await;
            }
        });
    }

    async fn update_token(&self) -> Result<()> {
        let token = self
            .iam_auth_token_provider
            .generate_token()
            .await
            .map_err(|err| anyhow!("Failed to generate token: {err}"))?;
        let base = self.connect_options.read().await.clone();
        let base = Arc::unwrap_or_clone(base);
        let connect_options = base.password(token.as_str());
        let mut guard = self.connect_options.write().await;
        *guard = Arc::new(connect_options);
        Ok(())
    }
}

pub struct DsqlIamDbAuthTokenProvider {
    sdk_config: SdkConfig,
    signer: AuthTokenGenerator,
    token_type: TokenType,
}

pub enum TokenType {
    Admin,
    Regular,
}

impl From<&str> for TokenType {
    fn from(value: &str) -> Self {
        match value.to_ascii_lowercase().trim_ascii() {
            "admin" => Self::Admin,
            _ => Self::Regular,
        }
    }
}

impl DsqlIamDbAuthTokenProvider {
    pub async fn new(
        hostname: &str,
        region: Region,
        token_type: TokenType,
        credential_provider: SharedCredentialsProvider,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            sdk_config: aws_config::defaults(BehaviorVersion::latest())
                .credentials_provider(credential_provider.clone())
                .region(region.clone())
                .load()
                .await,
            signer: AuthTokenGenerator::new(
                Config::builder()
                    .expires_in(TOKEN_VALIDITY_DURATION.as_secs())
                    .hostname(hostname)
                    .region(region)
                    .build()
                    .map_err(|err| anyhow!(err))?,
            ),
            token_type,
        })
    }

    async fn generate_token(&self) -> anyhow::Result<String> {
        match self.token_type {
            TokenType::Admin => {
                self.signer
                    .db_connect_admin_auth_token(&self.sdk_config)
                    .await
            }
            TokenType::Regular => self.signer.db_connect_auth_token(&self.sdk_config).await,
        }
        .map(|token| token.to_string())
        .map_err(|err| anyhow!(err))
    }
}
