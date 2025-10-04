use sqlx::{postgres::PgPoolOptions, query::Query};
use std::collections::HashMap;

use crate::utils::get_wildcard_string;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Cell<Tz: chrono::TimeZone = chrono::Utc> {
    // have to think about this
    JsonObject(serde_json::Value),
    Number(i64),
    String(String),
    Bool(bool),
    DateTime(chrono::NaiveDateTime),
    DateTimeTz(chrono::DateTime<Tz>),
    Null,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataRow {
    pub cells: HashMap<String, Cell>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct MQTable {
    pub name: String,
}

impl MQTable {
    pub fn from_topic(topic: &str) -> Self {
        // clean the topic to make it a valid table name
        let name = topic.replace("/", "_").replace("-", "_").replace(".", "_");
        MQTable {
            name: name.to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Default, Clone, Hash)]
pub enum Modifier {
    NotNull,
    PrimaryKey,
    Unique,
    #[default]
    None,
}

impl Modifier {
    fn to_db_string(&self) -> String {
        match self {
            Modifier::NotNull => "NOT NULL".to_string(),
            Modifier::PrimaryKey => "PRIMARY KEY".to_string(),
            Modifier::Unique => "UNIQUE".to_string(),
            Modifier::None => "".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DefaultValue {
    value: String,
}

impl From<String> for DefaultValue {
    fn from(value: String) -> Self {
        DefaultValue { value }
    }
}

impl From<&str> for DefaultValue {
    fn from(value: &str) -> Self {
        DefaultValue {
            value: value.to_string(),
        }
    }
}

impl DefaultValue {
    fn to_db_string(&self) -> String {
        format!("DEFAULT ({})", self.value)
    }
}

#[derive(sqlx::FromRow, Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct MQTableColumnInfo {
    pub column_name: String,
    pub data_type: String,
    #[sqlx(skip)]
    pub modifier: Modifier,
    #[sqlx(skip)]
    pub default_value: Option<DefaultValue>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MQTableInfo {
    pub columns: HashMap<String, MQTableColumnInfo>,
}

impl MQTableInfo {
    pub fn exists(&self) -> bool {
        self.columns.len() > 0
    }

    pub fn has_column(&self, column_name: &str) -> bool {
        self.columns.contains_key(column_name)
    }

    pub fn columns(&self) -> Vec<&MQTableColumnInfo> {
        self.columns.values().collect()
    }
}

impl From<Vec<MQTableColumnInfo>> for MQTableInfo {
    fn from(value: Vec<MQTableColumnInfo>) -> Self {
        MQTableInfo {
            columns: value
                .into_iter()
                .map(|c| (c.column_name.clone(), c))
                .collect(),
        }
    }
}

pub trait DBDriver {
    #[allow(async_fn_in_trait)]
    async fn connect(connection_string: &str) -> anyhow::Result<impl DBDriver>;
    #[allow(async_fn_in_trait)]
    async fn execute_query(&self, query: &str) -> anyhow::Result<String>;

    #[allow(async_fn_in_trait)]
    async fn insert_one(&self, item: DataRow, table: &MQTable) -> anyhow::Result<()>;

    #[allow(async_fn_in_trait)]
    async fn insert_many(&self, items: &[DataRow], table: &MQTable) -> anyhow::Result<()>;

    #[allow(async_fn_in_trait)]
    async fn get_table_info(&self, table: &MQTable) -> anyhow::Result<MQTableInfo>;

    #[allow(async_fn_in_trait)]
    async fn add_column_to_table(
        &self,
        table: &MQTable,
        column: &MQTableColumnInfo,
    ) -> anyhow::Result<()>;

    #[allow(async_fn_in_trait)]
    async fn create_table_if_not_exists(
        &self,
        table: &MQTable,
        info: &MQTableInfo,
    ) -> anyhow::Result<()>;

    fn convert_to_db_type_string(&self, cell: &Cell) -> String;
}
pub struct PostgresDriver {
    pool: sqlx::Pool<sqlx::Postgres>,
}
impl DBDriver for PostgresDriver {
    #[allow(refining_impl_trait)]
    async fn connect(connection_string: &str) -> anyhow::Result<PostgresDriver> {
        // Implement connection logic here
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await?;
        println!("Connected to Postgres with {}", connection_string);
        anyhow::Ok(PostgresDriver { pool })
    }

    async fn execute_query(&self, _: &str) -> anyhow::Result<String> {
        unimplemented!()
    }

    async fn insert_one(&self, row: DataRow, table: &MQTable) -> anyhow::Result<()> {
        self.insert_many(&[row], table).await
    }

    async fn insert_many(&self, items: &[DataRow], table: &MQTable) -> anyhow::Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let columns: Vec<_> = items[0].cells.keys().cloned().collect();

        let placeholder_string = get_wildcard_string(columns.len(), items.len());

        let query_string = format!(
            "INSERT INTO {} ({}) VALUES {} ON CONFLICT DO NOTHING",
            table.name,
            columns.join(", "),
            placeholder_string
        );

        let mut intermediate_query: Query<'_, _, _> = sqlx::query(&query_string);

        for item in items {
            for cell in item.cells.values() {
                intermediate_query = bind_to_query(intermediate_query, cell);
            }
        }

        intermediate_query.execute(&self.pool).await?;

        Ok(())
    }

    async fn get_table_info(&self, table: &MQTable) -> anyhow::Result<MQTableInfo> {
        sqlx::query_as::<_, MQTableColumnInfo>(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
        )
        .bind(table.name.as_str()) // Replace with actual table name
        .fetch_all(&self.pool)
        .await
        .map(|rows| rows.into())
        .map_err(|e| e.into())
    }

    async fn add_column_to_table(
        &self,
        table: &MQTable,
        column: &MQTableColumnInfo,
    ) -> anyhow::Result<()> {
        let query_string = format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
            table.name, column.column_name, column.data_type
        );

        sqlx::query(&query_string)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| e.into())
    }

    async fn create_table_if_not_exists(
        &self,
        table: &MQTable,
        info: &MQTableInfo,
    ) -> anyhow::Result<()> {
        let col_string = info
            .columns()
            .iter()
            .map(|col| {
                format!(
                    "{} {} {} {}",
                    col.column_name,
                    col.data_type,
                    col.modifier.to_db_string(),
                    col.default_value
                        .as_ref()
                        .map(|f| f.to_db_string())
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let query_string = format!("CREATE TABLE IF NOT EXISTS {} ({col_string})", table.name);

        sqlx::query(&query_string)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| e.into())
    }

    fn convert_to_db_type_string(&self, cell: &Cell) -> String {
        match cell {
            Cell::Number(_) => "BIGINT".to_string(),
            Cell::String(_) => "TEXT".to_string(),
            Cell::Bool(_) => "BOOLEAN".to_string(),
            Cell::Null => "TEXT".to_string(), // Default to TEXT for NULLs
            Cell::JsonObject(_) => "JSONB".to_string(),
            Cell::DateTime(_) => "TIMESTAMP".to_string(),
            Cell::DateTimeTz(_) => "TIMESTAMPTZ".to_string(),
        }
    }
}

fn bind_to_query<'a>(
    mut intermediate_query: Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
    cell: &'a Cell,
) -> Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match cell {
        Cell::Number(n) => {
            intermediate_query = intermediate_query.bind(n);
        }
        Cell::String(s) => {
            intermediate_query = intermediate_query.bind(s);
        }
        Cell::Bool(b) => {
            intermediate_query = intermediate_query.bind(b);
        }
        Cell::Null => {
            intermediate_query = intermediate_query.bind(None::<String>);
        }
        Cell::JsonObject(obj) => {
            intermediate_query = intermediate_query.bind(obj);
        }
        Cell::DateTime(dt) => {
            intermediate_query = intermediate_query.bind(dt);
        }
        Cell::DateTimeTz(dt) => {
            intermediate_query = intermediate_query.bind(dt);
        }
    }
    return intermediate_query;
}
