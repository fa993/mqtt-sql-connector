use std::collections::HashMap;

use crate::{
    db::{DBDriver, DataRow, MQTable, MQTableColumnInfo, MQTableInfo, Modifier},
    utils::PreDefinedColumn,
};

pub struct Manager<T: DBDriver + Send + Sync> {
    driver: T,
    col_cache: HashMap<MQTable, MQTableInfo>,
}

impl<T: DBDriver + Send + Sync> Manager<T> {
    pub fn new(driver: T) -> Self {
        Self {
            driver,
            col_cache: HashMap::new(),
        }
    }

    pub async fn initialize(&mut self, table: &MQTable) -> anyhow::Result<()> {
        let mut table_info = self.driver.get_table_info(table).await?;
        if !table_info.exists() {
            let col_info: MQTableInfo = vec![
                MQTableColumnInfo {
                    column_name: PreDefinedColumn::PKey.to_string(),
                    data_type: "SERIAL".to_string(),
                    modifier: Modifier::PrimaryKey,
                    ..Default::default()
                },
                MQTableColumnInfo {
                    column_name: PreDefinedColumn::Raw.to_string(),
                    data_type: "TEXT".to_string(),
                    ..Default::default()
                },
                MQTableColumnInfo {
                    column_name: PreDefinedColumn::InsertTs.to_string(),
                    data_type: "TIMESTAMP".to_string(),
                    default_value: Some("CURRENT_TIMESTAMP AT TIME ZONE 'UTC'".into()),
                    ..Default::default()
                },
                MQTableColumnInfo {
                    column_name: PreDefinedColumn::ReceivedTs.to_string(),
                    data_type: "TIMESTAMP".to_string(),
                    ..Default::default()
                },
            ]
            .into();

            self.driver
                .create_table_if_not_exists(table, &col_info)
                .await?;

            table_info = col_info;
        }

        self.col_cache.insert(table.clone(), table_info);

        Ok(())
    }

    async fn pre_process(&mut self, table: &MQTable, row: &DataRow) -> Result<(), anyhow::Error> {
        if !self.col_cache.contains_key(table) {
            self.initialize(table).await?;
        }
        let table_info = self.col_cache.get_mut(table).unwrap();
        Ok(for (col, val) in row.cells.iter() {
            if !table_info.has_column(&col) {
                let col_info = MQTableColumnInfo {
                    column_name: col.clone(),
                    // infer data type from cell
                    data_type: self.driver.convert_to_db_type_string(&val),
                    ..Default::default()
                };
                table_info
                    .columns
                    .insert(col_info.column_name.clone(), col_info.clone());
                self.driver.add_column_to_table(table, &col_info).await?;
            }
        })
    }

    pub async fn insert(&mut self, table: &MQTable, row: DataRow) -> anyhow::Result<()> {
        self.pre_process(table, &row).await?;

        self.driver.insert_one(row, table).await
    }

    pub async fn insert_many(&mut self, table: &MQTable, rows: &[DataRow]) -> anyhow::Result<()> {
        for row in rows {
            self.pre_process(table, row).await?;
        }

        self.driver.insert_many(rows, table).await
    }
}
