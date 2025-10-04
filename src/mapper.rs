use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::db::{Cell, DataRow};

pub fn json_to_data_row(json: &str, timestamp: DateTime<Utc>) -> anyhow::Result<DataRow> {
    let v: Value = serde_json::from_str(json)?;

    let original_json = v.clone();

    if let Value::Object(obj) = v {
        let mut cells: HashMap<String, Cell> = obj
            .into_iter()
            .map(|(k, v)| (k, json_value_to_cell(v)))
            .collect();

        cells.insert("raw".to_string(), Cell::JsonObject(original_json));
        cells.insert(
            "received_ts".to_string(),
            Cell::DateTime(timestamp.naive_utc()),
        );

        Ok(DataRow { cells })
    } else {
        anyhow::bail!("Not a JSON object");
    }
}

pub fn json_value_to_cell(value: Value) -> Cell {
    match value {
        Value::Null => Cell::Null,
        Value::Bool(b) => Cell::Bool(b),
        Value::Number(n) => Cell::Number(n.as_i64().unwrap_or_default()),
        Value::String(s) => Cell::String(s),
        Value::Array(_) => Cell::JsonObject(value), // store arrays as text
        Value::Object(_) => Cell::JsonObject(value), // store objects as text
    }
}
