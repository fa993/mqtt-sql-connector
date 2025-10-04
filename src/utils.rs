use itertools::Itertools;

pub fn get_wildcard_string(column_len: usize, items_len: usize) -> String {
    let placeholders = (1..=(column_len * items_len))
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .chunks(column_len)
        .map(|f| f.join(", "))
        .join("), (");

    format!("({})", placeholders)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PreDefinedColumn {
    PKey,
    Raw,
    InsertTs,
    ReceivedTs,
}

impl ToString for PreDefinedColumn {
    fn to_string(&self) -> String {
        match self {
            PreDefinedColumn::PKey => "pkey".to_string(),
            PreDefinedColumn::Raw => "raw".to_string(),
            PreDefinedColumn::InsertTs => "insert_ts".to_string(),
            PreDefinedColumn::ReceivedTs => "received_ts".to_string(),
        }
    }
}

impl std::str::FromStr for PreDefinedColumn {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pkey" => Ok(PreDefinedColumn::PKey),
            "raw" => Ok(PreDefinedColumn::Raw),
            "insert_ts" => Ok(PreDefinedColumn::InsertTs),
            "received_ts" => Ok(PreDefinedColumn::ReceivedTs),
            _ => anyhow::bail!("Unknown PreDefinedColumn: {}", s),
        }
    }
}

#[cfg(test)]
mod tests {
    mod get_wildcard_string {
        use crate::utils::get_wildcard_string;

        #[test]
        fn test_single_column_single_item() {
            let result = get_wildcard_string(1, 1);
            assert_eq!(result, "($1)");
        }

        #[test]
        fn test_single_column_multiple_items() {
            let result = get_wildcard_string(1, 3);
            assert_eq!(result, "($1), ($2), ($3)");
        }

        #[test]
        fn test_multiple_columns_single_item() {
            let result = get_wildcard_string(3, 1);
            assert_eq!(result, "($1, $2, $3)");
        }

        #[test]
        fn test_multiple_columns_multiple_items() {
            let result = get_wildcard_string(2, 3);
            assert_eq!(result, "($1, $2), ($3, $4), ($5, $6)");
        }

        #[test]
        fn test_large_case() {
            let result = get_wildcard_string(4, 2);
            assert_eq!(result, "($1, $2, $3, $4), ($5, $6, $7, $8)");
        }

        #[test]
        fn test_zero_items() {
            // This is an edge case. The current code will produce "()"
            // because `join` on empty Vec returns "".
            let result = get_wildcard_string(2, 0);
            assert_eq!(result, "()");
        }
    }

    mod pre_defined_column_to_string {
        use crate::utils::PreDefinedColumn;

        use std::{collections::HashSet, str::FromStr};

        #[test]
        fn test_to_string_and_from_str_roundtrip() {
            let variants = vec![
                PreDefinedColumn::PKey,
                PreDefinedColumn::Raw,
                PreDefinedColumn::InsertTs,
                PreDefinedColumn::ReceivedTs,
            ];

            for v in variants {
                let s = v.to_string();
                let parsed = PreDefinedColumn::from_str(&s).unwrap();
                assert_eq!(v, parsed, "Roundtrip failed for {:?}", v);
            }
        }

        #[test]
        fn test_invalid_from_str() {
            let result = PreDefinedColumn::from_str("not_a_column");
            assert!(result.is_err());
        }

        #[test]
        fn test_to_string_pkey() {
            let col = PreDefinedColumn::PKey;
            assert_eq!(col.to_string(), "pkey");
        }

        #[test]
        fn test_to_string_raw() {
            let col = PreDefinedColumn::Raw;
            assert_eq!(col.to_string(), "raw");
        }

        #[test]
        fn test_to_string_insert_ts() {
            let col = PreDefinedColumn::InsertTs;
            assert_eq!(col.to_string(), "insert_ts");
        }

        #[test]
        fn test_to_string_received_ts() {
            let col = PreDefinedColumn::ReceivedTs;
            assert_eq!(col.to_string(), "received_ts");
        }

        #[test]
        fn test_equality() {
            assert_eq!(PreDefinedColumn::PKey, PreDefinedColumn::PKey);
            assert_ne!(PreDefinedColumn::PKey, PreDefinedColumn::Raw);
        }

        #[test]
        fn test_cloning() {
            let col = PreDefinedColumn::InsertTs;
            let cloned = col.clone();
            assert_eq!(col, cloned);
        }

        #[test]
        fn test_hashing_unique() {
            let mut set = HashSet::new();
            set.insert(PreDefinedColumn::PKey);
            set.insert(PreDefinedColumn::Raw);
            set.insert(PreDefinedColumn::InsertTs);
            set.insert(PreDefinedColumn::ReceivedTs);

            assert_eq!(set.len(), 4); // all unique
        }
    }
}
