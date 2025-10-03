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

#[cfg(test)]
mod tests {
    use super::*;

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
