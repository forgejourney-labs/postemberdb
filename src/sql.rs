//! SQL token helpers and the currently supported SQL subset parser.

use super::{Column, ComparisonOperator, DataType, DbError, IndexKey, SqlPredicate, Table, Value};

pub(super) fn take_parenthesized(input: &str) -> Result<(&str, &str), DbError> {
    let input = input.trim_start();
    if !input.starts_with('(') {
        return Err(DbError::Parse("expected '('".into()));
    }
    let mut depth = 0i32;
    let mut quote = None;
    let chars = input.char_indices().collect::<Vec<_>>();
    let mut cursor = 0usize;
    while cursor < chars.len() {
        let (position, ch) = chars[cursor];
        if let Some(active) = quote {
            if ch == active {
                if cursor + 1 < chars.len() && chars[cursor + 1].1 == active {
                    cursor += 2;
                    continue;
                }
                quote = None;
            }
        } else if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if ch == '(' {
            depth += 1;
        } else if ch == ')' {
            depth -= 1;
            if depth == 0 {
                return Ok((&input[1..position], input[position + 1..].trim()));
            }
        }
        cursor += 1;
    }
    Err(DbError::Parse("unclosed parenthesized expression".into()))
}

pub(super) fn apply_upsert(table: &mut Table, values: &[Value], clause: &str) -> Result<bool, DbError> {
    let trimmed = clause.trim();
    let upper = trimmed.to_ascii_uppercase();
    let (conflict_index, assignments) = if upper.starts_with("ON CONFLICT") {
        let rest = trimmed["ON CONFLICT".len()..].trim();
        let (columns_text, after_columns) = take_parenthesized(rest)?;
        let column_names = split_csv(columns_text).into_iter().map(normalize_ident).collect::<Vec<_>>();
        let column_indices = column_names.iter().map(|name| {
            table.columns.iter().position(|column| column.name == *name)
                .ok_or_else(|| DbError::ColumnNotFound(name.clone()))
        }).collect::<Result<Vec<_>, _>>()?;
        let after_upper = after_columns.to_ascii_uppercase();
        let set_pos = after_upper.find("DO UPDATE SET")
            .ok_or_else(|| DbError::Parse("expected DO UPDATE SET in ON CONFLICT".into()))?;
        let index = table.indexes.values().find(|index| index.unique && index.column_indices == column_indices)
            .ok_or_else(|| DbError::Constraint("ON CONFLICT target is not a unique key".into()))?;
        (Some(index), after_columns[set_pos + "DO UPDATE SET".len()..].trim())
    } else if upper.starts_with("ON DUPLICATE KEY UPDATE") {
        (None, trimmed["ON DUPLICATE KEY UPDATE".len()..].trim())
    } else {
        return Err(DbError::Unsupported(format!("upsert clause: {trimmed}")));
    };

    let existing_row_id = if let Some(index) = conflict_index {
        let key = IndexKey::from_columns(values, &index.column_indices);
        index.entries.get(&key).and_then(|row_ids| row_ids.first()).copied()
    } else {
        table.indexes.values().filter(|index| index.unique).find_map(|index| {
            let key = IndexKey::from_columns(values, &index.column_indices);
            index.entries.get(&key).and_then(|row_ids| row_ids.first()).copied()
        })
    };
    let Some(row_id) = existing_row_id else { return Ok(false); };
    let Some(row_index) = table.row_positions.get(&row_id).copied() else {
        return Err(DbError::Storage("unique index references an unknown stable row identifier".into()));
    };

    let mut updates = Vec::new();
    for assignment in split_csv(assignments) {
        let equals = assignment.find('=').ok_or_else(|| DbError::Parse("expected '=' in upsert assignment".into()))?;
        let target_name = normalize_ident(assignment[..equals].trim());
        let target_index = table.columns.iter().position(|column| column.name == target_name)
            .ok_or_else(|| DbError::ColumnNotFound(target_name.clone()))?;
        let rhs = assignment[equals + 1..].trim();
        let upper_rhs = rhs.to_ascii_uppercase();
        let source_value = if upper_rhs.starts_with("EXCLUDED.") {
            let source_name = normalize_ident(rhs["EXCLUDED.".len()..].trim());
            let source_index = table.columns.iter().position(|column| column.name == source_name)
                .ok_or_else(|| DbError::ColumnNotFound(source_name.clone()))?;
            values[source_index].clone()
        } else if upper_rhs.starts_with("VALUES") {
            let (source_name, _) = take_parenthesized(rhs["VALUES".len()..].trim())?;
            let source_name = normalize_ident(source_name);
            let source_index = table.columns.iter().position(|column| column.name == source_name)
                .ok_or_else(|| DbError::ColumnNotFound(source_name.clone()))?;
            values[source_index].clone()
        } else {
            parse_value(rhs)?
        };
        validate_value(&table.columns[target_index], &source_value)?;
        updates.push((target_index, source_value));
    }
    for (target_index, _) in &updates {
        if table.indexes.values().any(|index| index.unique && index.column_indices.contains(target_index)) {
            return Err(DbError::Constraint("upsert cannot modify a primary-key column".into()));
        }
    }
    let old_values = table.rows[row_index].values.clone();
    for (target_index, value) in updates {
        table.rows[row_index].values[target_index] = value;
    }
    table.reindex_row(row_index, &old_values);
    Ok(true)
}

pub(super) fn find_unquoted_keyword(input: &str, keyword: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let needle = keyword.as_bytes();
    if needle.is_empty() || needle.len() > bytes.len() { return None; }
    let mut quote = None;
    let mut cursor = 0usize;
    while cursor + needle.len() <= bytes.len() {
        let byte = bytes[cursor];
        if let Some(active) = quote {
            if byte == active {
                if bytes.get(cursor + 1) == Some(&active) {
                    cursor += 2;
                    continue;
                }
                quote = None;
            }
            cursor += 1;
            continue;
        }
        if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
            cursor += 1;
            continue;
        }
        if bytes[cursor..cursor + needle.len()].eq_ignore_ascii_case(needle) {
            return Some(cursor);
        }
        cursor += 1;
    }
    None
}

pub(super) fn parse_order_by(columns: &[Column], clause: &str) -> Result<Vec<(usize, bool)>, DbError> {
    let parts = split_csv(clause);
    if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
        return Err(DbError::Parse("ORDER BY requires at least one column".into()));
    }
    parts.into_iter().map(|part| {
        let tokens = part.split_whitespace().collect::<Vec<_>>();
        if tokens.is_empty() || tokens.len() > 2 {
            return Err(DbError::Parse(format!("invalid ORDER BY item: {part}")));
        }
        let name = normalize_ident(tokens[0]);
        let descending = match tokens.get(1).map(|value| value.to_ascii_uppercase()) {
            None => false,
            Some(direction) if direction == "ASC" => false,
            Some(direction) if direction == "DESC" => true,
            Some(_) => return Err(DbError::Parse(format!("ORDER BY direction must be ASC or DESC: {part}"))),
        };
        let index = columns.iter().position(|column| column.name == name)
            .ok_or_else(|| DbError::ColumnNotFound(name.clone()))?;
        Ok((index, descending))
    }).collect()
}

pub(super) fn parse_where_predicates(
    columns: &[Column],
    predicate: &str,
) -> Result<Vec<SqlPredicate>, DbError> {
    let conditions = split_and_conditions(predicate);
    if conditions.is_empty() || conditions.iter().any(|condition| condition.is_empty()) {
        return Err(DbError::Parse("empty WHERE predicate".into()));
    }
    conditions.into_iter().map(|condition| {
        let trimmed = condition.trim();
        let (left, operator, value) = if let Some(position) = find_unquoted_keyword(trimmed, " IS NOT NULL") {
            (&trimmed[..position], ComparisonOperator::IsNotNull, None)
        } else if let Some(position) = find_unquoted_keyword(trimmed, " IS NULL") {
            (&trimmed[..position], ComparisonOperator::IsNull, None)
        } else if let Some(position) = find_unquoted_keyword(trimmed, " LIKE ") {
            (&trimmed[..position], ComparisonOperator::Like, Some(parse_value(trimmed[position + 6..].trim())?))
        } else {
            let operators = [
                ("<=", ComparisonOperator::LessOrEqual),
                (">=", ComparisonOperator::GreaterOrEqual),
                ("<>", ComparisonOperator::NotEqual),
                ("!=", ComparisonOperator::NotEqual),
                ("=", ComparisonOperator::Equal),
                ("<", ComparisonOperator::Less),
                (">", ComparisonOperator::Greater),
            ];
            let found = operators.iter().find_map(|(token, operator)| {
                find_unquoted_keyword(trimmed, token).map(|position| (position, *operator, token.len()))
            }).ok_or_else(|| DbError::Parse(format!("unsupported WHERE predicate: {trimmed}")))?;
            let (position, operator, width) = found;
            (&trimmed[..position], operator, Some(parse_value(trimmed[position + width..].trim())?))
        };
        let column_name = normalize_ident(left);
        let column_index = columns.iter().position(|column| column.name == column_name)
            .ok_or_else(|| DbError::ColumnNotFound(column_name.clone()))?;
        if let Some(value) = &value {
            validate_value(&columns[column_index], value)?;
        }
        if operator == ComparisonOperator::Like && columns[column_index].data_type != DataType::Text {
            return Err(DbError::TypeMismatch {
                column: column_name,
                expected: DataType::Text,
            });
        }
        Ok(SqlPredicate { column_index, operator, value })
    }).collect()
}

pub(super) fn parse_equality_predicates(columns: &[Column], predicate: &str) -> Result<Vec<(usize, Value)>, DbError> {
    let conditions = split_and_conditions(predicate);
    if conditions.is_empty() {
        return Err(DbError::Parse("empty WHERE predicate".into()));
    }
    conditions.into_iter().map(|condition| {
        let equals = condition.find('=').ok_or_else(|| DbError::Parse("expected '=' in equality predicate".into()))?;
        let column_name = normalize_ident(condition[..equals].trim());
        let value = parse_value(condition[equals + 1..].trim())?;
        let column_index = columns.iter().position(|column| column.name == column_name)
            .ok_or_else(|| DbError::ColumnNotFound(column_name.clone()))?;
        Ok((column_index, value))
    }).collect()
}

pub(super) fn split_and_conditions(input: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0usize;
    let mut quote = None;
    let chars = input.char_indices().collect::<Vec<_>>();
    let mut cursor = 0usize;
    while cursor < chars.len() {
        let (position, ch) = chars[cursor];
        if let Some(active) = quote {
            if ch == active {
                if cursor + 1 < chars.len() && chars[cursor + 1].1 == active {
                    cursor += 2;
                    continue;
                }
                quote = None;
            }
            cursor += 1;
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            cursor += 1;
            continue;
        }
        if input.as_bytes().get(position..position + 5)
            .map(|bytes| bytes.eq_ignore_ascii_case(b" AND "))
            .unwrap_or(false) {
            result.push(input[start..position].trim());
            start = position + 5;
            while cursor < chars.len() && chars[cursor].0 < start { cursor += 1; }
            continue;
        }
        cursor += 1;
    }
    result.push(input[start..].trim());
    result
}

pub(super) fn bind_parameters(sql: &str, parameters: &[Value]) -> Result<String, DbError> {
    let mut output = String::with_capacity(sql.len() + parameters.len() * 8);
    let mut parameter_index = 0usize;
    let mut quote = None;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if let Some(active_quote) = quote {
            output.push(ch);
            if ch == active_quote {
                if chars.peek() == Some(&active_quote) {
                    output.push(chars.next().expect("peeked quote"));
                } else {
                    quote = None;
                }
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            output.push(ch);
        } else if ch == '?' {
            let value = parameters.get(parameter_index)
                .ok_or_else(|| DbError::Parse("not enough SQL parameters".into()))?;
            output.push_str(&value_to_sql(value));
            parameter_index += 1;
        } else {
            output.push(ch);
        }
    }
    if parameter_index != parameters.len() {
        return Err(DbError::Parse(format!(
            "expected {parameter_index} SQL parameters, received {}", parameters.len()
        )));
    }
    Ok(output)
}

pub(super) fn value_to_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Integer(value) => value.to_string(),
        Value::Boolean(value) => if *value { "TRUE".into() } else { "FALSE".into() },
        Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
    }
}

pub(super) fn starts_with_kw(input: &str, keyword: &str) -> bool {
    input.get(..keyword.len()).map(|s| s.eq_ignore_ascii_case(keyword)).unwrap_or(false)
}

pub(super) fn find_kw(input: &str, keyword: &str) -> Option<usize> {
    input.to_ascii_uppercase().find(&keyword.to_ascii_uppercase())
}

pub(super) fn normalize_ident(input: &str) -> String {
    input.trim().trim_matches('`').trim_matches('"').to_ascii_lowercase()
}

pub(super) fn split_csv(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let (mut start, mut quoted, mut quote_char, mut depth) = (0, false, '\0', 0i32);
    for (i, ch) in input.char_indices() {
        if (ch == '\'' || ch == '"') && (!quoted || ch == quote_char) {
            if quoted { quoted = false; } else { quoted = true; quote_char = ch; }
        } else if !quoted && ch == '(' {
            depth += 1;
        } else if !quoted && ch == ')' {
            depth -= 1;
        } else if ch == ',' && !quoted && depth == 0 {
            parts.push(input[start..i].trim());
            start = i + 1;
        }
    }
    parts.push(input[start..].trim());
    parts
}

pub(super) fn parse_value(input: &str) -> Result<Value, DbError> {
    let s = input.trim();
    if s.eq_ignore_ascii_case("NULL") { return Ok(Value::Null); }
    if s.eq_ignore_ascii_case("TRUE") { return Ok(Value::Boolean(true)); }
    if s.eq_ignore_ascii_case("FALSE") { return Ok(Value::Boolean(false)); }
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        return Ok(Value::Text(s[1..s.len()-1].replace("''", "'")));
    }
    if let Ok(v) = s.parse::<i64>() { return Ok(Value::Integer(v)); }
    Err(DbError::Parse(format!("invalid value: {s}")))
}

pub(super) fn validate_value(column: &Column, value: &Value) -> Result<(), DbError> {
    if matches!(value, Value::Null) {
        if !column.nullable { return Err(DbError::Constraint(format!("{} may not be NULL", column.name))); }
        return Ok(());
    }
    let valid = matches!((&column.data_type, value),
        (DataType::Integer, Value::Integer(_)) |
        (DataType::Text, Value::Text(_)) |
        (DataType::Boolean, Value::Boolean(_)));
    if valid { Ok(()) } else {
        Err(DbError::TypeMismatch { column: column.name.clone(), expected: column.data_type.clone() })
    }
}
