//! SQL token helpers and the currently supported SQL subset parser.

use super::{Column, ComparisonOperator, DataType, DbError, IndexKey, SqlExpression, SqlPredicate, SqlValueExpression, Table, Value};


#[derive(Debug, Clone, PartialEq)]
enum SqlTokenKind { Identifier(String), Integer(i64), Text(String), Operator(String), LeftParen, RightParen, End }
#[derive(Debug, Clone, PartialEq)]
struct SqlToken { kind: SqlTokenKind, offset: usize }

fn tokenize_scalar_expression(input: &str) -> Result<Vec<SqlToken>, DbError> {
    let bytes = input.as_bytes();
    let mut cursor = 0usize;
    let mut tokens = Vec::new();
    while cursor < bytes.len() {
        let byte = bytes[cursor];
        if byte.is_ascii_whitespace() { cursor += 1; continue; }
        if byte == b'-' && bytes.get(cursor + 1) == Some(&b'-') {
            cursor += 2;
            while cursor < bytes.len() && bytes[cursor] != b'\n' { cursor += 1; }
            continue;
        }
        if byte == b'/' && bytes.get(cursor + 1) == Some(&b'*') {
            let start = cursor; cursor += 2;
            let mut closed = false;
            while cursor + 1 < bytes.len() {
                if bytes[cursor] == b'*' && bytes[cursor + 1] == b'/' { cursor += 2; closed = true; break; }
                cursor += 1;
            }
            if !closed { return Err(DbError::Parse(format!("unclosed SQL comment at byte {start}"))); }
            continue;
        }
        if byte == b'\'' {
            let start = cursor; cursor += 1;
            let mut value = String::new(); let mut closed = false;
            while cursor < bytes.len() {
                if bytes[cursor] == b'\'' {
                    if bytes.get(cursor + 1) == Some(&b'\'') { value.push('\''); cursor += 2; continue; }
                    cursor += 1; closed = true; break;
                }
                let ch = input[cursor..].chars().next().ok_or_else(|| DbError::Parse("invalid SQL string boundary".into()))?;
                value.push(ch); cursor += ch.len_utf8();
            }
            if !closed { return Err(DbError::Parse(format!("unclosed SQL string at byte {start}"))); }
            tokens.push(SqlToken { kind: SqlTokenKind::Text(value), offset: start }); continue;
        }
        if byte == b'"' || byte == 0x60 || byte == b'[' {
            let start = cursor; let closing = if byte == b'[' { b']' } else { byte }; cursor += 1;
            let mut value = String::new(); let mut closed = false;
            while cursor < bytes.len() {
                if bytes[cursor] == closing {
                    if bytes.get(cursor + 1) == Some(&closing) { value.push(closing as char); cursor += 2; continue; }
                    cursor += 1; closed = true; break;
                }
                let ch = input[cursor..].chars().next().ok_or_else(|| DbError::Parse("invalid quoted identifier boundary".into()))?;
                value.push(ch); cursor += ch.len_utf8();
            }
            if !closed { return Err(DbError::Parse(format!("unclosed quoted identifier at byte {start}"))); }
            tokens.push(SqlToken { kind: SqlTokenKind::Identifier(value), offset: start }); continue;
        }
        if byte.is_ascii_digit() {
            let start = cursor;
            while cursor < bytes.len() && bytes[cursor].is_ascii_digit() { cursor += 1; }
            if cursor < bytes.len() && matches!(bytes[cursor], b'.' | b'e' | b'E') {
                return Err(DbError::Unsupported("REAL numeric literals are not supported by the current Value model".into()));
            }
            let number = input[start..cursor].parse::<i64>().map_err(|_| DbError::Parse(format!("integer literal is out of range: {}", &input[start..cursor])))?;
            tokens.push(SqlToken { kind: SqlTokenKind::Integer(number), offset: start }); continue;
        }
        let ch = input[cursor..].chars().next().ok_or_else(|| DbError::Parse("invalid expression boundary".into()))?;
        if ch == '_' || ch.is_alphabetic() || !ch.is_ascii() {
            let start = cursor; cursor += ch.len_utf8();
            while cursor < bytes.len() {
                let next = input[cursor..].chars().next().expect("UTF-8 boundary");
                if next == '_' || next == '
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

fn parse_parenthesized_values(input: &str) -> Result<Vec<Value>, DbError> {
    let trimmed = input.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return Err(DbError::Parse("IN requires a parenthesized value list".into()));
    }
    let inner = trimmed[1..trimmed.len() - 1].trim();
    if inner.is_empty() {
        return Err(DbError::Parse("IN requires at least one value".into()));
    }
    split_csv(inner).into_iter().map(parse_value).collect()
}

pub(super) fn parse_where_expression(
    columns: &[Column],
    input: &str,
) -> Result<SqlExpression, DbError> {
    fn parse(columns: &[Column], source: &str) -> Result<SqlExpression, DbError> {
        let source = source.trim();
        if source.is_empty() {
            return Err(DbError::Parse("empty WHERE predicate".into()));
        }

        if source.starts_with('(') {
            let (inner, remainder) = take_parenthesized(source)?;
            if remainder.is_empty() {
                return parse(columns, inner);
            }
        }

        let disjunction = split_or_conditions(source);
        if disjunction.len() > 1 {
            return Ok(SqlExpression::Or(
                disjunction.iter().map(|part| parse(columns, part)).collect::<Result<Vec<_>, _>>()?
            ));
        }

        let conjunction = split_and_conditions(source);
        if conjunction.len() > 1 {
            return Ok(SqlExpression::And(
                conjunction.iter().map(|part| parse(columns, part)).collect::<Result<Vec<_>, _>>()?
            ));
        }

        let upper = source.to_ascii_uppercase();
        if upper.starts_with("NOT ") {
            return Ok(SqlExpression::Not(Box::new(parse(columns, source[4..].trim())?)));
        }
        if upper.starts_with("NOT(") {
            return Ok(SqlExpression::Not(Box::new(parse(columns, source[3..].trim())?)));
        }

        match parse_where_predicates(columns, source) {
            Ok(predicates) if predicates.len() == 1 => {
                Ok(SqlExpression::Predicate(predicates.into_iter().next().unwrap()))
            }
            Ok(_) => Err(DbError::Parse("invalid nested WHERE expression".into())),
            Err(predicate_error) => parse_scalar_comparison(columns, source).or(Err(predicate_error)),
        }
    }

    parse(columns, input)
}

pub(super) fn parse_where_predicates(
    columns: &[Column],
    predicate: &str,
) -> Result<Vec<SqlPredicate>, DbError> {
    // AND binds more tightly than OR; each OR branch is one conjunction.
    let groups = split_or_conditions(predicate);
    if groups.is_empty() || groups.iter().any(|group| group.is_empty()) {
        return Err(DbError::Parse("empty WHERE predicate".into()));
    }

    let mut parsed = Vec::new();
    for (group, expression) in groups.iter().enumerate() {
        let conditions = split_and_conditions(expression);
        if conditions.is_empty() || conditions.iter().any(|condition| condition.is_empty()) {
            return Err(DbError::Parse("empty WHERE predicate".into()));
        }

        for condition in conditions {
            let trimmed = condition.trim();
            let (left, operator, value, values) =
                if let Some(position) = find_unquoted_keyword(trimmed, " NOT IN ") {
                    let list = parse_parenthesized_values(trimmed[position + 8..].trim())?;
                    (&trimmed[..position], ComparisonOperator::NotIn, None, list)
                } else if let Some(position) = find_unquoted_keyword(trimmed, " IN ") {
                    let list = parse_parenthesized_values(trimmed[position + 4..].trim())?;
                    (&trimmed[..position], ComparisonOperator::In, None, list)
                } else if let Some(position) = find_unquoted_keyword(trimmed, " NOT BETWEEN ") {
                    let bounds = split_and_conditions(trimmed[position + 13..].trim());
                    if bounds.len() != 2 {
                        return Err(DbError::Parse(
                            "BETWEEN requires lower AND upper bounds".into(),
                        ));
                    }
                    (
                        &trimmed[..position],
                        ComparisonOperator::NotBetween,
                        None,
                        vec![parse_value(bounds[0])?, parse_value(bounds[1])?],
                    )
                } else if let Some(position) = find_unquoted_keyword(trimmed, " BETWEEN ") {
                    let bounds = split_and_conditions(trimmed[position + 9..].trim());
                    if bounds.len() != 2 {
                        return Err(DbError::Parse(
                            "BETWEEN requires lower AND upper bounds".into(),
                        ));
                    }
                    (
                        &trimmed[..position],
                        ComparisonOperator::Between,
                        None,
                        vec![parse_value(bounds[0])?, parse_value(bounds[1])?],
                    )
                } else if let Some(position) =
                    find_unquoted_keyword(trimmed, " IS NOT DISTINCT FROM ")
                {
                    (
                        &trimmed[..position],
                        ComparisonOperator::IsNotDistinctFrom,
                        Some(parse_value(trimmed[position + " IS NOT DISTINCT FROM ".len()..].trim())?),
                        Vec::new(),
                    )
                } else if let Some(position) =
                    find_unquoted_keyword(trimmed, " IS DISTINCT FROM ")
                {
                    (
                        &trimmed[..position],
                        ComparisonOperator::IsDistinctFrom,
                        Some(parse_value(trimmed[position + " IS DISTINCT FROM ".len()..].trim())?),
                        Vec::new(),
                    )
                } else if let Some(position) =
                    find_unquoted_keyword(trimmed, " IS NOT NULL")
                {
                    (
                        &trimmed[..position],
                        ComparisonOperator::IsNotNull,
                        None,
                        Vec::new(),
                    )
                } else if let Some(position) = find_unquoted_keyword(trimmed, " IS NULL") {
                    (
                        &trimmed[..position],
                        ComparisonOperator::IsNull,
                        None,
                        Vec::new(),
                    )
                } else if let Some(position) = find_unquoted_keyword(trimmed, " NOT LIKE ") {
                    (
                        &trimmed[..position],
                        ComparisonOperator::NotLike,
                        Some(parse_value(trimmed[position + 10..].trim())?),
                        Vec::new(),
                    )
                } else if let Some(position) = find_unquoted_keyword(trimmed, " LIKE ") {
                    (
                        &trimmed[..position],
                        ComparisonOperator::Like,
                        Some(parse_value(trimmed[position + 6..].trim())?),
                        Vec::new(),
                    )
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
                    let found = operators
                        .iter()
                        .find_map(|(token, operator)| {
                            find_unquoted_keyword(trimmed, token)
                                .map(|position| (position, *operator, token.len()))
                        })
                        .ok_or_else(|| {
                            DbError::Parse(format!(
                                "unsupported WHERE predicate: {trimmed}"
                            ))
                        })?;
                    let (position, operator, width) = found;
                    (
                        &trimmed[..position],
                        operator,
                        Some(parse_value(trimmed[position + width..].trim())?),
                        Vec::new(),
                    )
                };

            let column_name = normalize_ident(left);
            let column_index = columns
                .iter()
                .position(|column| column.name == column_name)
                .ok_or_else(|| DbError::ColumnNotFound(column_name.clone()))?;

            if let Some(value) = &value {
                if !matches!(value, Value::Null) {
                    validate_value(&columns[column_index], value)?;
                }
            }
            for candidate in &values {
                if !matches!(candidate, Value::Null) {
                    validate_value(&columns[column_index], candidate)?;
                }
            }

            if matches!(operator, ComparisonOperator::In | ComparisonOperator::NotIn)
                && values.is_empty()
            {
                return Err(DbError::Parse("IN requires at least one value".into()));
            }
            if matches!(operator, ComparisonOperator::Like | ComparisonOperator::NotLike)
                && !matches!(value, Some(Value::Null))
                && columns[column_index].data_type != DataType::Text
            {
                return Err(DbError::TypeMismatch {
                    column: column_name,
                    expected: DataType::Text,
                });
            }

            parsed.push(SqlPredicate {
                column_index,
                operator,
                value,
                values,
                group,
            });
        }
    }

    Ok(parsed)
}

pub(super) fn split_or_conditions(input: &str) -> Vec<&str> {
    split_top_level_keyword(input, " OR ")
}

fn split_top_level_keyword<'a>(input: &'a str, keyword: &str) -> Vec<&'a str> {
    let mut result = Vec::new();
    let mut start = 0usize;
    let mut quote = None;
    let mut depth = 0usize;
    let bytes = input.as_bytes();
    let needle = keyword.as_bytes();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
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
        if byte == b'(' { depth += 1; cursor += 1; continue; }
        if byte == b')' { depth = depth.saturating_sub(1); cursor += 1; continue; }
        if depth == 0 && cursor + needle.len() <= bytes.len()
            && bytes[cursor..cursor + needle.len()].eq_ignore_ascii_case(needle)
        {
            result.push(input[start..cursor].trim());
            cursor += needle.len();
            start = cursor;
            continue;
        }
        cursor += 1;
    }
    result.push(input[start..].trim());
    result
}

pub(super) fn split_and_conditions(input: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0usize;
    let mut quote = None;
    let mut depth = 0usize;
    let mut between_pending = false;
    let bytes = input.as_bytes();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        let byte = bytes[cursor];
        if let Some(active) = quote {
            if byte == active {
                if bytes.get(cursor + 1) == Some(&active) { cursor += 2; continue; }
                quote = None;
            }
            cursor += 1;
            continue;
        }
        if byte == b'\'' || byte == b'"' { quote = Some(byte); cursor += 1; continue; }
        if byte == b'(' { depth += 1; cursor += 1; continue; }
        if byte == b')' { depth = depth.saturating_sub(1); cursor += 1; continue; }
        if depth == 0 && cursor + 9 <= bytes.len()
            && bytes[cursor..cursor + 9].eq_ignore_ascii_case(b" BETWEEN ")
        {
            between_pending = true;
            cursor += 9;
            continue;
        }
        if depth == 0 && cursor + 5 <= bytes.len()
            && bytes[cursor..cursor + 5].eq_ignore_ascii_case(b" AND ")
        {
            if between_pending {
                between_pending = false;
                cursor += 5;
                continue;
            }
            result.push(input[start..cursor].trim());
            cursor += 5;
            start = cursor;
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
 || next.is_alphanumeric() || !next.is_ascii() { cursor += next.len_utf8(); } else { break; }
            }
            tokens.push(SqlToken { kind: SqlTokenKind::Identifier(input[start..cursor].to_owned()), offset: start }); continue;
        }
        if byte == b'(' { tokens.push(SqlToken { kind: SqlTokenKind::LeftParen, offset: cursor }); cursor += 1; continue; }
        if byte == b')' { tokens.push(SqlToken { kind: SqlTokenKind::RightParen, offset: cursor }); cursor += 1; continue; }
        let two = if cursor + 1 < bytes.len() { &input[cursor..cursor + 2] } else { "" };
        if matches!(two, "<=" | ">=" | "<>" | "!=" | "==") {
            tokens.push(SqlToken { kind: SqlTokenKind::Operator(two.to_owned()), offset: cursor }); cursor += 2; continue;
        }
        if matches!(byte, b'+' | b'-' | b'*' | b'/' | b'%' | b'=' | b'<' | b'>') {
            tokens.push(SqlToken { kind: SqlTokenKind::Operator((byte as char).to_string()), offset: cursor }); cursor += 1; continue;
        }
        return Err(DbError::Parse(format!("unexpected character '{}' in scalar expression at byte {cursor}", ch)));
    }
    tokens.push(SqlToken { kind: SqlTokenKind::End, offset: input.len() });
    Ok(tokens)
}

struct ScalarExpressionParser<'a> { tokens: Vec<SqlToken>, cursor: usize, columns: &'a [Column] }
impl<'a> ScalarExpressionParser<'a> {
    fn new(input: &str, columns: &'a [Column]) -> Result<Self, DbError> {
        Ok(Self { tokens: tokenize_scalar_expression(input)?, cursor: 0, columns })
    }
    fn current(&self) -> &SqlTokenKind { &self.tokens[self.cursor].kind }
    fn advance(&mut self) -> SqlTokenKind {
        let kind = self.tokens[self.cursor].kind.clone();
        if !matches!(kind, SqlTokenKind::End) { self.cursor += 1; }
        kind
    }
    fn parse_value_expression(&mut self, minimum_binding_power: u8) -> Result<SqlValueExpression, DbError> {
        let mut left = match self.advance() {
            SqlTokenKind::Integer(value) => SqlValueExpression::Literal(Value::Integer(value)),
            SqlTokenKind::Text(value) => SqlValueExpression::Literal(Value::Text(value)),
            SqlTokenKind::Identifier(value) if value.eq_ignore_ascii_case("NULL") => SqlValueExpression::Literal(Value::Null),
            SqlTokenKind::Identifier(value) if value.eq_ignore_ascii_case("TRUE") => SqlValueExpression::Literal(Value::Boolean(true)),
            SqlTokenKind::Identifier(value) if value.eq_ignore_ascii_case("FALSE") => SqlValueExpression::Literal(Value::Boolean(false)),
            SqlTokenKind::Identifier(value) => {
                let name = normalize_ident(&value);
                let index = self.columns.iter().position(|column| column.name == name).ok_or_else(|| DbError::ColumnNotFound(name.clone()))?;
                SqlValueExpression::Column(index)
            }
            SqlTokenKind::Operator(op) if op == "+" => SqlValueExpression::UnaryPlus(Box::new(self.parse_value_expression(30)?)),
            SqlTokenKind::Operator(op) if op == "-" => SqlValueExpression::UnaryMinus(Box::new(self.parse_value_expression(30)?)),
            SqlTokenKind::LeftParen => {
                let expression = self.parse_value_expression(0)?;
                if !matches!(self.current(), SqlTokenKind::RightParen) { return Err(DbError::Parse("expected ')' in scalar expression".into())); }
                self.advance(); expression
            }
            other => return Err(DbError::Parse(format!("expected scalar value, found {other:?}"))),
        };
        loop {
            let (power, operation) = match self.current() {
                SqlTokenKind::Operator(op) if op == "+" => (10, 0),
                SqlTokenKind::Operator(op) if op == "-" => (10, 1),
                SqlTokenKind::Operator(op) if op == "*" => (20, 2),
                SqlTokenKind::Operator(op) if op == "/" => (20, 3),
                SqlTokenKind::Operator(op) if op == "%" => (20, 4),
                _ => break,
            };
            if power < minimum_binding_power { break; }
            self.advance();
            let right = self.parse_value_expression(power + 1)?;
            left = match operation {
                0 => SqlValueExpression::Add(Box::new(left), Box::new(right)),
                1 => SqlValueExpression::Subtract(Box::new(left), Box::new(right)),
                2 => SqlValueExpression::Multiply(Box::new(left), Box::new(right)),
                3 => SqlValueExpression::Divide(Box::new(left), Box::new(right)),
                4 => SqlValueExpression::Remainder(Box::new(left), Box::new(right)),
                _ => unreachable!(),
            };
        }
        Ok(left)
    }
    fn parse_comparison(mut self) -> Result<SqlExpression, DbError> {
        let left = self.parse_value_expression(0)?;
        let operator = match self.advance() {
            SqlTokenKind::Operator(op) => match op.as_str() {
                "=" | "==" => ComparisonOperator::Equal,
                "!=" | "<>" => ComparisonOperator::NotEqual,
                "<" => ComparisonOperator::Less,
                "<=" => ComparisonOperator::LessOrEqual,
                ">" => ComparisonOperator::Greater,
                ">=" => ComparisonOperator::GreaterOrEqual,
                _ => return Err(DbError::Parse(format!("expected comparison operator, found {op}"))),
            },
            other => return Err(DbError::Parse(format!("expected comparison operator, found {other:?}"))),
        };
        let right = self.parse_value_expression(0)?;
        if !matches!(self.current(), SqlTokenKind::End) { return Err(DbError::Parse(format!("unexpected token {:?} after scalar comparison", self.current()))); }
        Ok(SqlExpression::ValueComparison { left, operator, right })
    }
}
fn parse_scalar_comparison(columns: &[Column], input: &str) -> Result<SqlExpression, DbError> {
    ScalarExpressionParser::new(input, columns)?.parse_comparison()
}


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

fn parse_parenthesized_values(input: &str) -> Result<Vec<Value>, DbError> {
    let trimmed = input.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return Err(DbError::Parse("IN requires a parenthesized value list".into()));
    }
    let inner = trimmed[1..trimmed.len() - 1].trim();
    if inner.is_empty() {
        return Err(DbError::Parse("IN requires at least one value".into()));
    }
    split_csv(inner).into_iter().map(parse_value).collect()
}

pub(super) fn parse_where_expression(
    columns: &[Column],
    input: &str,
) -> Result<SqlExpression, DbError> {
    fn parse(columns: &[Column], source: &str) -> Result<SqlExpression, DbError> {
        let source = source.trim();
        if source.is_empty() {
            return Err(DbError::Parse("empty WHERE predicate".into()));
        }

        if source.starts_with('(') {
            let (inner, remainder) = take_parenthesized(source)?;
            if remainder.is_empty() {
                return parse(columns, inner);
            }
        }

        let disjunction = split_or_conditions(source);
        if disjunction.len() > 1 {
            return Ok(SqlExpression::Or(
                disjunction.iter().map(|part| parse(columns, part)).collect::<Result<Vec<_>, _>>()?
            ));
        }

        let conjunction = split_and_conditions(source);
        if conjunction.len() > 1 {
            return Ok(SqlExpression::And(
                conjunction.iter().map(|part| parse(columns, part)).collect::<Result<Vec<_>, _>>()?
            ));
        }

        let upper = source.to_ascii_uppercase();
        if upper.starts_with("NOT ") {
            return Ok(SqlExpression::Not(Box::new(parse(columns, source[4..].trim())?)));
        }
        if upper.starts_with("NOT(") {
            return Ok(SqlExpression::Not(Box::new(parse(columns, source[3..].trim())?)));
        }

        let predicates = parse_where_predicates(columns, source)?;
        if predicates.len() != 1 {
            return Err(DbError::Parse("invalid nested WHERE expression".into()));
        }
        Ok(SqlExpression::Predicate(predicates.into_iter().next().unwrap()))
    }

    parse(columns, input)
}

pub(super) fn parse_where_predicates(
    columns: &[Column],
    predicate: &str,
) -> Result<Vec<SqlPredicate>, DbError> {
    // AND binds more tightly than OR; each OR branch is one conjunction.
    let groups = split_or_conditions(predicate);
    if groups.is_empty() || groups.iter().any(|group| group.is_empty()) {
        return Err(DbError::Parse("empty WHERE predicate".into()));
    }

    let mut parsed = Vec::new();
    for (group, expression) in groups.iter().enumerate() {
        let conditions = split_and_conditions(expression);
        if conditions.is_empty() || conditions.iter().any(|condition| condition.is_empty()) {
            return Err(DbError::Parse("empty WHERE predicate".into()));
        }

        for condition in conditions {
            let trimmed = condition.trim();
            let (left, operator, value, values) =
                if let Some(position) = find_unquoted_keyword(trimmed, " NOT IN ") {
                    let list = parse_parenthesized_values(trimmed[position + 8..].trim())?;
                    (&trimmed[..position], ComparisonOperator::NotIn, None, list)
                } else if let Some(position) = find_unquoted_keyword(trimmed, " IN ") {
                    let list = parse_parenthesized_values(trimmed[position + 4..].trim())?;
                    (&trimmed[..position], ComparisonOperator::In, None, list)
                } else if let Some(position) = find_unquoted_keyword(trimmed, " NOT BETWEEN ") {
                    let bounds = split_and_conditions(trimmed[position + 13..].trim());
                    if bounds.len() != 2 {
                        return Err(DbError::Parse(
                            "BETWEEN requires lower AND upper bounds".into(),
                        ));
                    }
                    (
                        &trimmed[..position],
                        ComparisonOperator::NotBetween,
                        None,
                        vec![parse_value(bounds[0])?, parse_value(bounds[1])?],
                    )
                } else if let Some(position) = find_unquoted_keyword(trimmed, " BETWEEN ") {
                    let bounds = split_and_conditions(trimmed[position + 9..].trim());
                    if bounds.len() != 2 {
                        return Err(DbError::Parse(
                            "BETWEEN requires lower AND upper bounds".into(),
                        ));
                    }
                    (
                        &trimmed[..position],
                        ComparisonOperator::Between,
                        None,
                        vec![parse_value(bounds[0])?, parse_value(bounds[1])?],
                    )
                } else if let Some(position) =
                    find_unquoted_keyword(trimmed, " IS NOT DISTINCT FROM ")
                {
                    (
                        &trimmed[..position],
                        ComparisonOperator::IsNotDistinctFrom,
                        Some(parse_value(trimmed[position + " IS NOT DISTINCT FROM ".len()..].trim())?),
                        Vec::new(),
                    )
                } else if let Some(position) =
                    find_unquoted_keyword(trimmed, " IS DISTINCT FROM ")
                {
                    (
                        &trimmed[..position],
                        ComparisonOperator::IsDistinctFrom,
                        Some(parse_value(trimmed[position + " IS DISTINCT FROM ".len()..].trim())?),
                        Vec::new(),
                    )
                } else if let Some(position) =
                    find_unquoted_keyword(trimmed, " IS NOT NULL")
                {
                    (
                        &trimmed[..position],
                        ComparisonOperator::IsNotNull,
                        None,
                        Vec::new(),
                    )
                } else if let Some(position) = find_unquoted_keyword(trimmed, " IS NULL") {
                    (
                        &trimmed[..position],
                        ComparisonOperator::IsNull,
                        None,
                        Vec::new(),
                    )
                } else if let Some(position) = find_unquoted_keyword(trimmed, " NOT LIKE ") {
                    (
                        &trimmed[..position],
                        ComparisonOperator::NotLike,
                        Some(parse_value(trimmed[position + 10..].trim())?),
                        Vec::new(),
                    )
                } else if let Some(position) = find_unquoted_keyword(trimmed, " LIKE ") {
                    (
                        &trimmed[..position],
                        ComparisonOperator::Like,
                        Some(parse_value(trimmed[position + 6..].trim())?),
                        Vec::new(),
                    )
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
                    let found = operators
                        .iter()
                        .find_map(|(token, operator)| {
                            find_unquoted_keyword(trimmed, token)
                                .map(|position| (position, *operator, token.len()))
                        })
                        .ok_or_else(|| {
                            DbError::Parse(format!(
                                "unsupported WHERE predicate: {trimmed}"
                            ))
                        })?;
                    let (position, operator, width) = found;
                    (
                        &trimmed[..position],
                        operator,
                        Some(parse_value(trimmed[position + width..].trim())?),
                        Vec::new(),
                    )
                };

            let column_name = normalize_ident(left);
            let column_index = columns
                .iter()
                .position(|column| column.name == column_name)
                .ok_or_else(|| DbError::ColumnNotFound(column_name.clone()))?;

            if let Some(value) = &value {
                if !matches!(value, Value::Null) {
                    validate_value(&columns[column_index], value)?;
                }
            }
            for candidate in &values {
                if !matches!(candidate, Value::Null) {
                    validate_value(&columns[column_index], candidate)?;
                }
            }

            if matches!(operator, ComparisonOperator::In | ComparisonOperator::NotIn)
                && values.is_empty()
            {
                return Err(DbError::Parse("IN requires at least one value".into()));
            }
            if matches!(operator, ComparisonOperator::Like | ComparisonOperator::NotLike)
                && !matches!(value, Some(Value::Null))
                && columns[column_index].data_type != DataType::Text
            {
                return Err(DbError::TypeMismatch {
                    column: column_name,
                    expected: DataType::Text,
                });
            }

            parsed.push(SqlPredicate {
                column_index,
                operator,
                value,
                values,
                group,
            });
        }
    }

    Ok(parsed)
}

pub(super) fn split_or_conditions(input: &str) -> Vec<&str> {
    split_top_level_keyword(input, " OR ")
}

fn split_top_level_keyword<'a>(input: &'a str, keyword: &str) -> Vec<&'a str> {
    let mut result = Vec::new();
    let mut start = 0usize;
    let mut quote = None;
    let mut depth = 0usize;
    let bytes = input.as_bytes();
    let needle = keyword.as_bytes();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
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
        if byte == b'(' { depth += 1; cursor += 1; continue; }
        if byte == b')' { depth = depth.saturating_sub(1); cursor += 1; continue; }
        if depth == 0 && cursor + needle.len() <= bytes.len()
            && bytes[cursor..cursor + needle.len()].eq_ignore_ascii_case(needle)
        {
            result.push(input[start..cursor].trim());
            cursor += needle.len();
            start = cursor;
            continue;
        }
        cursor += 1;
    }
    result.push(input[start..].trim());
    result
}

pub(super) fn split_and_conditions(input: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0usize;
    let mut quote = None;
    let mut depth = 0usize;
    let mut between_pending = false;
    let bytes = input.as_bytes();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        let byte = bytes[cursor];
        if let Some(active) = quote {
            if byte == active {
                if bytes.get(cursor + 1) == Some(&active) { cursor += 2; continue; }
                quote = None;
            }
            cursor += 1;
            continue;
        }
        if byte == b'\'' || byte == b'"' { quote = Some(byte); cursor += 1; continue; }
        if byte == b'(' { depth += 1; cursor += 1; continue; }
        if byte == b')' { depth = depth.saturating_sub(1); cursor += 1; continue; }
        if depth == 0 && cursor + 9 <= bytes.len()
            && bytes[cursor..cursor + 9].eq_ignore_ascii_case(b" BETWEEN ")
        {
            between_pending = true;
            cursor += 9;
            continue;
        }
        if depth == 0 && cursor + 5 <= bytes.len()
            && bytes[cursor..cursor + 5].eq_ignore_ascii_case(b" AND ")
        {
            if between_pending {
                between_pending = false;
                cursor += 5;
                continue;
            }
            result.push(input[start..cursor].trim());
            cursor += 5;
            start = cursor;
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
