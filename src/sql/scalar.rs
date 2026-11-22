use super::{Column, ComparisonOperator, DbError, SqlExpression, SqlValueExpression, Value};

#[derive(Debug, Clone, PartialEq)]
enum Token { Name(String), Number(i64), Text(String), Op(String), Open, Close, Comma, End }

fn tokenize(input: &str) -> Result<Vec<Token>, DbError> {
    let b = input.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() { i += 1; continue; }
        if c == b'-' && b.get(i + 1) == Some(&b'-') {
            i += 2;
            while i < b.len() && b[i] != b'\n' { i += 1; }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') { i += 1; }
            if i + 1 >= b.len() { return Err(DbError::Parse("unclosed SQL comment".into())); }
            i += 2;
            continue;
        }
        if c == b'\'' {
            i += 1;
            let mut value = String::new();
            let mut closed = false;
            while i < b.len() {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') { value.push('\''); i += 2; continue; }
                    i += 1; closed = true; break;
                }
                let ch = input[i..].chars().next().ok_or_else(|| DbError::Parse("invalid SQL string".into()))?;
                value.push(ch); i += ch.len_utf8();
            }
            if !closed { return Err(DbError::Parse("unclosed SQL string".into())); }
            out.push(Token::Text(value));
            continue;
        }
        if c == b'"' || c == 0x60 || c == b'[' {
            let close = if c == b'[' { b']' } else { c };
            i += 1;
            let mut value = String::new();
            let mut closed = false;
            while i < b.len() {
                if b[i] == close {
                    if b.get(i + 1) == Some(&close) { value.push(close as char); i += 2; continue; }
                    i += 1; closed = true; break;
                }
                let ch = input[i..].chars().next().ok_or_else(|| DbError::Parse("invalid quoted identifier".into()))?;
                value.push(ch); i += ch.len_utf8();
            }
            if !closed { return Err(DbError::Parse("unclosed quoted identifier".into())); }
            out.push(Token::Name(value));
            continue;
        }
        if c.is_ascii_digit() {
            let start = i;
            while i < b.len() && b[i].is_ascii_digit() { i += 1; }
            if i < b.len() && matches!(b[i], b'.' | b'e' | b'E') {
                return Err(DbError::Unsupported("REAL literals are not supported by the current Value model".into()));
            }
            let n = input[start..i].parse::<i64>().map_err(|_| DbError::Parse("integer literal out of range".into()))?;
            out.push(Token::Number(n));
            continue;
        }
        let ch = input[i..].chars().next().ok_or_else(|| DbError::Parse("invalid expression".into()))?;
        if ch == '_' || ch.is_alphabetic() || !ch.is_ascii() {
            let start = i;
            i += ch.len_utf8();
            while i < b.len() {
                let next = input[i..].chars().next().expect("UTF-8 boundary");
                if next == '_' || next == '$' || next.is_alphanumeric() || !next.is_ascii() { i += next.len_utf8(); }
                else { break; }
            }
            out.push(Token::Name(input[start..i].to_owned()));
            continue;
        }
        if c == b'(' { out.push(Token::Open); i += 1; continue; }
        if c == b')' { out.push(Token::Close); i += 1; continue; }
        if c == b',' { out.push(Token::Comma); i += 1; continue; }
        let pair = if i + 1 < b.len() { &input[i..i + 2] } else { "" };
        if matches!(pair, "<=" | ">=" | "<>" | "!=" | "==") {
            out.push(Token::Op(pair.to_owned())); i += 2; continue;
        }
        if matches!(c, b'+' | b'-' | b'*' | b'/' | b'%' | b'=' | b'<' | b'>') {
            out.push(Token::Op((c as char).to_string())); i += 1; continue;
        }
        return Err(DbError::Parse(format!("unexpected character '{ch}' in scalar expression")));
    }
    out.push(Token::End);
    Ok(out)
}

struct Parser<'a> { tokens: Vec<Token>, position: usize, columns: &'a [Column] }

impl<'a> Parser<'a> {
    fn new(input: &str, columns: &'a [Column]) -> Result<Self, DbError> {
        Ok(Self { tokens: tokenize(input)?, position: 0, columns })
    }
    fn current(&self) -> &Token { &self.tokens[self.position] }
    fn pop(&mut self) -> Token {
        let t = self.tokens[self.position].clone();
        if !matches!(t, Token::End) { self.position += 1; }
        t
    }
    fn value(&mut self, min: u8) -> Result<SqlValueExpression, DbError> {
        let mut left = match self.pop() {
            Token::Number(n) => SqlValueExpression::Literal(Value::Integer(n)),
            Token::Text(s) => SqlValueExpression::Literal(Value::Text(s)),
            Token::Name(s) if s.eq_ignore_ascii_case("NULL") => SqlValueExpression::Literal(Value::Null),
            Token::Name(s) if s.eq_ignore_ascii_case("TRUE") => SqlValueExpression::Literal(Value::Boolean(true)),
            Token::Name(s) if s.eq_ignore_ascii_case("FALSE") => SqlValueExpression::Literal(Value::Boolean(false)),
            Token::Name(name) if matches!(self.current(), Token::Open) => {
                self.pop();
                let mut arguments = Vec::new();
                if !matches!(self.current(), Token::Close) {
                    loop {
                        arguments.push(self.value(0)?);
                        match self.current() {
                            Token::Comma => { self.pop(); }
                            Token::Close => break,
                            token => return Err(DbError::Parse(format!(
                                "expected ',' or ')' in function call, found {token:?}"
                            ))),
                        }
                    }
                }
                self.pop();
                SqlValueExpression::Function {
                    name: name.to_ascii_uppercase(),
                    arguments,
                }
            }
            Token::Name(s) => {
                let name = s.trim().to_ascii_lowercase();
                let index = self.columns.iter().position(|c| c.name == name).ok_or_else(|| DbError::ColumnNotFound(name.clone()))?;
                SqlValueExpression::Column(index)
            }
            Token::Op(s) if s == "+" => SqlValueExpression::UnaryPlus(Box::new(self.value(30)?)),
            Token::Op(s) if s == "-" => SqlValueExpression::UnaryMinus(Box::new(self.value(30)?)),
            Token::Open => {
                let nested = self.value(0)?;
                if !matches!(self.current(), Token::Close) { return Err(DbError::Parse("expected ')'".into())); }
                self.pop(); nested
            }
            other => return Err(DbError::Parse(format!("expected scalar value, found {other:?}"))),
        };
        loop {
            let (bp, op) = match self.current() {
                Token::Op(s) if s == "+" => (10, 0),
                Token::Op(s) if s == "-" => (10, 1),
                Token::Op(s) if s == "*" => (20, 2),
                Token::Op(s) if s == "/" => (20, 3),
                Token::Op(s) if s == "%" => (20, 4),
                _ => break,
            };
            if bp < min { break; }
            self.pop();
            let right = self.value(bp + 1)?;
            left = match op {
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
    fn comparison(mut self) -> Result<SqlExpression, DbError> {
        let left = self.value(0)?;
        let op = match self.pop() {
            Token::Op(s) => match s.as_str() {
                "=" | "==" => ComparisonOperator::Equal,
                "!=" | "<>" => ComparisonOperator::NotEqual,
                "<" => ComparisonOperator::Less,
                "<=" => ComparisonOperator::LessOrEqual,
                ">" => ComparisonOperator::Greater,
                ">=" => ComparisonOperator::GreaterOrEqual,
                _ => return Err(DbError::Parse("unsupported scalar comparison operator".into())),
            },
            _ => return Err(DbError::Parse("expected scalar comparison operator".into())),
        };
        let right = self.value(0)?;
        if !matches!(self.current(), Token::End) { return Err(DbError::Parse("unexpected token after scalar comparison".into())); }
        Ok(SqlExpression::ValueComparison { left, operator: op, right })
    }
}

pub(super) fn parse_scalar_value_expression(
    columns: &[Column],
    input: &str,
) -> Result<SqlValueExpression, DbError> {
    let mut parser = Parser::new(input, columns)?;
    let expression = parser.value(0)?;
    if !matches!(parser.current(), Token::End) {
        return Err(DbError::Parse("unexpected token after scalar value expression".into()));
    }
    Ok(expression)
}

pub(super) fn parse_scalar_comparison(columns: &[Column], input: &str) -> Result<SqlExpression, DbError> {
    Parser::new(input, columns)?.comparison()
}
