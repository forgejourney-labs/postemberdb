use eframe::egui;
use postemberdb::{CatalogSnapshot, Column, DataType, Database, QueryResult, RemoteClient, Row, TableInfo, Value};

const PAGE_SIZE: usize = 100;

#[derive(Clone)]
struct FieldDraft {
    text: String,
    is_null: bool,
}
impl FieldDraft {
    fn from_value(value: &Value) -> Self {
        Self { text: match value {
            Value::Null => String::new(),
            Value::Integer(value) => value.to_string(),
            Value::Text(value) => value.clone(),
            Value::Boolean(value) => if *value { "TRUE".into() } else { "FALSE".into() },
        }, is_null: matches!(value, Value::Null) }
    }
    fn empty(column: &Column) -> Self {
        Self {
            text: match &column.data_type {
                DataType::Boolean => "FALSE".into(),
                _ => String::new(),
            },
            is_null: column.nullable,
        }
    }
}

#[derive(Clone)]
struct RowDraft {
    table_name: String,
    original_values: Vec<Value>,
    fields: Vec<FieldDraft>,
}
#[derive(Clone)]
struct DeleteDraft {
    table_name: String,
    values: Vec<Value>,
}

struct BrowserApp {
    database_path: String,
    remote_address: String,
    remote_server_name: String,
    remote_ca_bundle: String,
    remote_username: String,
    remote_password: String,
    sql: String,
    database: Option<Database>,
    remote: Option<RemoteClient>,
    catalog: CatalogSnapshot,
    selected_table: Option<String>,
    page_offset: usize,
    browsing_table: bool,
    result: Option<QueryResult>,
    insert_draft: Option<RowDraft>,
    edit_draft: Option<RowDraft>,
    pending_delete: Option<DeleteDraft>,
    status: String,
    table_filter: String,
    query_history: Vec<String>,
}

impl Default for BrowserApp {
    fn default() -> Self {
        Self {
            database_path: "postemberdb.pbd".into(),
            remote_address: "127.0.0.1:7432".into(),
            remote_server_name: "localhost".into(),
            remote_ca_bundle: "server-ca.pem".into(),
            remote_username: String::new(),
            remote_password: String::new(),
            sql: "SELECT * FROM your_table LIMIT 100;".into(),
            database: None,
            remote: None,
            catalog: CatalogSnapshot::default(),
            selected_table: None,
            page_offset: 0,
            browsing_table: false,
            result: None,
            insert_draft: None,
            edit_draft: None,
            pending_delete: None,
            status: "No database opened".into(),
            table_filter: String::new(),
            query_history: Vec::new(),
        }
    }
}

fn valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
fn sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Integer(value) => value.to_string(),
        Value::Boolean(value) => if *value { "TRUE".into() } else { "FALSE".into() },
        Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
    }
}
fn field_value(field: &FieldDraft, column: &Column) -> Result<Value, String> {
    if field.is_null {
        return if column.nullable { Ok(Value::Null) } else { Err(format!("{} does not allow NULL", column.name)) };
    }
    match &column.data_type {
        DataType::Integer => field.text.trim().parse::<i64>()
            .map(Value::Integer).map_err(|_| format!("{} must be a signed 64-bit integer", column.name)),
        DataType::Text => Ok(Value::Text(field.text.clone())),
        DataType::Boolean => match field.text.trim().to_ascii_uppercase().as_str() {
            "TRUE" => Ok(Value::Boolean(true)),
            "FALSE" => Ok(Value::Boolean(false)),
            _ => Err(format!("{} must be TRUE or FALSE", column.name)),
        },
    }
}
fn key_predicate(table: &TableInfo, row_values: &[Value]) -> Result<(Vec<String>, String), String> {
    let unique = table.indexes.iter().find(|index| index.unique)
        .ok_or_else(|| "Editing and deleting require a UNIQUE index or PRIMARY KEY".to_owned())?;
    let mut terms = Vec::with_capacity(unique.columns.len());
    for name in &unique.columns {
        if !valid_identifier(name) { return Err("A key column name cannot be used safely in SQL".into()); }
        let column_index = table.columns.iter().position(|column| column.name == *name)
            .ok_or_else(|| format!("Key column {name} is missing"))?;
        let value = row_values.get(column_index)
            .ok_or_else(|| "The row does not contain every key column".to_owned())?;
        terms.push(format!("{name} = {}", sql_literal(value)));
    }
    Ok((unique.columns.clone(), terms.join(" AND ")))
}
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Integer(value) => serde_json::json!(value),
        Value::Text(value) => serde_json::json!(value),
        Value::Boolean(value) => serde_json::json!(value),
    }
}
fn csv_cell(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

impl BrowserApp {
    fn table_info(&self, name: &str) -> Option<TableInfo> {
        self.catalog.tables.iter().find(|table| table.name == name).cloned()
    }
    fn is_connected(&self) -> bool {
        self.database.is_some() || self.remote.is_some()
    }

    fn refresh_catalog(&mut self) {
        if let Some(remote) = self.remote.as_mut() {
            match remote.catalog() {
                Ok(catalog) => self.catalog = catalog,
                Err(error) => self.status = format!("Remote catalog failed: {error}"),
            }
        } else {
            self.catalog = self.database.as_ref().map(Database::catalog).unwrap_or_default();
        }
    }

    fn connect_remote(&mut self) {
        match RemoteClient::connect(
            self.remote_address.trim(),
            self.remote_server_name.trim(),
            self.remote_ca_bundle.trim(),
            self.remote_username.trim(),
            &self.remote_password,
        ) {
            Ok(mut remote) => match remote.catalog() {
                Ok(catalog) => {
                    let username = remote.username().to_owned();
                    self.database = None;
                    self.remote = Some(remote);
                    self.catalog = catalog;
                    self.selected_table = None;
                    self.page_offset = 0;
                    self.browsing_table = false;
                    self.result = None;
                    self.insert_draft = None;
                    self.edit_draft = None;
                    self.pending_delete = None;
                    self.remote_password.clear();
                    self.status = format!("Connected to {} as {}", self.remote_address, username);
                }
                Err(error) => self.status = format!("Connected, but catalog load failed: {error}"),
            },
            Err(error) => self.status = format!("Remote connection failed: {error}"),
        }
    }

    fn disconnect_remote(&mut self) {
        self.remote = None;
        self.catalog = CatalogSnapshot::default();
        self.selected_table = None;
        self.result = None;
        self.browsing_table = false;
        self.insert_draft = None;
        self.edit_draft = None;
        self.pending_delete = None;
        self.status = "Remote connection closed".into();
    }

    fn execute_write_batch(&mut self, statements: &[String]) -> Result<(), String> {
        if let Some(remote) = self.remote.as_mut() {
            remote.execute_batch(statements).map_err(|error| error.to_string())
        } else if let Some(database) = self.database.as_mut() {
            database.transaction(|transaction| {
                for statement in statements {
                    transaction.execute(statement)?;
                }
                Ok(())
            }).map_err(|error| error.to_string())
        } else {
            Err("Open a local database or connect to a remote server first".into())
        }
    }
    fn open_database(&mut self) {
        match Database::open(self.database_path.trim()) {
            Ok(database) => {
                self.catalog = database.catalog();
                self.remote = None;
                self.database = Some(database);
                self.selected_table = None;
                self.page_offset = 0;
                self.browsing_table = false;
                self.result = None;
                self.insert_draft = None;
                self.edit_draft = None;
                self.pending_delete = None;
                self.status = format!("Opened {}", self.database_path.trim());
            }
            Err(error) => {
                // Keep the currently active local or remote session intact when
                // opening a different file fails. A failed open is not a disconnect.
                self.status = format!("Open failed; current connection was kept: {error}");
            }
        }
    }
    fn execute_sql(&mut self) {
        if !self.is_connected() {
            self.status = "Open a database or connect to a remote server first".into();
            return;
        }
        self.browsing_table = false;
        self.insert_draft = None;
        self.edit_draft = None;
        self.pending_delete = None;
        let result = if let Some(remote) = self.remote.as_mut() {
            remote.execute(self.sql.trim()).map_err(|error| error.to_string())
        } else {
            self.database.as_mut().expect("connected local database").execute(self.sql.trim())
                .map_err(|error| error.to_string())
        };
        match result {
            Ok(result) => {
                self.status = result.message.clone();
                let executed = self.sql.trim().to_owned();
                if !executed.is_empty() {
                    self.query_history.retain(|entry| entry != &executed);
                    self.query_history.insert(0, executed);
                    self.query_history.truncate(20);
                }
                self.result = Some(result);
                self.refresh_catalog();
            }
            Err(error) => {
                self.result = None;
                self.status = format!("SQL error: {error}");
            }
        }
    }
    fn load_table_page(&mut self, offset: usize) {
        let Some(table_name) = self.selected_table.clone() else {
            self.status = "Choose a table in Explorer first".into();
            return;
        };
        let result = if let Some(remote) = self.remote.as_mut() {
            remote.read_table_page(&table_name, offset, PAGE_SIZE)
                .map(|page| page.result)
                .map_err(|error| error.to_string())
        } else if let Some(database) = self.database.as_ref() {
            database.read_table_page(&table_name, offset, PAGE_SIZE)
                .map_err(|error| error.to_string())
        } else {
            Err("Open a database or connect to a remote server first".into())
        };
        match result {
            Ok(result) => {
                self.page_offset = offset;
                self.browsing_table = true;
                self.status = result.message.clone();
                self.result = Some(result);
            }
            Err(error) => {
                self.result = None;
                self.status = format!("Could not load table page: {error}");
            }
        }
    }
    fn browse_table(&mut self, table_name: &str) {
        self.selected_table = Some(table_name.to_owned());
        self.insert_draft = None;
        self.edit_draft = None;
        self.pending_delete = None;
        self.load_table_page(0);
    }
    fn total_rows(&self) -> usize {
        self.selected_table.as_ref()
            .and_then(|name| self.catalog.tables.iter().find(|table| &table.name == name))
            .map(|table| table.row_count).unwrap_or(0)
    }
    fn can_edit_selected_table(&self) -> bool {
        self.selected_table.as_ref()
            .and_then(|name| self.table_info(name))
            .is_some_and(|table| table.indexes.iter().any(|index| index.unique)
                && valid_identifier(&table.name)
                && table.columns.iter().all(|column| valid_identifier(&column.name)))
    }
    fn can_insert_selected_table(&self) -> bool {
        self.selected_table.as_ref()
            .and_then(|name| self.table_info(name))
            .is_some_and(|table| valid_identifier(&table.name)
                && table.columns.iter().all(|column| valid_identifier(&column.name)))
    }
    fn begin_insert(&mut self) {
        let Some(table_name) = self.selected_table.clone() else { return; };
        let Some(info) = self.table_info(&table_name) else { return; };
        if !info.columns.iter().all(|column| valid_identifier(&column.name)) || !valid_identifier(&table_name) {
            self.status = "This table contains identifiers that the editor cannot safely use".into();
            return;
        }
        self.insert_draft = Some(RowDraft {
            table_name,
            original_values: Vec::new(),
            fields: info.columns.iter().map(FieldDraft::empty).collect(),
        });
        self.edit_draft = None;
    }
    fn begin_edit(&mut self, row: &Row) {
        let Some(table_name) = self.selected_table.clone() else { return; };
        let Some(info) = self.table_info(&table_name) else { return; };
        if key_predicate(&info, &row.values).is_err() {
            self.status = "Editing requires a UNIQUE index or PRIMARY KEY".into();
            return;
        }
        self.edit_draft = Some(RowDraft {
            table_name,
            original_values: row.values.clone(),
            fields: row.values.iter().map(FieldDraft::from_value).collect(),
        });
        self.insert_draft = None;
        self.pending_delete = None;
    }
    fn request_delete(&mut self, row: &Row) {
        let Some(table_name) = self.selected_table.clone() else { return; };
        self.pending_delete = Some(DeleteDraft { table_name, values: row.values.clone() });
        self.edit_draft = None;
    }
    fn save_insert(&mut self) {
        let Some(draft) = self.insert_draft.clone() else { return; };
        let Some(info) = self.table_info(&draft.table_name) else {
            self.status = "Table metadata is unavailable".into();
            return;
        };
        let mut values = Vec::with_capacity(info.columns.len());
        for (field, column) in draft.fields.iter().zip(info.columns.iter()) {
            match field_value(field, column) {
                Ok(value) => values.push(value),
                Err(error) => { self.status = error; return; }
            }
        }
        let columns = info.columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>();
        let statement = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            draft.table_name,
            columns.join(", "),
            values.iter().map(sql_literal).collect::<Vec<_>>().join(", ")
        );
        let result = if let Some(remote) = self.remote.as_mut() {
            remote.execute(&statement).map_err(|error| error.to_string())
        } else if let Some(database) = self.database.as_mut() {
            database.execute(&statement).map_err(|error| error.to_string())
        } else {
            Err("Open a database or connect to a remote server first".into())
        };
        match result {
            Ok(result) => {
                self.status = result.message;
                self.insert_draft = None;
                self.refresh_catalog();
                self.load_table_page(self.page_offset);
            }
            Err(error) => self.status = format!("Insert failed: {error}"),
        }
    }
    fn save_edit(&mut self) {
        let Some(draft) = self.edit_draft.clone() else { return; };
        let Some(info) = self.table_info(&draft.table_name) else {
            self.status = "Table metadata is unavailable".into();
            return;
        };
        let (key_columns, predicate) = match key_predicate(&info, &draft.original_values) {
            Ok(value) => value,
            Err(error) => { self.status = error; return; }
        };
        if !valid_identifier(&draft.table_name) {
            self.status = "Table name cannot be used safely in SQL".into();
            return;
        }
        let mut statements = Vec::new();
        for (index, (field, column)) in draft.fields.iter().zip(info.columns.iter()).enumerate() {
            if key_columns.contains(&column.name) { continue; }
            if !valid_identifier(&column.name) {
                self.status = format!("Column {} cannot be used safely in SQL", column.name);
                return;
            }
            let value = match field_value(field, column) {
                Ok(value) => value,
                Err(error) => { self.status = error; return; }
            };
            if draft.original_values.get(index) != Some(&value) {
                statements.push(format!(
                    "UPDATE {} SET {} = {} WHERE {}",
                    draft.table_name, column.name, sql_literal(&value), predicate
                ));
            }
        }
        if statements.is_empty() {
            self.status = "No editable fields were changed".into();
            self.edit_draft = None;
            return;
        }
        let result = self.execute_write_batch(&statements);
        match result {
            Ok(()) => {
                self.status = format!("Updated {} field(s) in one transaction", statements.len());
                self.edit_draft = None;
                self.refresh_catalog();
                self.load_table_page(self.page_offset);
            }
            Err(error) => self.status = format!("Update failed: {error}"),
        }
    }
    fn confirm_delete(&mut self) {
        let Some(draft) = self.pending_delete.clone() else { return; };
        let Some(info) = self.table_info(&draft.table_name) else {
            self.status = "Table metadata is unavailable".into();
            return;
        };
        let (_, predicate) = match key_predicate(&info, &draft.values) {
            Ok(value) => value,
            Err(error) => { self.status = error; return; }
        };
        let statement = format!("DELETE FROM {} WHERE {}", draft.table_name, predicate);
        let result = if let Some(remote) = self.remote.as_mut() {
            remote.execute(&statement).map_err(|error| error.to_string())
        } else if let Some(database) = self.database.as_mut() {
            database.execute(&statement).map_err(|error| error.to_string())
        } else {
            Err("Open a database or connect to a remote server first".into())
        };
        match result {
            Ok(result) if result.affected_rows == 1 => {
                self.status = "Row deleted".into();
                self.pending_delete = None;
                self.refresh_catalog();
                self.load_table_page(self.page_offset);
            }
            Ok(result) => self.status = format!("Delete stopped: expected one row, removed {}", result.affected_rows),
            Err(error) => self.status = format!("Delete failed: {error}"),
        }
    }
    fn export_result(&mut self, format: &str) {
        let Some(result) = self.result.as_ref() else {
            self.status = "There are no results to export".into();
            return;
        };
        if result.columns.is_empty() {
            self.status = "The current result has no rows or columns to export".into();
            return;
        }
        let raw_name = self.selected_table.as_deref().unwrap_or("query-results");
        let base_name = raw_name.chars()
            .map(|ch| if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' { ch } else { '_' })
            .collect::<String>();
        let filename = if self.browsing_table {
            format!("{base_name}.page-{}.{}", self.page_offset / PAGE_SIZE + 1, format)
        } else {
            format!("{base_name}.{format}")
        };
        let content = if format == "csv" {
            let mut lines = Vec::with_capacity(result.rows.len() + 1);
            lines.push(result.columns.iter().map(|value| csv_cell(value)).collect::<Vec<_>>().join(","));
            for row in &result.rows {
                lines.push(row.values.iter().map(|value| csv_cell(&value.to_string())).collect::<Vec<_>>().join(","));
            }
            format!("\u{feff}{}", lines.join("\r\n"))
        } else {
            let rows = result.rows.iter().map(|row| {
                let mut object = serde_json::Map::new();
                for (column, value) in result.columns.iter().zip(row.values.iter()) {
                    object.insert(column.clone(), value_to_json(value));
                }
                serde_json::Value::Object(object)
            }).collect::<Vec<_>>();
            match serde_json::to_string_pretty(&rows) {
                Ok(json) => json,
                Err(error) => {
                    self.status = format!("JSON export failed: {error}");
                    return;
                }
            }
        };
        match std::fs::write(&filename, content) {
            Ok(()) => self.status = format!("Exported {}", filename),
            Err(error) => self.status = format!("Export failed: {error}"),
        }
    }
    fn render_field(ui: &mut egui::Ui, field: &mut FieldDraft, column: &Column, enabled: bool) {
        ui.horizontal(|ui| {
            ui.label(&column.name);
            ui.add_enabled_ui(enabled && !field.is_null, |ui| {
                ui.add(egui::TextEdit::singleline(&mut field.text).desired_width(190.0));
            });
            if column.nullable && enabled {
                ui.checkbox(&mut field.is_null, "NULL");
            } else if !enabled {
                ui.weak("key");
            }
        });
    }
    fn render_insert_form(&mut self, ui: &mut egui::Ui) {
        let Some(table_name) = self.insert_draft.as_ref().map(|draft| draft.table_name.clone()) else { return; };
        let Some(info) = self.table_info(&table_name) else { return; };
        let mut save = false;
        let mut cancel = false;
        ui.group(|ui| {
            ui.heading(format!("Insert row into {table_name}"));
            if let Some(draft) = self.insert_draft.as_mut() {
                for (field, column) in draft.fields.iter_mut().zip(info.columns.iter()) {
                    Self::render_field(ui, field, column, true);
                }
            }
            ui.horizontal(|ui| {
                save = ui.button("Insert row").clicked();
                cancel = ui.button("Cancel").clicked();
            });
        });
        if save { self.save_insert(); }
        if cancel { self.insert_draft = None; }
    }
    fn render_edit_form(&mut self, ui: &mut egui::Ui) {
        let Some(table_name) = self.edit_draft.as_ref().map(|draft| draft.table_name.clone()) else { return; };
        let Some(info) = self.table_info(&table_name) else { return; };
        let key_columns = info.indexes.iter().find(|index| index.unique)
            .map(|index| index.columns.clone()).unwrap_or_default();
        let mut save = false;
        let mut cancel = false;
        ui.group(|ui| {
            ui.heading(format!("Edit row in {table_name}"));
            ui.label("Primary/unique key fields are locked so the row can be identified safely.");
            if let Some(draft) = self.edit_draft.as_mut() {
                for (field, column) in draft.fields.iter_mut().zip(info.columns.iter()) {
                    Self::render_field(ui, field, column, !key_columns.contains(&column.name));
                }
            }
            ui.horizontal(|ui| {
                save = ui.button("Save changes").clicked();
                cancel = ui.button("Cancel").clicked();
            });
        });
        if save { self.save_edit(); }
        if cancel { self.edit_draft = None; }
    }
}

impl eframe::App for BrowserApp {
    fn update(&mut self, context: &egui::Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("main_menu").show(context, |ui| {
            ui.horizontal(|ui| {
                ui.strong("PostemberDB Browser");
                ui.separator();
                if ui.button("Open database").clicked() { self.open_database(); }
                if ui.button("Refresh catalog").clicked() { self.refresh_catalog(); }
                if ui.button("Run SQL  ▶").clicked() { self.execute_sql(); }
                ui.separator();
                ui.small(format!("{} table(s)", self.catalog.tables.len()));
            });
        });

        egui::SidePanel::left("connection_panel")
            .default_width(265.0)
            .resizable(true)
            .show(context, |ui| {
                ui.heading("Remote TLS connection");
                ui.label("Server address");
                ui.text_edit_singleline(&mut self.remote_address);
                ui.label("TLS server name");
                ui.text_edit_singleline(&mut self.remote_server_name);
                ui.label("Trusted CA PEM bundle");
                ui.text_edit_singleline(&mut self.remote_ca_bundle);
                ui.label("Username");
                ui.text_edit_singleline(&mut self.remote_username);
                ui.label("Password");
                ui.add(egui::TextEdit::singleline(&mut self.remote_password).password(true));
                ui.horizontal(|ui| {
                    if ui.button("Connect securely").clicked() { self.connect_remote(); }
                    if self.remote.is_some() && ui.button("Disconnect").clicked() { self.disconnect_remote(); }
                });
                if let Some(remote) = self.remote.as_ref() {
                    ui.small(format!("Remote session: {}", remote.username()));
                }
                ui.separator();
                ui.heading("Database");
                ui.separator();
                ui.label("Database file");
                ui.text_edit_singleline(&mut self.database_path);
                if ui.button("Open").clicked() { self.open_database(); }
                ui.add_space(8.0);
                ui.label(if self.remote.is_some() { "● Connected remotely · TLS" }
                    else if self.database.is_some() { "● Connected locally" }
                    else { "○ Disconnected" });
                ui.separator();
                ui.heading("Explorer");
                if self.is_connected() {
                    ui.label("Filter tables");
                    ui.text_edit_singleline(&mut self.table_filter);
                }
                if !self.is_connected() {
                    ui.weak("Open a .pbd or legacy .db file, or connect to a remote TLS server.");
                } else if self.catalog.tables.is_empty() {
                    ui.weak("No SQL tables in this database.");
                } else {
                    let tables = self.catalog.tables.clone();
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        for table in tables {
                            let table_name = table.name.clone();
                            if !table_name.to_ascii_lowercase().contains(&self.table_filter.trim().to_ascii_lowercase()) {
                                continue;
                            }
                            let selected = self.selected_table.as_deref() == Some(table_name.as_str());
                            ui.horizontal(|ui| {
                                if ui.selectable_label(selected, format!("▦ {table_name}")).clicked() {
                                    self.browse_table(&table_name);
                                }
                                ui.small(format!("{} rows", table.row_count));
                            });
                            ui.collapsing(format!("Structure · {} column(s)", table.columns.len()), |ui| {
                                for column in &table.columns {
                                    let nullability = if column.nullable { "NULL" } else { "NOT NULL" };
                                    ui.label(format!("{}  {:?}  {}", column.name, column.data_type, nullability));
                                }
                                if !table.indexes.is_empty() {
                                    ui.separator();
                                    ui.weak("Indexes");
                                    for index in &table.indexes {
                                        let uniqueness = if index.unique { "UNIQUE" } else { "INDEX" };
                                        ui.label(format!("{} · {} ({})", index.name, uniqueness, index.columns.join(", ")));
                                    }
                                }
                            });
                        }
                    });
                }
                ui.add_space(10.0);
                ui.heading("Connections");
                if self.database.is_some() { ui.label("Local / embedded"); }
                if self.remote.is_some() { ui.label("Remote / verified TLS"); }
            });

        egui::CentralPanel::default().show(context, |ui| {
            ui.heading("SQL editor");
            if !self.query_history.is_empty() {
                egui::ComboBox::from_label("Recent queries")
                    .selected_text("Choose a previous query")
                    .show_ui(ui, |ui| {
                        for query in self.query_history.clone() {
                            let label = query.chars().take(72).collect::<String>();
                            let label = if query.chars().count() > 72 { format!("{label}…") } else { label };
                            if ui.selectable_label(false, label).clicked() {
                                self.sql = query;
                            }
                        }
                    });
            }
            ui.add(egui::TextEdit::multiline(&mut self.sql)
                .desired_rows(8).code_editor().desired_width(f32::INFINITY));
            ui.horizontal(|ui| {
                if ui.button("Execute").clicked() { self.execute_sql(); }
                if ui.button("Clear").clicked() {
                    self.sql.clear();
                    self.result = None;
                    self.browsing_table = false;
                }
                if let Some(table) = &self.selected_table {
                    ui.separator();
                    ui.small(format!("Selected: {table}"));
                }
            });
            ui.separator();
            ui.heading("Results");
            if self.browsing_table {
                let total = self.total_rows();
                let shown = self.result.as_ref().map(|result| result.rows.len()).unwrap_or(0);
                ui.horizontal(|ui| {
                    if ui.add_enabled(self.page_offset > 0, egui::Button::new("◀ Previous")).clicked() {
                        self.load_table_page(self.page_offset.saturating_sub(PAGE_SIZE));
                    }
                    if ui.add_enabled(self.page_offset + shown < total, egui::Button::new("Next ▶")).clicked() {
                        self.load_table_page(self.page_offset.saturating_add(PAGE_SIZE));
                    }
                    ui.label(format!("Rows {}–{} of {}", if shown == 0 {0} else {self.page_offset + 1}, self.page_offset + shown, total));
                    if self.can_insert_selected_table() && ui.button("＋ Insert row").clicked() {
                        self.begin_insert();
                    }
                });
                if self.selected_table.as_ref().is_some_and(|name| !self.can_edit_selected_table()) {
                    ui.weak("Row editing requires a UNIQUE index or PRIMARY KEY and simple SQL identifiers.");
                }
            }
            ui.horizontal(|ui| {
                if ui.button("Export CSV").clicked() { self.export_result("csv"); }
                if ui.button("Export JSON").clicked() { self.export_result("json"); }
            });

            self.render_insert_form(ui);
            self.render_edit_form(ui);

            if let Some(pending) = self.pending_delete.clone() {
                ui.group(|ui| {
                    ui.horizontal(|ui| {
                        ui.colored_label(egui::Color32::from_rgb(160, 50, 50), format!("Delete a row from {}?", pending.table_name));
                        if ui.button("Confirm delete").clicked() { self.confirm_delete(); }
                        if ui.button("Cancel").clicked() { self.pending_delete = None; }
                    });
                });
            }

            if let Some(result) = self.result.clone() {
                ui.label(format!("{} row(s) returned · {} affected", result.rows.len(), result.affected_rows));
                let table_info = self.selected_table.as_ref().and_then(|name| self.table_info(name));
                let show_actions = self.browsing_table && table_info.as_ref()
                    .is_some_and(|table| table.indexes.iter().any(|index| index.unique)
                        && valid_identifier(&table.name)
                        && table.columns.iter().all(|column| valid_identifier(&column.name)));
                if !result.columns.is_empty() {
                    egui::ScrollArea::both().auto_shrink([false, false]).show(ui, |ui| {
                        egui::Grid::new("result_grid").striped(true).show(ui, |ui| {
                            for column in &result.columns { ui.strong(column); }
                            if show_actions { ui.strong("Actions"); }
                            ui.end_row();
                            for row in &result.rows {
                                for value in &row.values { ui.label(value.to_string()); }
                                if show_actions {
                                    if ui.button("Edit").clicked() { self.begin_edit(row); }
                                    if ui.button("Delete").clicked() { self.request_delete(row); }
                                }
                                ui.end_row();
                            }
                        });
                    });
                } else {
                    ui.label(&result.message);
                }
            } else {
                ui.label("Choose a table in Explorer or run a SQL query.");
            }
        });

        egui::TopBottomPanel::bottom("status_bar").show(context, |ui| {
            ui.horizontal(|ui| {
                ui.small(if self.remote.is_some() { "PostemberDB Browser · remote TLS mode" }
                    else { "PostemberDB Browser · local mode" });
                ui.separator();
                ui.small(&self.status);
            });
        });
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1120.0, 740.0])
            .with_min_inner_size([780.0, 540.0]),
        ..Default::default()
    };
    eframe::run_native(
        "PostemberDB Browser",
        options,
        Box::new(|_creation_context| Ok(Box::<BrowserApp>::default())),
    )
}
