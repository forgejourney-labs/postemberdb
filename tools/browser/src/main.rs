use eframe::egui;
use postemberdb::{CatalogSnapshot, Database, QueryResult, Value};

const PAGE_SIZE: usize = 100;

struct BrowserApp {
    database_path: String,
    sql: String,
    database: Option<Database>,
    catalog: CatalogSnapshot,
    selected_table: Option<String>,
    page_offset: usize,
    browsing_table: bool,
    result: Option<QueryResult>,
    status: String,
}

impl Default for BrowserApp {
    fn default() -> Self {
        Self {
            database_path: "postemberdb.pbd".into(),
            sql: "SELECT * FROM your_table LIMIT 100;".into(),
            database: None,
            catalog: CatalogSnapshot::default(),
            selected_table: None,
            page_offset: 0,
            browsing_table: false,
            result: None,
            status: "No database opened".into(),
        }
    }
}

impl BrowserApp {
    fn refresh_catalog(&mut self) {
        self.catalog = self.database.as_ref().map(Database::catalog).unwrap_or_default();
    }

    fn open_database(&mut self) {
        match Database::open(self.database_path.trim()) {
            Ok(database) => {
                self.catalog = database.catalog();
                self.database = Some(database);
                self.selected_table = None;
                self.page_offset = 0;
                self.browsing_table = false;
                self.result = None;
                self.status = format!("Opened {}", self.database_path.trim());
            }
            Err(error) => {
                self.database = None;
                self.catalog = CatalogSnapshot::default();
                self.selected_table = None;
                self.browsing_table = false;
                self.result = None;
                self.status = format!("Open failed: {error}");
            }
        }
    }

    fn execute_sql(&mut self) {
        let Some(database) = self.database.as_mut() else {
            self.status = "Open a database first".into();
            return;
        };
        self.browsing_table = false;
        match database.execute(self.sql.trim()) {
            Ok(result) => {
                self.status = result.message.clone();
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
        let Some(database) = self.database.as_ref() else {
            self.status = "Open a database first".into();
            return;
        };
        match database.read_table_page(&table_name, offset, PAGE_SIZE) {
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
        self.load_table_page(0);
    }

    fn total_rows(&self) -> usize {
        self.selected_table.as_ref()
            .and_then(|name| self.catalog.tables.iter().find(|table| &table.name == name))
            .map(|table| table.row_count)
            .unwrap_or(0)
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

        let base_name = self.selected_table.as_deref().unwrap_or("query-results");
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
            serde_json::to_string_pretty(&rows).unwrap_or_else(|error| format!("JSON export failed: {error}"))
        };

        match std::fs::write(&filename, content) {
            Ok(()) => self.status = format!("Exported {}", filename),
            Err(error) => self.status = format!("Export failed: {error}"),
        }
    }
}

fn csv_cell(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Integer(value) => serde_json::json!(value),
        Value::Text(value) => serde_json::json!(value),
        Value::Boolean(value) => serde_json::json!(value),
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
                ui.heading("Database");
                ui.separator();
                ui.label("Database file");
                ui.text_edit_singleline(&mut self.database_path);
                if ui.button("Open").clicked() { self.open_database(); }
                ui.add_space(8.0);
                ui.label(if self.database.is_some() { "● Connected locally" } else { "○ Disconnected" });
                ui.separator();
                ui.heading("Explorer");
                if self.database.is_none() {
                    ui.weak("Open a .pbd or legacy .db file to inspect it.");
                } else if self.catalog.tables.is_empty() {
                    ui.weak("No SQL tables in this database.");
                } else {
                    let tables = self.catalog.tables.clone();
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        for table in tables {
                            let table_name = table.name.clone();
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
                ui.label("Local / embedded");
                ui.add_enabled(false, egui::Button::new("Remote server (coming later)"));
            });

        egui::CentralPanel::default().show(context, |ui| {
            ui.heading("SQL editor");
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
                });
            }
            ui.horizontal(|ui| {
                if ui.button("Export CSV").clicked() { self.export_result("csv"); }
                if ui.button("Export JSON").clicked() { self.export_result("json"); }
            });
            if let Some(result) = &self.result {
                ui.label(format!("{} row(s) returned · {} affected", result.rows.len(), result.affected_rows));
                if !result.columns.is_empty() {
                    egui::ScrollArea::both().auto_shrink([false, false]).show(ui, |ui| {
                        egui::Grid::new("result_grid").striped(true).show(ui, |ui| {
                            for column in &result.columns { ui.strong(column); }
                            ui.end_row();
                            for row in &result.rows {
                                for value in &row.values { ui.label(value.to_string()); }
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
                ui.small("PostemberDB Browser · local mode");
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
