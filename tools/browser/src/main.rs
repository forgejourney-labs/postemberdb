use eframe::egui;
use postemberdb::{Database, QueryResult};

struct BrowserApp {
    database_path: String,
    sql: String,
    database: Option<Database>,
    result: Option<QueryResult>,
    status: String,
}

impl Default for BrowserApp {
    fn default() -> Self {
        Self {
            database_path: "postemberdb.pbd".into(),
            sql: "SELECT * FROM your_table LIMIT 100;".into(),
            database: None,
            result: None,
            status: "No database opened".into(),
        }
    }
}

impl BrowserApp {
    fn open_database(&mut self) {
        match Database::open(self.database_path.trim()) {
            Ok(database) => {
                self.database = Some(database);
                self.result = None;
                self.status = format!("Opened {}", self.database_path.trim());
            }
            Err(error) => {
                self.database = None;
                self.status = format!("Open failed: {error}");
            }
        }
    }

    fn execute_sql(&mut self) {
        let Some(database) = self.database.as_mut() else {
            self.status = "Open a database first".into();
            return;
        };
        match database.execute(self.sql.trim()) {
            Ok(result) => {
                self.status = result.message.clone();
                self.result = Some(result);
            }
            Err(error) => {
                self.result = None;
                self.status = format!("SQL error: {error}");
            }
        }
    }
}

impl eframe::App for BrowserApp {
    fn update(&mut self, context: &egui::Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("main_menu").show(context, |ui| {
            ui.horizontal(|ui| {
                ui.strong("PostemberDB Browser");
                ui.separator();
                if ui.button("Open database").clicked() { self.open_database(); }
                if ui.button("Run SQL  ▶").clicked() { self.execute_sql(); }
            });
        });

        egui::SidePanel::left("connection_panel")
            .default_width(235.0)
            .resizable(true)
            .show(context, |ui| {
                ui.heading("Database");
                ui.separator();
                ui.label("Database file");
                ui.text_edit_singleline(&mut self.database_path);
                if ui.button("Open").clicked() { self.open_database(); }
                ui.add_space(12.0);
                ui.label(if self.database.is_some() { "● Connected locally" } else { "○ Disconnected" });
                ui.separator();
                ui.heading("Explorer");
                ui.label("Catalog explorer");
                ui.small("Table and index metadata will appear here when the core exposes read-only catalog inspection APIs.");
                ui.add_space(12.0);
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
                }
            });
            ui.separator();
            ui.heading("Results");
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
                ui.label("Run a query to see its result here.");
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
            .with_inner_size([1100.0, 720.0])
            .with_min_inner_size([760.0, 520.0]),
        ..Default::default()
    };
    eframe::run_native(
        "PostemberDB Browser",
        options,
        Box::new(|_creation_context| Ok(Box::<BrowserApp>::default())),
    )
}
