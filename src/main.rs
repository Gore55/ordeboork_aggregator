
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
use orderbook_aggregator::start;
use eframe::egui;
use tokio::runtime::Runtime;

struct MyApp {
    symbol: String,
    status: bool
}

impl Default for MyApp {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            status: false
        }
    }
}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            if !self.status {
                ui.horizontal(|ui| {
                    let symbols = ui.label("Symbols: ");
                    ui.text_edit_singleline(&mut self.symbol)
                        .labelled_by(symbols.id);
                });

                if ui.button(format!("Start")).clicked() {
                    let symbols = self.symbol.clone();
                    
                    if symbols.is_empty() {
                        return
                    }
    
                    
                    let rt = Runtime::new().expect("Unable to create Runtime");
                    
                    let _enter = rt.enter();
                    
                    // Execute the runtime in its own thread.
                    std::thread::spawn(move || {
                        rt.block_on(async {
                            start(symbols).await
                        })
                    });
    
                    self.status = true;
                }                
            } else {
                ui.label("Server Running");
            }
        });
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let options = eframe::NativeOptions {
        maximized: false,
        resizable: false,
        initial_window_size: Some(egui::vec2(200.0, 100.0)),
        ..Default::default()
    };
    match eframe::run_native(
        "Keyrock Challenge",
        options,
        Box::new(|_cc| Box::new(MyApp::default())),
    ) {
        Ok(_) => Ok(()),
        Err(e) => Err(e.into())
    }
}