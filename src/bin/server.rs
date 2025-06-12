use std::{fs::OpenOptions, io::stdout, time::Duration};

use logge_rs::{error, info, setup_logger};
use tokio::time::sleep;
use transit_server::server::{server_loop, update_loop};

const LOGGER_FILE: &'static str = "server.log";

#[tokio::main]
async fn main() {
    setup_logger!(
        ("stdout", stdout()) // (
                             //     "file",
                             //     OpenOptions::new()
                             //         .create(true)
                             //         .write(true)
                             //         .append(true)
                             //         .open(LOGGER_FILE)
                             //         .unwrap()
                             // )
    )
    .unwrap();

    loop {
        info!("Starting new server instance");

        // There are no ways for the futures to return Ok
        if let (comp, Err(err)) = tokio::select! {
            server = server_loop() => ("Server", server),
            updater = update_loop() => ("Updater", updater),
        } {
            error!("{} thread failed: {}", comp, err);
            sleep(Duration::from_secs(1)).await;
        } else {
            panic!();
        }
    }
}
