use std::{fs::OpenOptions, io::stdout, time::Duration};

use logge_rs::{error, setup_logger};
use tokio::time::sleep;
use transit_server::{cacher::cacher_serve_loop, error::ScheduleError};

const LOGGER_FILE: &'static str = "cacher.log";

#[tokio::main]
async fn main() -> Result<(), ScheduleError> {
    setup_logger!(
        ("stdout", stdout()),
        (
            "file",
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(LOGGER_FILE)
                .unwrap()
        )
    )
    .unwrap();

    loop {
        if let Err(e) = cacher_serve_loop().await {
            error!("Cacher server failed: {e}");

            // In case of non-temporary failure like the main server being down, we want to avoid
            // starting it in a tight loop for efficiency reasons
            sleep(Duration::from_secs(1)).await;
        }
    }
}
