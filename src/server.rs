use std::time::Duration;

use blake3::Hash;
use chrono::{DateTime, Days, Timelike};
use chrono::{Local, Utc};
use chrono_tz::Tz;
use tokio::time::sleep;
use tonic::{codec::CompressionEncoding, transport::Server};

use transit_server::shared;
use transit_server::{
    error::ScheduleError,
    shared::{
        ScheduleService,
        db_transit::{ScheduleResponse, schedule_server::ScheduleServer},
    },
};

const SUPP_URL: &'static str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip";

fn get_next_update(dt: DateTime<Tz>) -> DateTime<Tz> {
    const INTERVAL_M: u32 = 5;
    const LAST_VALID: u32 = (60 / INTERVAL_M) - 1;

    let interval = dt.minute() / INTERVAL_M;

    if interval == LAST_VALID {
        if dt.hour() == 23 {
            // Next update will wrap to next day, set time to midnight
            dt.checked_add_days(Days::new(1))
                .unwrap_or_else(|| panic!("Unable to add day to date: {}", dt))
                .with_hour(0)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap()
        } else {
            dt.with_hour(dt.hour() + 1).unwrap().with_minute(0).unwrap()
        }
    } else {
        dt.with_minute((interval + 1) * INTERVAL_M)
            .unwrap()
            .with_second(0)
            .unwrap()
    }
}

/// Get the current MTA zip file, check it for differences using the optional hash, and process it
async fn get_update(
    old_hash: Option<Hash>,
) -> Result<Option<(ScheduleResponse, Hash)>, ScheduleError> {
    return unimplemented!();

    let resp: Vec<u8> = reqwest::get(SUPP_URL).await?.bytes().await?.into();

    let hash = blake3::hash(resp.as_slice());
    unimplemented!();
}

async fn update_loop() -> Result<(), ScheduleError> {
    return unimplemented!();
    let (mut curr_schedule, mut curr_hash) = get_update(None)
        .await?
        .expect("Unable to get initial schedule");

    // // Send initial schedule to waiting server loop
    // let update_global = async |sch: ScheduleResponse| {
    //     println!("Boutta grab lock");
    //     let mut lock = shared::BASE_LOCK.write().await;
    //     *lock = Some(sch.clone());
    //     println!("Boutta release lock");
    // };
    //
    // update_global(curr_schedule).await;
    //
    // // Calculate the next update time, from testing it seems like updates happen around the HH:30
    // // mark
    // let mut next_update = get_next_update(shared::get_nyc_datetime());
    //
    // loop {
    //     if shared::get_nyc_datetime() >= next_update {
    //         match get_update(Some(curr_hash)).await? {
    //             Some((new_schedule, new_hash)) => {
    //                 println!("Found new update at {}", Utc::now());
    //                 curr_schedule = new_schedule;
    //                 curr_hash = new_hash;
    //                 update_global(curr_schedule).await;
    //             }
    //             None => println!("No new update at {}", Utc::now()),
    //         }
    //
    //         next_update = get_next_update(shared::get_nyc_datetime());
    //     }
    //
    //     sleep(Duration::new(10, 0)).await;
    // }
}

async fn server_loop() -> Result<(), ScheduleError> {
    println!("Server waiting for initial schedule");
    // Try to get initial schedule
    // while let None = *UPDATE_LOCK.read().await {
    //     sleep(Duration::new(1, 0)).await;
    // }

    println!(
        "Server thread recieved initial schedule at {}",
        Local::now()
    );

    let addr = "[::1]:50051".parse()?;

    Server::builder()
        .add_service(
            ScheduleServer::new(ScheduleService::default())
                .send_compressed(CompressionEncoding::Gzip),
        )
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), ScheduleError> {
    loop {
        let server = tokio::spawn(async move { server_loop().await.unwrap_or_default() });
        let updater = tokio::spawn(async move { update_loop().await.unwrap_or_default() });

        // Check that both are still running
        while !server.is_finished() && !updater.is_finished() {
            sleep(Duration::new(5, 0)).await;
        }

        // If either crashes we just restart from scratch, abort them
        server.abort();
        updater.abort();

        // Wait for them to actually shutdown to restart
        while !server.is_finished() || !updater.is_finished() {
            sleep(Duration::new(1, 0)).await;
        }
    }
}
