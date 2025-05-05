use std::collections::HashMap;
use std::io::Cursor;
use std::time::Duration;

use blake3::Hash;
use chrono::Local;
use chrono::{DateTime, Days, Timelike};
use chrono_tz::Tz;
use gtfs_parsing::schedule::Schedule;
use tokio::time::sleep;
use tonic::{codec::CompressionEncoding, transport::Server};

use transit_server::diff::ScheduleIR;
use transit_server::shared::{DIFFS_LOCK, FULL_LOCK, HISTORY_LOCK, get_nyc_datetime};
use transit_server::{
    error::ScheduleError,
    shared::{ScheduleService, db_transit::schedule_server::ScheduleServer},
};
use zip::ZipArchive;

const SUPP_URL: &'static str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip";
const MAX_HISTORY_LEN: usize = 10;

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
/// Leaves out any information outside of the current calendar day
async fn get_update(
    old_hash: Option<Hash>,
    old_schedule: Option<&ScheduleIR>,
) -> Result<(Option<ScheduleIR>, Option<Hash>), ScheduleError> {
    let resp: Vec<u8> = reqwest::get(SUPP_URL).await?.bytes().await?.into();

    let hash = blake3::hash(resp.as_slice());

    if old_hash.is_some() && old_hash.unwrap() == hash {
        // No need to update, hash is the same as previous
        Ok((None, None))
    } else {
        let schedule: ScheduleIR = Schedule::from_zip(ZipArchive::new(Cursor::new(resp))?, None)
            .ok_or("Unable to parse server response")?
            .into();

        // Check equality directly, we can save a lot of space if updates are infrequent
        if old_schedule.is_some() && old_schedule.unwrap() == &schedule {
            Ok((None, Some(hash)))
        } else {
            Ok((Some(schedule), Some(hash)))
        }
    }
}

async fn update_global_state(schedule: ScheduleIR) {
    let time = get_nyc_datetime();

    println!("{}: Starting global state update", time.time());

    let timestamp = time.timestamp();

    let mut history_locked = HISTORY_LOCK.write().await;
    let mut diffs_locked = DIFFS_LOCK.write().await;

    // Remove the first entry
    if history_locked.len() == MAX_HISTORY_LEN {
        let (ts, _) = history_locked.remove(0);
        diffs_locked.remove(&ts);
    }

    history_locked.push((timestamp as u32, schedule.clone()));
    *(FULL_LOCK.write().await) = Some(schedule.clone().into());

    let mut diffs_map = HashMap::new();
    for (p_timestamp, p_schedule) in HISTORY_LOCK.read().await.iter() {
        diffs_map.insert(*p_timestamp, schedule.get_diff(p_schedule).into());
    }

    *diffs_locked = diffs_map;

    verify_global_state().await;

    println!("{}: Finished global state update", time.time());
}

async fn verify_global_state() {
    println!(
        "Global state contains {} diffs",
        HISTORY_LOCK.read().await.len()
    );

    assert_eq!(
        HISTORY_LOCK.read().await.len(),
        DIFFS_LOCK.read().await.len()
    );
    let mut h_times: Vec<u32> = HISTORY_LOCK.read().await.iter().map(|h| h.0).collect();
    let mut d_times: Vec<u32> = DIFFS_LOCK.read().await.keys().cloned().collect();
    h_times.sort();
    d_times.sort();

    assert_eq!(h_times, d_times);

    for (timestamp, diff) in DIFFS_LOCK.read().await.iter() {
        println!(
            "Timestamp {} contains {} route diffs, and a total of {} trip diffs",
            timestamp,
            diff.route_diffs.len(),
            diff.route_diffs
                .iter()
                .flat_map(|d| d.trip_diffs.iter())
                .count()
        );
    }
}

async fn update_loop() -> Result<(), ScheduleError> {
    let update = get_update(None, None).await?;
    let (mut curr_schedule, mut curr_hash) = (
        update.0.expect("Unable to get initial schedule"),
        update.1.expect("Unable to get initial hash"),
    );

    update_global_state(curr_schedule.clone()).await;

    let mut next_update = get_next_update(get_nyc_datetime());

    loop {
        if get_nyc_datetime() >= next_update {
            match get_update(Some(curr_hash), Some(&curr_schedule)).await? {
                (Some(new_schedule), Some(new_hash)) => {
                    println!("Found new update at {}", get_nyc_datetime().time());
                    (curr_schedule, curr_hash) = (new_schedule, new_hash);
                    // TODO fix the logic on entering new day
                    update_global_state(curr_schedule.clone()).await;
                }
                (None, Some(new_hash)) => {
                    println!(
                        "Found immaterial new update at {}",
                        get_nyc_datetime().time()
                    );
                    curr_hash = new_hash;
                }
                (None, None) => println!("No new update at {}", get_nyc_datetime().time()),
                u => panic!("Unexpected result: {:?}", u),
            }

            next_update = get_next_update(get_nyc_datetime());
        }
    }
}

async fn server_loop() -> Result<(), ScheduleError> {
    println!("Server waiting for initial schedule");
    // Try to get initial schedule
    while let None = *FULL_LOCK.read().await {
        sleep(Duration::new(1, 0)).await;
    }

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
        println!(
            "Starting new server instance at {}",
            get_nyc_datetime().time()
        );
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
