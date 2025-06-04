use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Cursor, stdout};
use std::time::Duration;

use blake3::Hash;
use chrono::{DateTime, Days, Timelike};
use chrono_tz::Tz;
use gtfs_parsing::schedule::Schedule;
use logge_rs::{error, info, setup_logger};
use tokio::time::sleep;
use tonic::{codec::CompressionEncoding, transport::Server};

use transit_server::diff::core::ScheduleUpdate;
use transit_server::diff::ir::ScheduleIR;
use transit_server::shared::db_transit::FullSchedule;
use transit_server::shared::{DIFFS_LOCK, FULL_LOCK, HISTORY_LOCK, get_nyc_datetime};
use transit_server::{
    error::ScheduleError,
    shared::{ScheduleService, db_transit::schedule_server::ScheduleServer},
};
use zip::ZipArchive;

const SUPP_URL: &'static str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip";
const MAX_HISTORY_LEN: usize = 10;

const INTERVAL_M: u32 = 5;
const LAST_VALID: u32 = (60 / INTERVAL_M) - 1;

fn get_next_update(dt: DateTime<Tz>) -> DateTime<Tz> {
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

    info!("Starting global state update");

    let timestamp = time.timestamp();

    {
        let mut history_locked = HISTORY_LOCK.write().await;
        let mut diffs_locked = DIFFS_LOCK.write().await;

        // Remove the first entry
        if history_locked.len() == MAX_HISTORY_LEN {
            let (ts, _) = history_locked.remove(0);
            diffs_locked.remove(&ts);
        }

        let prev_diff: ScheduleUpdate = schedule.get_diff(
            history_locked
                .last()
                .map(|(_, (ir, _))| ir)
                .unwrap_or(&schedule),
        );

        history_locked.push((
            timestamp as u32,
            (schedule.clone().into(), ScheduleUpdate::default()),
        ));

        let full_schedule: FullSchedule = schedule.clone().into();

        *(FULL_LOCK.write().await) = Some(full_schedule);

        let mut diffs_map = HashMap::new();
        for (p_timestamp, (p_schedule, p_update)) in history_locked.iter_mut() {
            // This is the diff from directly comparing the current schedule to the previous
            let update = schedule.get_diff(p_schedule);
            // Alternative way of getting to the same state (ideally)
            let alt_update = p_update.combine(&prev_diff);

            let alt_schedule = p_schedule.clone();
            let alt_schedule = alt_update.apply_to_schedule(alt_schedule);

            if alt_schedule != schedule {
                error!("Mismatched diff combining values, check code");
            }

            diffs_map.insert(*p_timestamp, schedule.get_diff(p_schedule).into());

            *p_update = update;
        }

        *diffs_locked = diffs_map;
    }

    verify_global_state().await;

    info!("Finished global state update");
}

async fn verify_global_state() {
    info!(
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
        info!(
            "Timestamp {} contains {} added trips and {} removed trips",
            timestamp,
            diff.added_trips.len(),
            diff.removed_trip_ids.len()
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
                    info!("Found new update");
                    (curr_schedule, curr_hash) = (new_schedule, new_hash);
                    // TODO fix the logic on entering new day
                    update_global_state(curr_schedule.clone()).await;
                }
                (None, Some(new_hash)) => {
                    info!("Found no new update");
                    curr_hash = new_hash;
                }
                (None, None) => {
                    info!("Found no new update");
                }
                u => panic!("Unexpected result: {:?}", u),
            }

            next_update = get_next_update(get_nyc_datetime());
        }

        sleep(Duration::new(30, 0)).await;
    }
}

async fn server_loop() -> Result<(), ScheduleError> {
    info!("Server waiting for initial schedule");
    // Try to get initial schedule
    while let None = *FULL_LOCK.read().await {
        sleep(Duration::new(1, 0)).await;
    }

    info!("Server thread recieved initial schedule");

    let addr = "[::1]:50052".parse()?;

    Server::builder()
        .add_service(
            ScheduleServer::new(ScheduleService::default())
                .send_compressed(CompressionEncoding::Gzip),
        )
        .serve(addr)
        .await?;

    unreachable!()
}

const LOGGER_FILE: &'static str = "server.log";

#[tokio::main]
async fn main() {
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
