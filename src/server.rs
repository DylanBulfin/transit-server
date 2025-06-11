use std::ops::DerefMut;
use std::{collections::HashMap, sync::LazyLock};

use chrono::{DateTime, Days, Timelike};
use chrono_tz::Tz;

use db_transit::schedule_server::{Schedule, ScheduleServer};
use db_transit::{
    FullSchedule, LastUpdateRequest, LastUpdateResponse, ScheduleDiff, ScheduleRequest,
    ScheduleResponse,
};
use tokio::runtime::{Handle, RuntimeMetrics};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::diff::{core::ScheduleUpdate, ir::ScheduleIR};
use crate::get_nyc_datetime;
use std::io::Cursor;
use std::time::Duration;

use blake3::Hash;
use logge_rs::{error, info};
use tokio::time::sleep;
use tonic::{codec::CompressionEncoding, transport::Server};

use crate::error::ScheduleError;
use zip::ZipArchive;

const SUPP_URL: &'static str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip";
const MAX_HISTORY_LEN: usize = 10;

const INTERVAL_M: u32 = 1;
const LAST_VALID: u32 = (60 / INTERVAL_M) - 1;

// Holds the history of full schedule states for current day
pub static HISTORY_LOCK: RwLock<Vec<(u32, (ScheduleIR, ScheduleUpdate))>> =
    RwLock::const_new(Vec::new());

// Holds the full state of the schedule in GRPC format
pub static FULL_LOCK: RwLock<Option<FullSchedule>> = RwLock::const_new(None);
// Holds history of diffs, indexed by applicable timestamp
pub static DIFFS_LOCK: LazyLock<RwLock<HashMap<u32, ScheduleDiff>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub mod db_transit {
    tonic::include_proto!("db_transit"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct ScheduleService {}

#[tonic::async_trait]
impl Schedule for ScheduleService {
    async fn get_schedule(
        &self,
        _request: Request<ScheduleRequest>,
    ) -> Result<Response<ScheduleResponse>, Status> {
        println!("Recieved new request at {:?}", get_nyc_datetime());
        // Timestamp user was last updated
        let timestamp = _request.into_inner().timestamp.unwrap_or(0);
        let diff_map = DIFFS_LOCK.read().await;

        // Timestamp for most recent state
        let rec_timestamp: Option<u32> = HISTORY_LOCK.read().await.last().map(|(ts, _)| *ts);

        if diff_map.contains_key(&timestamp) {
            println!("Done processing new request at {:?}", get_nyc_datetime());
            Ok(Response::new(ScheduleResponse {
                full_schedule: None,
                schedule_diff: diff_map.get(&timestamp).unwrap().clone().into(),
                timestamp: rec_timestamp,
            }))
        } else if let Some(sched) = FULL_LOCK.read().await.clone() {
            println!("Done processing new request at {:?}", get_nyc_datetime());
            Ok(Response::new(ScheduleResponse {
                full_schedule: Some(sched),
                schedule_diff: None,
                timestamp: rec_timestamp,
            }))
        } else {
            Err(Status::new(tonic::Code::Internal, "Unable to find data"))
        }
    }

    async fn get_last_update(
        &self,
        _request: Request<LastUpdateRequest>,
    ) -> Result<Response<LastUpdateResponse>, Status> {
        let timestamp: Option<u32> = HISTORY_LOCK.read().await.last().map(|(ts, _)| *ts);

        Ok(Response::new(LastUpdateResponse { timestamp }))
    }
}

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

    // if old_hash.is_some() && old_hash.unwrap() == hash {
    //     // No need to update, hash is the same as previous
    //     Ok((None, None))
    // } else {
    let schedule: ScheduleIR =
        gtfs_parsing::schedule::Schedule::from_zip(ZipArchive::new(Cursor::new(resp))?, None)
            .ok_or("Unable to parse server response")?
            .into();

    // // Check equality directly, we can save a lot of space if updates are infrequent
    // if old_schedule.is_some() && old_schedule.unwrap() == &schedule {
    //     Ok((None, Some(hash)))
    // } else {
    Ok((Some(schedule), Some(hash)))
    // }
    // }
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
            let (ts, os) = history_locked.remove(0);
            let od = diffs_locked.remove(&ts);

            drop(os);
            drop(od);
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

        let old_schedule =
            std::mem::replace(FULL_LOCK.write().await.deref_mut(), Some(full_schedule));
        drop(old_schedule);

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

            let t = std::mem::replace(p_update, update);
            drop(t);
        }

        *diffs_locked = diffs_map;

        // diffs_locked.shrink_to_fit();
        // history_locked.shrink_to_fit();
        println!(
            "Diffs capacity: {}, history capacity: {}",
            diffs_locked.capacity(),
            history_locked.capacity()
        );

        println!(
            "Diffs nested capacity: {}, history diff capacity: {}, routes capacity: {}, other schedule capacity: {}",
            diffs_locked
                .values()
                .map(|d| d.added_trips.capacity()
                    + d.added_stops.capacity()
                    + d.added_shapes.capacity()
                    + d.removed_trip_ids.capacity()
                    + d.removed_stop_ids.capacity()
                    + d.removed_shape_ids.capacity())
                .sum::<usize>(),
            history_locked
                .iter()
                .map(|(_, (_, up))| up.added_trips.capacity()
                    + up.added_stops.capacity()
                    + up.added_shapes.capacity()
                    + up.removed_trip_ids.capacity()
                    + up.removed_stop_ids.capacity()
                    + up.removed_shape_ids.capacity())
                .sum::<usize>(),
            history_locked
                .iter()
                .map(|(_, (ir, _))| ir.routes.capacity())
                .sum::<usize>(),
            history_locked
                .iter()
                .map(|(_, (ir, _))| ir
                    .routes
                    .values()
                    .flat_map(|r| r.trips.values())
                    .map(|t| t.stop_times.capacity())
                    .sum::<usize>()
                    + ir.shapes.capacity()
                    + ir.stops.capacity())
                .sum::<usize>(),
        );
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

    for (timestamp, (ir, diff)) in HISTORY_LOCK.read().await.iter() {
        info!(
            "Timestamp {} ir contains {} trips, update contains {} added trips and {} removed trips",
            timestamp,
            ir.routes.values().map(|r| r.trips.len()).sum::<usize>(),
            diff.added_trips.len(),
            diff.removed_trip_ids.len()
        )
    }
}

pub async fn update_loop() -> Result<(), ScheduleError> {
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

pub async fn server_loop() -> Result<(), ScheduleError> {
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
