use std::{io::Cursor, time::Duration};

use chrono::{DateTime, Days, Timelike};
use chrono::{Local, Utc};
use chrono_tz::{America::New_York, Tz};
use tokio::time::sleep;
use tonic::{codec::CompressionEncoding, transport::Server};
use transit_server::shared::{self, UPDATE_LOCK};
use zip::ZipArchive;

use transit_server::{
    error::ScheduleError,
    shared::{
        ScheduleService,
        db_transit::{ScheduleResponse, schedule_server::ScheduleServer},
    },
};

const SUPP_URL: &'static str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip";
const UPDATE_MINUTE: u32 = 35;

fn get_nyc_datetime() -> DateTime<Tz> {
    let curr_time = Utc::now();
    curr_time.with_timezone(&New_York)
}

fn get_next_update(dt: DateTime<Tz>) -> DateTime<Tz> {
    if dt.minute() >= UPDATE_MINUTE {
        if dt.hour() == 23 {
            // Next update will wrap
            dt.checked_add_days(Days::new(1))
                .unwrap_or_else(|| panic!("Unable to add day to date: {}", dt))
                .with_hour(0)
                .unwrap()
                .with_minute(UPDATE_MINUTE)
                .unwrap()
        } else {
            dt.with_hour(dt.hour() + 1)
                .unwrap()
                .with_minute(UPDATE_MINUTE)
                .unwrap()
        }
    } else {
        dt.with_minute(UPDATE_MINUTE).unwrap() // should be infallible
    }
}

/// Get the current MTA zip file, check it for differences using the optional hash, and process it
async fn get_update(
    old_hash: Option<[u8; 32]>,
) -> Result<Option<(ScheduleResponse, [u8; 32])>, ScheduleError> {
    let resp: Vec<u8> = reqwest::get(SUPP_URL).await?.bytes().await?.into();

    let hash = blake3::hash(resp.as_slice());

    if old_hash.is_some() && hash == old_hash.unwrap() {
        Ok(None)
    } else {
        // ZipArchive needs Read + Seek, I'm not sure how efficient this is
        let mut archive = ZipArchive::new(Cursor::new(resp))?;
        archive.extract("./gtfs_data/")?;

        let schedule = gtfs_parsing::schedule::Schedule::from_dir("./gtfs_data/", false);
        let schedule: ScheduleResponse = schedule.try_into()?;

        Ok(Some((schedule, hash.as_bytes().clone())))
    }
}

async fn update_loop() -> Result<(), ScheduleError> {
    let (mut curr_schedule, mut curr_hash) = get_update(None)
        .await?
        .expect("Unable to get initial schedule");

    // Send initial schedule to waiting server loop
    let update_global = async |sch: ScheduleResponse| {
        println!("Boutta grab lock");
        let mut lock = shared::UPDATE_LOCK.write().await;
        *lock = Some(sch.clone());
        println!("Boutta release lock");
    };

    update_global(curr_schedule).await;

    // Calculate the next update time, from testing it seems like updates happen around the HH:30
    // mark
    let mut next_update = get_next_update(get_nyc_datetime());

    loop {
        if get_nyc_datetime() >= next_update {
            match get_update(Some(curr_hash)).await? {
                Some((new_schedule, new_hash)) => {
                    println!("Found new update at {}", Utc::now());
                    curr_schedule = new_schedule;
                    curr_hash = new_hash;
                    update_global(curr_schedule).await;
                }
                None => println!("No new update at {}", Utc::now()),
            }

            next_update = get_next_update(get_nyc_datetime());
        }
    }
}

async fn server_loop() -> Result<(), ScheduleError> {
    println!("Server waiting for initial schedule");
    // Try to get initial schedule
    while let None = *UPDATE_LOCK.read().await {
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
        let server = tokio::spawn(async move { server_loop().await.unwrap() });
        let updater = tokio::spawn(async move { update_loop().await.unwrap() });

        while !server.is_finished() && !updater.is_finished() {
            sleep(Duration::new(5, 0)).await;
        }

        server.abort();
        updater.abort();

        while !server.is_finished() || !updater.is_finished() {
            sleep(Duration::new(0, 1_000_000)).await;
        }
    }
}
