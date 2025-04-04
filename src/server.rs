use std::{
    io::Cursor,
    time::Duration,
};

use chrono::{Local, Utc};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::transport::Server;
use zip::ZipArchive;

use transit_server::{
    error::ScheduleError,
    shared::{
        ScheduleService,
        db_transit::{ScheduleResponse, schedule_server::ScheduleServer},
    },
};

const SUPP_URL: &'static str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip";

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

async fn update_loop(tx: Sender<ScheduleResponse>) -> Result<(), ScheduleError> {
    let (mut curr_schedule, mut curr_hash) = get_update(None)
        .await?
        .expect("Unable to get initial schedule");

    tx.send(curr_schedule).await.map_err(|e| e.to_string())?;

    loop {
        match get_update(Some(curr_hash)).await? {
            Some((new_schedule, new_hash)) => {
                println!("Found new update at {}", Utc::now());
                curr_schedule = new_schedule;
                curr_hash = new_hash;
                tx.send(curr_schedule).await.map_err(|e| e.to_string())?;
            }
            None => println!("No new update at {}", Utc::now()),
        }

        tokio::time::sleep(Duration::new(300, 0)).await;
    }
}

async fn server_loop(mut rx: Receiver<ScheduleResponse>) -> Result<(), ScheduleError> {
    println!("Server waiting for initial schedule");
    // Try to get initial schedule
    let mut curr_schedule = match rx.recv().await {
        Some(cs) => cs,
        None => return Err("Unable to get the schedule in server thread")?,
    };

    println!("Server thread recieved new schedule at {}", Local::now());

    let addr = "[::1]:50051".parse()?;

    let mut server_future = tokio::spawn(async move {
        let service = ScheduleService::new(curr_schedule);

        Server::builder()
            .add_service(ScheduleServer::new(service))
            .serve(addr)
            .await
            .expect("Unable to start server");
    });

    loop {
        // Check for updated schedule
        curr_schedule = match rx.recv().await {
            Some(cs) => cs,
            None => return Err("Unable to get the schedule in server thread")?,
        };

        println!("Server thread recieved new schedule at {}", Local::now());

        let addr = "[::1]:50051".parse()?;

        server_future.abort();
        server_future = tokio::spawn(async move {
            let service = ScheduleService::new(curr_schedule);

            Server::builder()
                .add_service(ScheduleServer::new(service))
                .serve(addr)
                .await
                .expect("Unable to start server");
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), ScheduleError> {
    loop {
        let (tx, rx) = mpsc::channel(1);

        let server = tokio::spawn(async move { server_loop(rx).await.unwrap() });
        let updater = tokio::spawn(async move { update_loop(tx).await.unwrap() });

        while !server.is_finished() && !updater.is_finished() {
            tokio::time::sleep(Duration::new(5, 0)).await;
        }

        server.abort();
        updater.abort();
    }
}
