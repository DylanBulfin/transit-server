use std::{
    error::Error,
    fs::File,
    io::{BufReader, Cursor, Read},
    task::Poll,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use db_transit::{
    ScheduleRequest, ScheduleResponse,
    schedule_server::{Schedule, ScheduleServer},
};
use prost::bytes::Buf;
use tokio::{net::TcpStream, time::sleep};
use tonic::{Request, Response, Status, transport::Server};
use zip::{ZipArchive, read::root_dir_common_filter};

const SUPP_URL: &'static str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip";

pub mod db_transit {
    tonic::include_proto!("db_transit"); // The string specified here must match the proto package name
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ScheduleService {}

#[tonic::async_trait]
impl Schedule for ScheduleService {
    async fn get_schedule(
        &self,
        request: Request<ScheduleRequest>,
    ) -> Result<Response<ScheduleResponse>, Status> {
        unimplemented!()
    }
    // async fn get_hello(
    //         &self,
    // request: request<hellorequest>,
    //     ) -> result<response<helloresponse>, status> {
    //         println!("received request: {:?}", request);
    //
    //         let hellorequest {
    //             first_name,
    //             last_name,
    //         } = request.into_inner();
    //
    //         let response = helloresponse {
    //             greeting: format!("hello, {} {}", first_name, last_name),
    //         };
    //
    //         ok(response::new(response))
    //     }
}

// #[tonic::async_trait]
// impl test for tester {
// async fn get_hello(
//         &self,
// request: request<hellorequest>,
//     ) -> result<response<helloresponse>, status> {
//         println!("received request: {:?}", request);
//
//         let hellorequest {
//             first_name,
//             last_name,
//         } = request.into_inner();
//
//         let response = helloresponse {
//             greeting: format!("hello, {} {}", first_name, last_name),
//         };
//
//         ok(response::new(response))
//     }
// }

/// Get the current MTA zip file, check it for differences using the optional hash, and process it
async fn get_update(
    old_hash: Option<[u8; 32]>,
) -> Result<Option<(gtfs_parsing::schedule::Schedule, [u8; 32])>, String> {
    let resp: Vec<u8> = reqwest::get(SUPP_URL)
        .await
        .expect("Can't download zip")
        .bytes()
        .await
        .expect("Can't download zip")
        .into();

    let hash = blake3::hash(resp.as_slice());

    if old_hash.is_some() && hash == old_hash.unwrap() {
        Ok(None)
    } else {
        // ZipArchive needs Read + Seek, I'm not sure how efficient this is
        let mut archive = ZipArchive::new(Cursor::new(resp)).expect("Unable to parse zip");
        archive
            .extract("./gtfs_data/")
            .expect("Unable to extract zip");

        let schedule = gtfs_parsing::schedule::Schedule::from_dir("./gtfs_data/", false);

        Ok(Some((schedule, hash.as_bytes().clone())))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = ScheduleService::default();

    // Server::builder()
    //     .add_service(TestServer::new(greeter))
    //     .serve(addr)
    //     .await?;

    // get_update().await?;

    let (mut curr_schedule, mut curr_hash) = get_update(None)
        .await?
        .expect("Unable to get initial schedule");

    println!("Fetching initial schedule at {}", Utc::now());

    let mut update_future = tokio::spawn(get_update(Some(curr_hash.clone())));
    let mut server_future = Server::builder()
        .add_service(ScheduleServer::new(service))
        .serve(addr);

    loop {
        if update_future.is_finished() {
            match update_future.await?? {
                Some((schedule, hash)) => {
                    println!("Found new update at {}", Utc::now());
                    curr_schedule = schedule;
                    curr_hash = hash;
                }
                None => println!("No update at {}", Utc::now()),
            }

            update_future = tokio::spawn(async {
                sleep(Duration::new(300, 0)).await;
                get_update(Some(curr_hash.clone())).await
            });
        }
    }

    // tokio::task::spawn(async move { update_loop().await.unwrap() }).await?;

    Ok(())
}
