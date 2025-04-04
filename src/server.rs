use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    error::Error,
    io::Cursor,
    time::Duration,
};

use chrono::{Datelike, Local, Utc};
use db_transit::{
    Position, Route, ScheduleRequest, ScheduleResponse, Shape, Stop, StopTime, Transfer, Trip,
    schedule_server::{Schedule, ScheduleServer},
};
use gtfs_parsing::schedule::{calendar::ExceptionType, trips::DirectionType};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::{Request, Response, Status, transport::Server};
use zip::ZipArchive;

const SUPP_URL: &'static str = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip";

pub mod db_transit {
    tonic::include_proto!("db_transit"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct ScheduleService {
    pub schedule: ScheduleResponse,
}

impl ScheduleService {
    pub fn new(schedule: ScheduleResponse) -> Self {
        Self { schedule }
    }
}

impl TryFrom<gtfs_parsing::schedule::Schedule> for ScheduleResponse {
    type Error = String;

    fn try_from(value: gtfs_parsing::schedule::Schedule) -> Result<Self, Self::Error> {
        let gtfs_parsing::schedule::Schedule {
            routes,
            shapes,
            stops,
            stop_times,
            services,
            service_exceptions,
            trips,
            transfers,
            agencies,
        } = value;

        if agencies.len() > 1 {
            Err("Uh-oh")?
        }

        let curr_date = Local::now().date_naive();
        let weekday = curr_date.weekday();
        let curr_date = format!(
            "{:04}{:02}{:02}",
            curr_date.year(),
            curr_date.month(),
            curr_date.day()
        );

        // These are the service_ids that are active at the current time.
        let mut active_service_ids: HashSet<String> = HashSet::new();

        for s in services {
            if s.start_date <= curr_date
                && s.end_date >= curr_date
                && match weekday {
                    chrono::Weekday::Mon => s.monday.into(),
                    chrono::Weekday::Tue => s.tuesday.into(),
                    chrono::Weekday::Wed => s.wednesday.into(),
                    chrono::Weekday::Thu => s.thursday.into(),
                    chrono::Weekday::Fri => s.friday.into(),
                    chrono::Weekday::Sat => s.saturday.into(),
                    chrono::Weekday::Sun => s.sunday.into(),
                }
            {
                active_service_ids.insert(s.service_id);
            }
        }

        for s in service_exceptions {
            if s.date == curr_date {
                if s.exception_type == ExceptionType::Added {
                    // Service is added on this date, add as active service_id
                    active_service_ids.insert(s.service_id);
                } else if active_service_ids.contains(&s.service_id) {
                    // Service was removed from an active service schedule today, take out of
                    // active ids
                    active_service_ids.remove(&s.service_id);
                }
            }
        }

        // Filter out trips that aren't active
        let trips: Vec<gtfs_parsing::schedule::trips::Trip> = trips
            .into_iter()
            .filter(|t| active_service_ids.contains(&t.service_id))
            .collect();

        // Create a map of stop time entries, keyed by trip_id. This is the biggest table by far in
        // the supplemented feed so it is very beneficial to avoid
        let mut stop_times_map: HashMap<String, Vec<StopTime>> = HashMap::new();

        for stop_time in stop_times {
            let new_entry = StopTime {
                stop_id: stop_time.stop_id,
                arrival_time: stop_time.arrival_time,
                departure_time: stop_time.departure_time,
                stop_sequence: Some(stop_time.stop_sequence),
            };
            match stop_times_map.entry(stop_time.trip_id.clone()) {
                Entry::Occupied(mut e) => {
                    e.get_mut().push(new_entry);
                }
                Entry::Vacant(e) => {
                    e.insert(vec![new_entry]);
                }
            }
        }

        // Organized by route_id
        let mut trip_map: HashMap<String, Vec<Trip>> = HashMap::new();

        // Get new list of trips with metadata filled
        for trip in trips {
            let new_entry = Trip {
                headsign: trip.trip_headsign,
                shape_id: trip.shape_id,
                stop_times: stop_times_map.remove(&trip.trip_id).unwrap_or(vec![]),
                direction: trip
                    .direction_id
                    .map(|id| if id == DirectionType::Uptown { 0 } else { 1 }),
                trip_id: Some(trip.trip_id),
            };

            match trip_map.entry(trip.route_id) {
                Entry::Occupied(mut e) => e.get_mut().push(new_entry),
                Entry::Vacant(e) => {
                    e.insert(vec![new_entry]);
                }
            }
        }

        // Initialize routes as well
        let final_routes: Vec<Route> = routes
            .into_iter()
            .map(|r| Route {
                trips: trip_map.remove(&r.route_id).unwrap_or(vec![]),
                route_id: Some(r.route_id),
            })
            .collect();

        let final_shapes = shapes
            .into_iter()
            .map(|s| {
                let gtfs_parsing::schedule::shapes::Shape { shape_id, points } = s;
                Shape {
                    shape_id: Some(shape_id),
                    points: points
                        .into_iter()
                        .map(|p| Position {
                            lat: Some(p.shape_pt_lat),
                            lon: Some(p.shape_pt_lon),
                        })
                        .collect(),
                }
            })
            .collect();

        // Map transfers by from_stop_id for convenience
        let mut transfer_map: HashMap<String, Vec<Transfer>> = HashMap::new();

        for transfer in transfers {
            let new_entry: Transfer = Transfer {
                from_stop_id: transfer.from_stop_id.clone(),
                to_stop_id: transfer.to_stop_id,
                min_transfer_time: transfer.min_transfer_time,
            };

            match transfer_map.entry(transfer.from_stop_id.unwrap_or_default()) {
                Entry::Occupied(mut e) => {
                    e.get_mut().push(new_entry);
                }
                Entry::Vacant(e) => {
                    e.insert(vec![new_entry]);
                }
            }
        }

        let final_stops = stops
            .into_iter()
            .map(|s| Stop {
                transfers_from: transfer_map.get(&s.stop_id).cloned().unwrap_or(vec![]),
                stop_id: Some(s.stop_id),
                stop_name: s.stop_name,
                position: Some(Position {
                    lat: Some(
                        s.stop_lat
                            .unwrap_or("0.0".to_string())
                            .parse::<f64>()
                            .unwrap_or(0.0),
                    ),
                    lon: Some(
                        s.stop_lon
                            .unwrap_or("0.0".to_string())
                            .parse::<f64>()
                            .unwrap_or(0.0),
                    ),
                }),
                parent_stop_id: s.parent_station,
                route_ids: vec![], //TODO actually calculate these
            })
            .collect();

        Ok(Self {
            routes: final_routes,
            stops: final_stops,
            shapes: final_shapes,
        })
    }
}

#[tonic::async_trait]
impl Schedule for ScheduleService {
    async fn get_schedule(
        &self,
        _request: Request<ScheduleRequest>,
    ) -> Result<Response<ScheduleResponse>, Status> {
        unimplemented!()
    }
}

/// Get the current MTA zip file, check it for differences using the optional hash, and process it
async fn get_update(
    old_hash: Option<[u8; 32]>,
) -> Result<Option<(ScheduleResponse, [u8; 32])>, String> {
    let resp: Vec<u8> = reqwest::get(SUPP_URL)
        .await
        .map_err(|e| e.to_string())?
        .bytes()
        .await
        .map_err(|e| e.to_string())?
        .into();

    let hash = blake3::hash(resp.as_slice());

    if old_hash.is_some() && hash == old_hash.unwrap() {
        Ok(None)
    } else {
        // ZipArchive needs Read + Seek, I'm not sure how efficient this is
        let mut archive = ZipArchive::new(Cursor::new(resp)).map_err(|e| e.to_string())?;
        archive.extract("./gtfs_data/").map_err(|e| e.to_string())?;

        let schedule = gtfs_parsing::schedule::Schedule::from_dir("./gtfs_data/", false);
        let schedule: ScheduleResponse = schedule.try_into()?;

        Ok(Some((schedule, hash.as_bytes().clone())))
    }
}

async fn update_loop(tx: Sender<ScheduleResponse>) -> Result<(), String> {
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

async fn server_loop(mut rx: Receiver<ScheduleResponse>) -> Result<(), Box<dyn Error>> {
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
async fn main() -> Result<(), Box<dyn Error>> {
    loop {
        let (tx, rx) = mpsc::channel(1);

        let server = tokio::spawn(async move { server_loop(rx).await.unwrap() });
        let updater = tokio::spawn(async move { update_loop(tx).await.unwrap() });

        let _ = server.await;

        println!("Server exiting");
        let _ = updater.await;
    }
}
