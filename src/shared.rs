use chrono::{DateTime, Utc};
use chrono_tz::{America::New_York, Tz};

use db_transit::{FullSchedule, ScheduleRequest, ScheduleResponse, schedule_server::Schedule};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

pub mod db_transit {
    tonic::include_proto!("db_transit"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct ScheduleService {}

pub static BASE_LOCK: RwLock<Option<FullSchedule>> = RwLock::const_new(None);

pub fn get_nyc_datetime() -> DateTime<Tz> {
    let curr_time = Utc::now();
    curr_time.with_timezone(&New_York)
}

#[tonic::async_trait]
impl Schedule for ScheduleService {
    async fn get_schedule(
        &self,
        _request: Request<ScheduleRequest>,
    ) -> Result<Response<ScheduleResponse>, Status> {
        // UPDATE_LOCK
        //     .read()
        //     .await
        //     .clone()
        //     .ok_or(Status::from_error("Unable to acquire read lock".into()))
        //     .map(|s| Response::new(s))
        unimplemented!()
    }
}
