use std::{collections::HashMap, sync::LazyLock};

use chrono::{DateTime, Utc};
use chrono_tz::{America::New_York, Tz};

use db_transit::{
    FullSchedule, ScheduleDiff, ScheduleRequest, ScheduleResponse, schedule_server::Schedule,
};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::diff::ScheduleIR;

pub mod db_transit {
    tonic::include_proto!("db_transit"); // The string specified here must match the proto package name
}

// Holds the history of full schedule states for current day
pub static HISTORY_LOCK: RwLock<Vec<(u32, ScheduleIR)>> = RwLock::const_new(Vec::new());

// Holds the full state of the schedule in GRPC format
pub static FULL_LOCK: RwLock<Option<FullSchedule>> = RwLock::const_new(None);
// Holds history of diffs, indexed by applicable timestamp
pub static DIFFS_LOCK: LazyLock<RwLock<HashMap<u32, ScheduleDiff>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

#[derive(Debug, Default)]
pub struct ScheduleService {}

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
        let timestamp = _request.into_inner().timestamp.unwrap_or(0);
        let diff_map = DIFFS_LOCK.read().await;

        if diff_map.contains_key(&timestamp) {
            Ok(Response::new(ScheduleResponse {
                full_schedule: None,
                schedule_diff: diff_map.get(&timestamp).unwrap().clone().into(),
            }))
        } else if let Some(full_schedule) = FULL_LOCK.read().await.clone() {
            Ok(Response::new(ScheduleResponse {
                full_schedule: Some(full_schedule),
                schedule_diff: None,
            }))
        } else {
            Err(Status::new(tonic::Code::Internal, "Unable to find data"))
        }
    }
}
