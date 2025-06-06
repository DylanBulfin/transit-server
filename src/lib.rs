use chrono::{DateTime, Utc};
use chrono_tz::{America::New_York, Tz};

pub mod diff;
pub mod error;

pub mod cacher;
pub mod server;

pub fn get_nyc_datetime() -> DateTime<Tz> {
    let curr_time = Utc::now();
    curr_time.with_timezone(&New_York)
}
