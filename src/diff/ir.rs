use std::collections::HashMap;

use chrono::{Datelike, Days, NaiveDate, Weekday};
use gtfs_parsing::schedule::{calendar::ExceptionType, trips::DirectionType};

use crate::{
    get_nyc_datetime,
    server::db_transit::{FullSchedule, Position, Route, Shape, Stop, StopTime, Transfer, Trip},
};

macro_rules! make_collection_wrapper_type {
    ($id:ident, $ty:ty $(; $tt:tt)?) => {
        #[derive(Debug, Clone, PartialEq $(, $tt)?)]
        pub struct $id($ty);

        impl std::ops::Deref for $id {
            type Target = $ty;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $id {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl From<$ty> for $id {
            fn from(value: $ty) -> Self {
                Self(value)
            }
        }

        impl $id {
            pub fn into_inner(self) -> $ty {
                self.0
            }
        }
    };
}

make_collection_wrapper_type!(RouteIRs, HashMap<String, RouteIR>);
make_collection_wrapper_type!(ShapeIRs, HashMap<String, Shape>);
make_collection_wrapper_type!(StopIRs, HashMap<String, Stop>);

// Create intermediate representations that use HashMap instead of Vec
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleIR {
    pub routes: RouteIRs,
    pub shapes: ShapeIRs,
    pub stops: StopIRs,
}

make_collection_wrapper_type!(TripIRs, HashMap<String, TripIR>);

#[derive(Debug, Clone, PartialEq)]
pub struct RouteIR {
    pub route_id: String,

    pub trips: TripIRs,
}

impl From<RouteIR> for Route {
    fn from(value: RouteIR) -> Self {
        let RouteIR { route_id, trips } = value;

        Self {
            route_id: Some(route_id),
            trips: trips.into_inner().into_values().map(TripIR::into).collect(),
        }
    }
}

make_collection_wrapper_type!(StopTimeIRs, HashMap<u32, StopTime>);

#[derive(Debug, Clone, PartialEq)]
pub struct TripIR {
    pub trip_id: String,
    pub stop_times: StopTimeIRs,

    pub headsign: Option<String>,
    pub shape_id: Option<String>,
    pub direction: Option<u32>,

    pub mask_start_date: String,
    pub date_mask: u32,
}

impl From<TripIR> for Trip {
    fn from(value: TripIR) -> Self {
        let TripIR {
            trip_id,
            stop_times,
            headsign,
            shape_id,
            direction,
            mask_start_date,
            date_mask,
        } = value;

        Self {
            trip_id: Some(trip_id),
            stop_times: stop_times.into_inner().into_values().collect(),
            headsign,
            shape_id,
            direction,
            mask_start_date: Some(mask_start_date),
            date_mask: Some(date_mask),
        }
    }
}

// StopTime only implements PartialEq but Eq is just a marker trait so we don't need to do anything
impl Eq for TripIR {}

impl ScheduleIR {
    pub fn try_from_schedule_with_dates(
        value: gtfs_parsing::schedule::Schedule,
        start_date: NaiveDate,
        days: u8,
    ) -> Self {
        let gtfs_parsing::schedule::Schedule {
            trips: s_trips,
            routes: s_routes,
            services: s_services,
            service_exceptions: s_service_exceptions,
            shapes: s_shapes,
            stops: s_stops,
            stop_times: mut s_stop_times,
            transfers: mut s_transfers,
            agencies: _, // Slightly more explicit than ..
        } = value;

        let mut routes = RouteIRs(HashMap::new());
        for route_id in s_routes.into_keys() {
            routes.insert(
                route_id.clone(),
                RouteIR {
                    route_id,
                    trips: TripIRs(HashMap::default()),
                },
            );
        }

        let start_date_str = format!(
            "{:04}{:02}{:02}",
            start_date.year(),
            start_date.month(),
            start_date.day()
        );

        for (
            trip_id,
            gtfs_parsing::schedule::trips::Trip {
                shape_id,
                trip_headsign: headsign,
                direction_id,
                ref route_id,
                ref service_id,
                ..
            },
        ) in s_trips
        {
            let mut date_mask = 0u32;

            for day in 0..days {
                let date = start_date
                    .checked_add_days(Days::new(day as u64))
                    .unwrap_or_else(|| {
                        panic!("Unable to add {} days to date {}", days, start_date)
                    });
                let dow = date.weekday();
                let date_str = format!("{:04}{:02}{:02}", date.year(), date.month(), date.day());

                let mut active = false;

                if let Some(service) = s_services.get(service_id) {
                    active = match dow {
                        Weekday::Mon => service.monday.into(),
                        Weekday::Tue => service.tuesday.into(),
                        Weekday::Wed => service.wednesday.into(),
                        Weekday::Thu => service.thursday.into(),
                        Weekday::Fri => service.friday.into(),
                        Weekday::Sat => service.saturday.into(),
                        Weekday::Sun => service.sunday.into(),
                    } && service.start_date <= date_str
                        && service.end_date >= date_str;
                }
                if let Some(service_exceptions) = s_service_exceptions.get(service_id) {
                    if let Some(service_exception) = service_exceptions.get(&date_str) {
                        active = service_exception.exception_type == ExceptionType::Added;
                    }
                }

                if active {
                    date_mask += 1 << day;
                }
            }

            if date_mask == 0 {
                // No active dates found, skip this trip
                continue;
            }

            let stop_times = StopTimeIRs(
                s_stop_times
                    .remove(&trip_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            );

            let trip = TripIR {
                trip_id: trip_id.clone(),
                shape_id,
                headsign,
                direction: direction_id.map(|s| if s == DirectionType::Uptown { 0 } else { 1 }),
                stop_times,
                date_mask,
                mask_start_date: start_date_str.clone(),
            };

            if let Some(route) = routes.get_mut(route_id) {
                route.trips.insert(trip_id, trip);
            }
        }

        let shapes: ShapeIRs = ShapeIRs(
            s_shapes
                .into_iter()
                .map(|(k, shape)| {
                    let gtfs_parsing::schedule::shapes::Shape { shape_id, points } = shape;
                    (
                        k,
                        Shape {
                            shape_id: Some(shape_id),
                            points: points
                                .into_iter()
                                .map(|p| Position {
                                    lat: Some(p.shape_pt_lat),
                                    lon: Some(p.shape_pt_lon),
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
        );

        let stops: StopIRs = StopIRs(
            s_stops
                .into_iter()
                .map(|(k, stop)| {
                    let gtfs_parsing::schedule::stops::Stop {
                        stop_id,
                        stop_lat,
                        stop_lon,
                        stop_name,
                        parent_station,
                        ..
                    } = stop;

                    let transfers_from = s_transfers
                        .remove(&stop_id)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|t| Transfer {
                            from_stop_id: t.from_stop_id,
                            to_stop_id: t.to_stop_id,
                            min_transfer_time: t.min_transfer_time,
                        })
                        .collect();

                    (
                        k,
                        Stop {
                            stop_id: Some(stop_id),
                            stop_name,
                            parent_stop_id: parent_station,
                            transfers_from,
                            route_ids: Vec::new(), // TODO calculate this
                            position: if let (Some(lat), Some(lon)) = (stop_lat, stop_lon) {
                                if let (Ok(lat), Ok(lon)) = (lat.parse(), lon.parse()) {
                                    Some(Position {
                                        lat: Some(lat),
                                        lon: Some(lon),
                                    })
                                } else {
                                    None
                                }
                            } else {
                                None
                            },
                        },
                    )
                })
                .collect(),
        );

        Self {
            routes,
            stops,
            shapes,
        }
    }
}

impl From<gtfs_parsing::schedule::Schedule> for ScheduleIR {
    fn from(value: gtfs_parsing::schedule::Schedule) -> Self {
        // By default, keep the next 32
        Self::try_from_schedule_with_dates(value, get_nyc_datetime().date_naive(), 32)
    }
}

impl From<ScheduleIR> for FullSchedule {
    fn from(value: ScheduleIR) -> Self {
        let ScheduleIR {
            routes,
            shapes,
            stops,
        } = value;

        Self {
            routes: routes
                .into_inner()
                .into_values()
                .map(RouteIR::into)
                .collect(),
            shapes: shapes.into_inner().into_values().collect(),
            stops: stops.into_inner().into_values().collect(),
        }
    }
}

impl From<gtfs_parsing::schedule::stop_times::StopTime> for StopTime {
    fn from(value: gtfs_parsing::schedule::stop_times::StopTime) -> Self {
        let gtfs_parsing::schedule::stop_times::StopTime {
            stop_id,
            arrival_time,
            departure_time,
            stop_sequence,
            ..
        } = value;
        Self {
            stop_id,
            arrival_time: time_str_to_int(arrival_time),
            departure_time: time_str_to_int(departure_time),
            stop_sequence: Some(stop_sequence),
        }
    }
}

// Converts a time string to a number of seconds since midnight
fn time_str_to_int(time: Option<String>) -> Option<u32> {
    let parts: Vec<u32> = time?
        .split(":")
        .map(|p| p.parse::<u32>().unwrap_or_default())
        .collect();

    let mut res = 0u32;
    for (i, part) in parts.into_iter().enumerate() {
        res += part * 60u32.pow(i as u32);
    }

    Some(res)
}
