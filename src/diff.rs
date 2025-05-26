use std::collections::{HashMap, HashSet, hash_map::Entry};

use chrono::{Datelike, Days, NaiveDate, Weekday};
use gtfs_parsing::schedule::{calendar::ExceptionType, trips::DirectionType};

use crate::shared::{
    db_transit::{
        FullSchedule, Position, Route, RouteTripsDiff, ScheduleDiff, Shape, Stop, StopTime,
        Transfer, Trip, TripStopTimesDiff,
    },
    get_nyc_datetime,
};

// Create intermediate representations that use HashMap instead of Vec
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleIR {
    pub routes: HashMap<String, RouteIR>,
    pub shapes: HashMap<String, Shape>,
    pub stops: HashMap<String, Stop>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RouteIR {
    pub route_id: String,

    pub trips: HashMap<String, TripIR>,
}

impl From<RouteIR> for Route {
    fn from(value: RouteIR) -> Self {
        let RouteIR { route_id, trips } = value;

        Self {
            route_id: Some(route_id),
            trips: trips.into_values().map(TripIR::into).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TripIR {
    pub trip_id: String,
    pub stop_times: HashMap<u32, StopTime>, // Keyed by stop sequence for convenience

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
            stop_times: stop_times.into_values().collect(),
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
    fn try_from_schedule_with_dates(
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

        let mut routes = HashMap::new();
        for route_id in s_routes.into_keys() {
            routes.insert(
                route_id.clone(),
                RouteIR {
                    route_id,
                    trips: HashMap::default(),
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

            let stop_times = s_stop_times
                .remove(&trip_id)
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect();

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

        let shapes: HashMap<String, Shape> = s_shapes
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
            .collect();

        let stops: HashMap<String, Stop> = s_stops
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
            .collect();

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
            routes: routes.into_values().map(RouteIR::into).collect(),
            shapes: shapes.into_values().collect(),
            stops: stops.into_values().collect(),
        }
    }
}

impl ScheduleIR {
    // In in this situation self is the newest
    pub fn get_diff(&self, prev: &Self) -> ScheduleUpdate {
        let (added_stops, removed_stop_ids) = self.get_stop_diffs(prev);
        let (added_shapes, removed_shape_ids) = self.get_shape_diffs(prev);

        let mut route_diffs: Vec<RouteTripsUpdate> = Vec::new();
        for route_id in self.routes.keys() {
            if prev.routes.contains_key(route_id) {
                let new_diff = Self::get_route_trips_diff(
                    self.routes.get(route_id).unwrap(),
                    prev.routes.get(route_id).unwrap(),
                );

                if new_diff.added_trips.is_empty()
                    && new_diff.removed_trip_ids.is_empty()
                    && new_diff.trip_diffs.len() == 0
                {
                    // Don't add one if there were no changes
                    continue;
                }

                route_diffs.push(new_diff)
            } else {
                panic!("Unexpected new route: {}", route_id);
            }
        }
        for route_id in prev.routes.keys() {
            if !self.routes.contains_key(route_id) {
                panic!("Unexpected removed route: {}", route_id)
            }
        }

        ScheduleUpdate {
            route_diffs,
            added_shapes,
            removed_shape_ids,
            added_stops,
            removed_stop_ids,
        }
    }

    pub fn get_stop_diffs(&self, prev: &Self) -> (HashMap<String, Stop>, HashSet<String>) {
        let mut added_stops: HashMap<String, Stop> = HashMap::new();
        let mut removed_stop_ids: HashSet<String> = HashSet::new();

        for stop_id in self.stops.keys() {
            if prev.stops.contains_key(stop_id) {
                if self.stops.get(stop_id) != prev.stops.get(stop_id) {
                    // This is an updated entry, add to both removed and added
                    removed_stop_ids.insert(stop_id.clone());
                    added_stops.insert(stop_id.clone(), self.stops.get(stop_id).cloned().unwrap());
                }
            } else {
                // This is a new entry
                added_stops.insert(stop_id.clone(), self.stops.get(stop_id).cloned().unwrap());
            }
        }
        for stop_id in prev.stops.keys() {
            if !self.stops.contains_key(stop_id) {
                removed_stop_ids.insert(stop_id.clone());
            }
        }

        (added_stops, removed_stop_ids)
    }

    pub fn get_shape_diffs(&self, prev: &Self) -> (HashMap<String, Shape>, HashSet<String>) {
        let mut added_shapes: HashMap<String, Shape> = HashMap::new();
        let mut removed_shape_ids: HashSet<String> = HashSet::new();

        for shape_id in self.shapes.keys() {
            if prev.shapes.contains_key(shape_id) {
                if self.shapes.get(shape_id) != prev.shapes.get(shape_id) {
                    // This is an updated entry, add to both removed and added
                    removed_shape_ids.insert(shape_id.clone());
                    added_shapes.insert(
                        shape_id.clone(),
                        self.shapes.get(shape_id).cloned().unwrap(),
                    );
                }
            } else {
                // This is a new entry
                added_shapes.insert(
                    shape_id.clone(),
                    self.shapes.get(shape_id).cloned().unwrap(),
                );
            }
        }
        for shape_id in prev.shapes.keys() {
            if !self.shapes.contains_key(shape_id) {
                removed_shape_ids.insert(shape_id.clone());
            }
        }

        (added_shapes, removed_shape_ids)
    }

    pub fn get_route_trips_diff(curr: &RouteIR, prev: &RouteIR) -> RouteTripsUpdate {
        let mut added_trips: HashMap<String, TripIR> = HashMap::new();
        let mut removed_trip_ids: HashSet<String> = HashSet::new();

        let mut trip_diffs: Vec<TripStopTimesUpdate> = Vec::new();

        for trip_id in curr.trips.keys() {
            if prev.trips.contains_key(trip_id) {
                let curr_trip = curr.trips.get(trip_id).unwrap();
                let prev_trip = prev.trips.get(trip_id).unwrap();

                if curr_trip.shape_id != prev_trip.shape_id
                    || curr_trip.headsign != prev_trip.headsign
                    || curr_trip.direction != prev_trip.direction
                {
                    // Just do a normal update here, should almost never happen anyway
                    removed_trip_ids.insert(trip_id.clone());
                    added_trips.insert(trip_id.clone(), curr.trips.get(trip_id).cloned().unwrap());
                } else {
                    if curr_trip.stop_times != prev_trip.stop_times {
                        trip_diffs.push(Self::get_trip_stop_times_diff(curr_trip, prev_trip));
                    }
                }
            } else {
                added_trips.insert(trip_id.clone(), curr.trips.get(trip_id).cloned().unwrap());
            }
        }
        for trip_id in prev.trips.keys() {
            if !curr.trips.contains_key(trip_id) {
                removed_trip_ids.insert(trip_id.clone());
            }
        }

        RouteTripsUpdate {
            route_id: curr.route_id.clone(),
            trip_diffs,
            added_trips,
            removed_trip_ids,
        }
    }

    pub fn get_trip_stop_times_diff(curr: &TripIR, prev: &TripIR) -> TripStopTimesUpdate {
        let mut added_stop_times: HashMap<u32, StopTime> = HashMap::new();
        let mut removed_stop_time_seq: HashSet<u32> = HashSet::new();

        for stop_seq in curr.stop_times.keys() {
            if prev.stop_times.contains_key(stop_seq) {
                if curr.stop_times.get(stop_seq) != prev.stop_times.get(stop_seq) {
                    removed_stop_time_seq.insert(*stop_seq);
                    added_stop_times
                        .insert(*stop_seq, curr.stop_times.get(stop_seq).cloned().unwrap());
                }
            } else {
                added_stop_times.insert(*stop_seq, curr.stop_times.get(stop_seq).cloned().unwrap());
            }
        }
        for stop_seq in prev.stop_times.keys() {
            if !curr.stop_times.contains_key(stop_seq) {
                removed_stop_time_seq.insert(*stop_seq);
            }
        }

        TripStopTimesUpdate {
            trip_id: curr.trip_id.clone(),
            added_stop_times,
            removed_stop_time_seq,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleUpdate {
    pub route_diffs: Vec<RouteTripsUpdate>,

    pub added_shapes: HashMap<String, Shape>,
    pub removed_shape_ids: HashSet<String>,

    pub added_stops: HashMap<String, Stop>,
    pub removed_stop_ids: HashSet<String>,
}

impl From<ScheduleUpdate> for ScheduleDiff {
    fn from(value: ScheduleUpdate) -> Self {
        let ScheduleUpdate {
            route_diffs,
            added_stops,
            added_shapes,
            removed_stop_ids,
            removed_shape_ids,
        } = value;

        Self {
            route_diffs: route_diffs
                .into_iter()
                .map(RouteTripsUpdate::into)
                .collect(),
            added_shapes: added_shapes.into_values().collect(),
            removed_shape_ids: removed_shape_ids.into_iter().collect(),
            added_stops: added_stops.into_values().collect(),
            removed_stop_ids: removed_stop_ids.into_iter().collect(),
        }
    }
}

// r1, r2 = whether element in question is in removed_<>_ids for self and other
// a1, a2 = same but in added map
// returns (aT, rT),
// rT = whether in final removal list
// aT = whether in final added map, and which version to use (None is not in it, false is list 1,
// true list 2

// fn get_diff_diff_mask(r1: bool, r2: bool, a1: bool, a2: bool) -> (bool, Option<bool>) {
//     match (r1 as u8) << 3 + (r2 as u8) << 2 + (a1 as u8) << 1 + a2 as u8 {
//         0b1111 | 0b0111 | 0b1101 | 0b1001 | 0b0101 => (true, Some(true)),
//         0b1010 => (true, Some(false)),
//         0b1110 | 0b0110 | 0b1000 | 0b0100 => (true, None),
//         0b0010 => (false, Some(false)),
//         0b0001 => (false, Some(true)),
//         0b0000 => (false, None),
//         0b1011 | 0b1100 | 0b0011 => panic!("Unexpected situation with diffs"),
//         a => panic!("Unexpected diff mask: {:b}", a),
//     }
// }

// impl ScheduleUpdate {
//     fn combine(&self, other: &ScheduleUpdate) -> Self {
//         let ScheduleUpdate {
//             removed_shape_ids,
//             removed_stop_ids,
//             added_shapes,
//             added_stops,
//             route_diffs,
//         } = self;
//
//         let ScheduleUpdate {
//             removed_shape_ids: other_removed_shape_ids,
//             removed_stop_ids: other_removed_stop_ids,
//             added_shapes: other_added_shapes,
//             added_stops: other_added_stops,
//             route_diffs: other_route_diffs,
//         } = other;
//
//         let all_shape_ids = 
//     }
// }

#[derive(Debug, Clone, PartialEq)]
pub struct RouteTripsUpdate {
    pub route_id: String,

    pub added_trips: HashMap<String, TripIR>,
    pub removed_trip_ids: HashSet<String>,

    pub trip_diffs: Vec<TripStopTimesUpdate>,
}

impl From<RouteTripsUpdate> for RouteTripsDiff {
    fn from(value: RouteTripsUpdate) -> Self {
        let RouteTripsUpdate {
            route_id,
            added_trips,
            removed_trip_ids,
            trip_diffs,
        } = value;

        Self {
            route_id: Some(route_id),
            added_trips: added_trips.into_values().map(TripIR::into).collect(),
            removed_trip_ids: removed_trip_ids.into_iter().collect(),
            trip_diffs: trip_diffs
                .into_iter()
                .map(TripStopTimesUpdate::into)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TripStopTimesUpdate {
    pub trip_id: String,

    pub added_stop_times: HashMap<u32, StopTime>,
    pub removed_stop_time_seq: HashSet<u32>, // Use stop_sequence for key
}

impl From<TripStopTimesUpdate> for TripStopTimesDiff {
    fn from(value: TripStopTimesUpdate) -> Self {
        let TripStopTimesUpdate {
            trip_id,
            added_stop_times,
            removed_stop_time_seq,
        } = value;

        Self {
            trip_id: Some(trip_id),
            added_stop_times: added_stop_times.into_values().collect(),
            removed_stop_seq: removed_stop_time_seq.into_iter().collect(),
        }
    }
}

impl ScheduleUpdate {
    pub fn apply_to_schedule(&self, mut response: ScheduleIR) -> ScheduleIR {
        for shape_id in self.removed_shape_ids.iter() {
            response.shapes.remove(shape_id);
        }
        for shape_id in self.added_shapes.keys() {
            response.shapes.insert(
                shape_id.clone(),
                self.added_shapes.get(shape_id).unwrap().clone(),
            );
        }

        for stop_id in self.removed_stop_ids.iter() {
            response.stops.remove(stop_id);
        }
        for stop_id in self.added_stops.keys() {
            response.stops.insert(
                stop_id.clone(),
                self.added_stops.get(stop_id).unwrap().clone(),
            );
        }

        for route_diff in self.route_diffs.iter() {
            if let Entry::Occupied(mut e) = response.routes.entry(route_diff.route_id.clone()) {
                for trip_id in route_diff.removed_trip_ids.iter() {
                    e.get_mut().trips.remove(trip_id);
                }
                for trip_id in route_diff.added_trips.keys() {
                    e.get_mut().trips.insert(
                        trip_id.clone(),
                        route_diff.added_trips.get(trip_id).unwrap().clone(),
                    );
                }

                for trip_diff in route_diff.trip_diffs.iter() {
                    if let Entry::Occupied(mut e2) =
                        e.get_mut().trips.entry(trip_diff.trip_id.clone())
                   {
                        for stop_seq in trip_diff.removed_stop_time_seq.iter() {
                            e2.get_mut().stop_times.remove(stop_seq);
                        }
                        for stop_seq in trip_diff.added_stop_times.keys() {
                            e2.get_mut().stop_times.insert(
                                *stop_seq,
                                trip_diff.added_stop_times.get(stop_seq).cloned().unwrap(),
                            );
                        }
                    }
                }
            }
        }

        response
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
            arrival_time,
            departure_time,
            stop_sequence: Some(stop_sequence),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::NaiveDate;
    use gtfs_parsing::schedule::Schedule;

    use crate::shared::db_transit::{FullSchedule, Position, Shape, Stop, StopTime};

    use super::{RouteIR, ScheduleIR, TripIR, TripStopTimesUpdate};

    macro_rules! setup_new_schedule {
        ($dir:expr, $bounds:expr) => {{
            let agency_reader =
                std::fs::File::open(format!("./gtfs_data/{}/agency.txt", $dir)).unwrap();
            let stop_reader =
                std::fs::File::open(format!("./gtfs_data/{}/stops.txt", $dir)).unwrap();
            let stop_time_reader =
                std::fs::File::open(format!("./gtfs_data/{}/stop_times.txt", $dir)).unwrap();
            let service_reader =
                std::fs::File::open(format!("./gtfs_data/{}/calendar.txt", $dir)).unwrap();
            let service_exception_reader =
                std::fs::File::open(format!("./gtfs_data/{}/calendar_dates.txt", $dir)).unwrap();
            let shape_reader =
                std::fs::File::open(format!("./gtfs_data/{}/shapes.txt", $dir)).unwrap();
            let transfer_reader =
                std::fs::File::open(format!("./gtfs_data/{}/transfers.txt", $dir)).unwrap();
            let route_reader =
                std::fs::File::open(format!("./gtfs_data/{}/routes.txt", $dir)).unwrap();
            let trip_reader =
                std::fs::File::open(format!("./gtfs_data/{}/trips.txt", $dir)).unwrap();

            gtfs_parsing::schedule::Schedule::from_readers(
                agency_reader,
                stop_reader,
                stop_time_reader,
                service_reader,
                service_exception_reader,
                shape_reader,
                transfer_reader,
                route_reader,
                trip_reader,
                $bounds,
            )
        }};
    }

    fn nm<T, U>() -> HashMap<T, U> {
        HashMap::default()
    }

    #[test]
    fn test_shape_diffs() {
        let id1 = "Testing".to_owned();
        let id2 = "Testing2".to_owned();
        let id3 = "Testing3".to_owned();

        let mut shapes1 = nm();
        shapes1.insert(
            id1.clone(),
            Shape {
                shape_id: Some(id1.clone()),
                points: vec![Position {
                    lat: Some(1.0),
                    lon: Some(2.0),
                }],
            },
        );
        shapes1.insert(
            id2.clone(),
            Shape {
                shape_id: Some(id2.clone()),
                points: vec![Position {
                    lat: Some(2.0),
                    lon: Some(3.0),
                }],
            },
        );

        let mut shapes2 = nm();
        shapes2.insert(
            id2.clone(),
            Shape {
                shape_id: Some(id2.clone()),
                points: vec![Position {
                    lat: Some(2.0),
                    lon: Some(3.0),
                }],
            },
        );
        shapes2.insert(
            id3.clone(),
            Shape {
                shape_id: Some(id3.clone()),
                points: vec![Position {
                    lat: Some(3.0),
                    lon: Some(4.0),
                }],
            },
        );

        let ir1 = ScheduleIR {
            routes: nm(),
            stops: nm(),
            shapes: shapes1,
        };
        let ir2 = ScheduleIR {
            routes: nm(),
            stops: nm(),
            shapes: shapes2,
        };

        let (added_shapes, removed_shape_ids) = ir2.get_shape_diffs(&ir1);

        assert_eq!(removed_shape_ids.iter().collect::<Vec<_>>(), vec![&id1]);
        assert_eq!(
            added_shapes.values().collect::<Vec<_>>(),
            vec![&Shape {
                shape_id: Some(id3.clone()),
                points: vec![Position {
                    lat: Some(3.0),
                    lon: Some(4.0),
                }],
            }]
        );
    }

    #[test]
    fn test_stop_diffs() {
        let id1 = "Testing".to_owned();
        let id2 = "Testing2".to_owned();
        let id3 = "Testing3".to_owned();

        let stop1 = Stop {
            stop_id: Some(id1.clone()),
            stop_name: None,
            parent_stop_id: None,
            transfers_from: vec![],
            position: Some(Position {
                lat: Some(1.0),
                lon: Some(2.0),
            }),
            route_ids: vec![],
        };

        let stop2 = Stop {
            stop_id: Some(id2.clone()),
            stop_name: None,
            parent_stop_id: None,
            transfers_from: vec![],
            position: Some(Position {
                lat: Some(2.0),
                lon: Some(3.0),
            }),
            route_ids: vec![],
        };

        let stop3 = Stop {
            stop_id: Some(id3.clone()),
            stop_name: None,
            parent_stop_id: None,
            transfers_from: vec![],
            position: Some(Position {
                lat: Some(3.0),
                lon: Some(4.0),
            }),
            route_ids: vec![],
        };

        let stop4 = Stop {
            stop_id: Some(id1.clone()),
            stop_name: Some("Name".to_owned()),
            parent_stop_id: None,
            transfers_from: vec![],
            position: Some(Position {
                lat: Some(4.0),
                lon: Some(4.0),
            }),
            route_ids: vec![],
        };

        let mut stops1 = nm();
        let mut stops2 = nm();

        stops1.insert(id1.clone(), stop1.clone());
        stops1.insert(id2.clone(), stop2.clone());

        stops2.insert(id2.clone(), stop2.clone());
        stops2.insert(id3.clone(), stop3.clone());
        stops2.insert(id1.clone(), stop4.clone());

        let ir1 = ScheduleIR {
            routes: nm(),
            shapes: nm(),
            stops: stops1,
        };
        let ir2 = ScheduleIR {
            routes: nm(),
            shapes: nm(),
            stops: stops2,
        };

        let (added_stops, removed_stop_ids) = ir2.get_stop_diffs(&ir1);

        let mut added_vec = added_stops.values().collect::<Vec<_>>();
        let mut exp_added = vec![&stop3, &stop4];
        added_vec.sort_by_key(|s| s.stop_id.clone().unwrap_or_default());
        exp_added.sort_by_key(|s| s.stop_id.clone().unwrap_or_default());

        assert_eq!(removed_stop_ids.iter().collect::<Vec<_>>(), vec![&id1]);
        assert_eq!(added_vec, exp_added);
    }

    #[test]
    fn test_trip_stop_time_diffs() {
        let id1 = "Testing1".to_owned();
        let id2 = "Testing2".to_owned();
        let id3 = "Testing3".to_owned();

        let ss1 = 12;
        let ss2 = 22;
        let ss3 = 13;

        let stop_time1 = StopTime {
            arrival_time: Some("12:00:00".to_owned()),
            departure_time: Some("13:00:00".to_owned()),
            stop_sequence: Some(ss1),
            stop_id: Some(id1.clone()),
        };
        let stop_time2 = StopTime {
            arrival_time: Some("22:00:00".to_owned()),
            departure_time: Some("23:00:00".to_owned()),
            stop_sequence: Some(ss2),
            stop_id: Some(id2.clone()),
        };
        let stop_time3 = StopTime {
            arrival_time: Some("15:00:00".to_owned()),
            departure_time: Some("16:00:00".to_owned()),
            stop_sequence: Some(ss3),
            stop_id: Some(id3.clone()),
        };
        let stop_time4 = StopTime {
            arrival_time: Some("22:00:00".to_owned()),
            departure_time: Some("23:00:00".to_owned()),
            stop_sequence: Some(ss1),
            stop_id: Some(id2.clone()),
        };

        let mut times1 = nm();
        let mut times2 = nm();

        times1.insert(ss1, stop_time1.clone());
        times1.insert(ss2, stop_time2.clone());

        times2.insert(ss2, stop_time2.clone());
        times2.insert(ss3, stop_time3.clone());
        times2.insert(ss1, stop_time4.clone());

        let trip1 = TripIR {
            trip_id: "TestTrip1".to_owned(),
            stop_times: times1,
            headsign: Some("TestSign1".to_owned()),
            shape_id: Some("TestShape1".to_owned()),
            direction: Some(1),
            date_mask: 1,
            mask_start_date: "20250401".to_owned(),
        };
        let trip2 = TripIR {
            trip_id: "TestTrip2".to_owned(),
            stop_times: times2,
            headsign: Some("TestSign2".to_owned()),
            shape_id: Some("TestShape2".to_owned()),
            direction: Some(2),
            date_mask: 1,
            mask_start_date: "20250401".to_owned(),
        };

        let TripStopTimesUpdate {
            added_stop_times,
            removed_stop_time_seq,
            ..
        } = ScheduleIR::get_trip_stop_times_diff(&trip2, &trip1);

        let mut added_vec: Vec<_> = added_stop_times.values().collect();
        let mut exp_added: Vec<_> = vec![&stop_time3, &stop_time4];

        added_vec.sort_by_key(|t| t.stop_sequence.unwrap_or_default());
        exp_added.sort_by_key(|t| t.stop_sequence.unwrap_or_default());

        assert_eq!(removed_stop_time_seq.iter().collect::<Vec<_>>(), vec![&ss1]);
        assert_eq!(added_vec, exp_added);
    }

    fn test_diff_full(schedule1: ScheduleIR, schedule2: ScheduleIR) {
        let two_minus_one = schedule2.get_diff(&schedule1);
        let one_minus_two = schedule1.get_diff(&schedule2);

        assert_eq!(one_minus_two.route_diffs.len(), 26);
        assert_eq!(one_minus_two.added_shapes.len(), 0);
        assert_eq!(one_minus_two.removed_shape_ids.len(), 0);
        assert_eq!(one_minus_two.added_stops.len(), 0);
        assert_eq!(one_minus_two.removed_stop_ids.len(), 0);
        assert_eq!(
            one_minus_two
                .route_diffs
                .iter()
                .flat_map(|d| d.added_trips.iter())
                .count(),
            6647
        );
        assert_eq!(
            one_minus_two
                .route_diffs
                .iter()
                .flat_map(|d| d.removed_trip_ids.iter())
                .count(),
            6644
        );
        assert_eq!(
            one_minus_two
                .route_diffs
                .iter()
                .flat_map(|d| d.trip_diffs.iter())
                .flat_map(|d| d.added_stop_times.iter())
                .count(),
            0
        );
        assert_eq!(
            one_minus_two
                .route_diffs
                .iter()
                .flat_map(|d| d.trip_diffs.iter())
                .flat_map(|d| d.removed_stop_time_seq.iter())
                .count(),
            0
        );
        assert_eq!(two_minus_one.route_diffs.len(), 26);
        assert_eq!(two_minus_one.added_shapes.len(), 0);
        assert_eq!(two_minus_one.removed_shape_ids.len(), 0);
        assert_eq!(two_minus_one.added_stops.len(), 0);
        assert_eq!(two_minus_one.removed_stop_ids.len(), 0);
        assert_eq!(
            two_minus_one
                .route_diffs
                .iter()
                .flat_map(|d| d.added_trips.iter())
                .count(),
            6644
        );
        assert_eq!(
            two_minus_one
                .route_diffs
                .iter()
                .flat_map(|d| d.removed_trip_ids.iter())
                .count(),
            6647
        );
        assert_eq!(
            two_minus_one
                .route_diffs
                .iter()
                .flat_map(|d| d.trip_diffs.iter())
                .flat_map(|d| d.added_stop_times.iter())
                .count(),
            0
        );
        assert_eq!(
            two_minus_one
                .route_diffs
                .iter()
                .flat_map(|d| d.trip_diffs.iter())
                .flat_map(|d| d.removed_stop_time_seq.iter())
                .count(),
            0
        );

        let exp_schedule1 = one_minus_two.apply_to_schedule(schedule2.clone());
        assert_eq!(schedule1.routes.len(), exp_schedule1.routes.len());
        let (mut s1_trips, mut es1_trips): (Vec<_>, Vec<_>) = (
            schedule1
                .routes
                .values()
                .flat_map(|r| r.trips.values())
                .collect(),
            exp_schedule1
                .routes
                .values()
                .flat_map(|r| r.trips.values())
                .collect(),
        );
        s1_trips.sort_by_key(|t| &t.trip_id);
        es1_trips.sort_by_key(|t| &t.trip_id);
        assert_eq!(s1_trips, es1_trips);
        let (mut s1_times, mut es1_times): (Vec<_>, Vec<_>) = (
            s1_trips
                .iter()
                .flat_map(|t| t.stop_times.values())
                .collect(),
            es1_trips
                .iter()
                .flat_map(|t| t.stop_times.values())
                .collect(),
        );
        s1_times.sort_by_key(|t| t.stop_sequence);
        es1_times.sort_by_key(|t| t.stop_sequence);
        assert_eq!(s1_times, es1_times);
        let (mut s1_shapes, mut es1_shapes): (Vec<_>, Vec<_>) = (
            schedule1.shapes.values().collect(),
            exp_schedule1.shapes.values().collect(),
        );
        s1_shapes.sort_by_key(|s| &s.shape_id);
        es1_shapes.sort_by_key(|s| &s.shape_id);
        assert_eq!(s1_shapes, es1_shapes);
        let (mut s1_stops, mut es1_stops): (Vec<_>, Vec<_>) = (
            schedule1.stops.values().collect(),
            exp_schedule1.stops.values().collect(),
        );
        s1_stops.sort_by_key(|s| &s.stop_id);
        es1_stops.sort_by_key(|s| &s.stop_id);
        assert_eq!(s1_stops, es1_stops);
        assert_eq!(schedule1, exp_schedule1);

        let exp_schedule2 = two_minus_one.apply_to_schedule(schedule1);
        assert_eq!(schedule2.routes.len(), exp_schedule2.routes.len());
        let (mut s2_trips, mut es2_trips): (Vec<_>, Vec<_>) = (
            schedule2
                .routes
                .values()
                .flat_map(|r| r.trips.values())
                .collect(),
            exp_schedule2
                .routes
                .values()
                .flat_map(|r| r.trips.values())
                .collect(),
        );
        s2_trips.sort_by_key(|t| &t.trip_id);
        es2_trips.sort_by_key(|t| &t.trip_id);
        assert_eq!(s2_trips, es2_trips);
        let (mut s2_times, mut es2_times): (Vec<_>, Vec<_>) = (
            s2_trips
                .iter()
                .flat_map(|t| t.stop_times.values())
                .collect(),
            es2_trips
                .iter()
                .flat_map(|t| t.stop_times.values())
                .collect(),
        );
        s2_times.sort_by_key(|t| t.stop_sequence);
        es2_times.sort_by_key(|t| t.stop_sequence);
        assert_eq!(s2_times, es2_times);
        let (mut s2_shapes, mut es2_shapes): (Vec<_>, Vec<_>) = (
            schedule2.shapes.values().collect(),
            exp_schedule2.shapes.values().collect(),
        );
        s2_shapes.sort_by_key(|s| &s.shape_id);
        es2_shapes.sort_by_key(|s| &s.shape_id);
        assert_eq!(s2_shapes, es2_shapes);
        let (mut s2_stops, mut es2_stops): (Vec<_>, Vec<_>) = (
            schedule2.stops.values().collect(),
            exp_schedule2.stops.values().collect(),
        );
        s2_stops.sort_by_key(|s| &s.stop_id);
        es2_stops.sort_by_key(|s| &s.stop_id);
        assert_eq!(s2_stops, es2_stops);
        assert_eq!(schedule2, exp_schedule2);
    }

    fn test_from_ir(schedule: ScheduleIR) {
        let full_schedule: FullSchedule = schedule.clone().into();

        assert_eq!(schedule.routes.len(), full_schedule.routes.len());
        assert_eq!(schedule.shapes.len(), full_schedule.shapes.len());
        assert_eq!(schedule.stops.len(), full_schedule.stops.len());

        assert_eq!(
            schedule
                .routes
                .values()
                .flat_map(|r| r.trips.values())
                .count(),
            full_schedule
                .routes
                .iter()
                .flat_map(|r| r.trips.iter())
                .count()
        );
        assert_eq!(
            schedule
                .routes
                .values()
                .flat_map(|r| r.trips.values())
                .flat_map(|t| t.stop_times.values())
                .count(),
            full_schedule
                .routes
                .iter()
                .flat_map(|r| r.trips.iter())
                .flat_map(|t| t.stop_times.iter())
                .count()
        );
    }

    fn test_id(schedule: ScheduleIR) {
        let schedule2 = schedule.clone();

        let diff1 = schedule.get_diff(&schedule2);
        let diff2 = schedule2.get_diff(&schedule);

        assert_eq!(diff1.route_diffs.len(), 0);
        assert_eq!(diff1.added_stops.len(), 0);
        assert_eq!(diff1.removed_stop_ids.len(), 0);
        assert_eq!(diff1.added_shapes.len(), 0);
        assert_eq!(diff1.removed_shape_ids.len(), 0);

        assert_eq!(diff2.route_diffs.len(), 0);
        assert_eq!(diff2.added_stops.len(), 0);
        assert_eq!(diff2.removed_stop_ids.len(), 0);
        assert_eq!(diff2.added_shapes.len(), 0);
        assert_eq!(diff2.removed_shape_ids.len(), 0);
    }

    fn test_id_ne(schedule: ScheduleIR) {
        let mut schedule2 = schedule.clone();

        assert_eq!(schedule, schedule2);

        schedule2.routes.get_mut("A").unwrap().trips.insert(
            "Test Trip".to_owned(),
            TripIR {
                trip_id: "Test Trip".to_owned(),
                stop_times: HashMap::new(),
                headsign: None,
                shape_id: None,
                direction: None,
                date_mask: 1,
                mask_start_date: "20250401".to_owned(),
            },
        );

        assert_ne!(schedule, schedule2);
    }

    fn test_schedule_ir(schedule: Schedule, schedule_abbrev: Schedule) {
        let schedule_ir_abbrev = ScheduleIR::try_from_schedule_with_dates(
            schedule_abbrev,
            NaiveDate::from_ymd_opt(2025, 2, 17).unwrap(),
            1,
        );
        let schedule_ir = ScheduleIR::try_from_schedule_with_dates(
            schedule,
            NaiveDate::from_ymd_opt(2025, 2, 17).unwrap(),
            1,
        );

        assert_eq!(schedule_ir.routes.len(), schedule_ir_abbrev.routes.len());
        assert_eq!(
            schedule_ir
                .routes
                .values()
                .map(|r| r.trips.len())
                .sum::<usize>(),
            schedule_ir_abbrev
                .routes
                .values()
                .map(|r| r.trips.len())
                .sum::<usize>()
        );
        assert_eq!(
            schedule_ir
                .routes
                .values()
                .flat_map(|r| r.trips.values())
                .map(|t| t.stop_times.len())
                .sum::<usize>(),
            schedule_ir_abbrev
                .routes
                .values()
                .flat_map(|r| r.trips.values())
                .map(|t| t.stop_times.len())
                .sum::<usize>(),
        );
        assert_eq!(schedule_ir.stops.len(), schedule_ir_abbrev.stops.len());
        assert_eq!(
            schedule_ir
                .stops
                .values()
                .flat_map(|s| s.transfers_from.iter())
                .count(),
            schedule_ir_abbrev
                .stops
                .values()
                .flat_map(|s| s.transfers_from.iter())
                .count(),
        );
        assert_eq!(schedule_ir.shapes.len(), schedule_ir_abbrev.shapes.len());

        assert_eq!(schedule_ir.routes.len(), 30);
        assert_eq!(
            schedule_ir
                .routes
                .values()
                .flat_map(|r| r.trips.iter())
                .count(),
            6190
        );
        assert_eq!(
            schedule_ir
                .routes
                .values()
                .flat_map(|r| r.trips.values())
                .map(|t| t.stop_times.len())
                .sum::<usize>(),
            169423
        );
        assert_eq!(schedule_ir.shapes.len(), 311);
        assert_eq!(schedule_ir.stops.len(), 1497);
        assert_eq!(
            schedule_ir
                .stops
                .values()
                .flat_map(|s| s.transfers_from.iter())
                .count(),
            616
        );
    }

    fn test_ranges(schedule: Schedule) {
        let ScheduleIR { routes, .. } = ScheduleIR::try_from_schedule_with_dates(
            schedule,
            NaiveDate::from_ymd_opt(2025, 04, 01).unwrap(),
            32,
        );

        for RouteIR { trips, .. } in routes.into_values() {
            for TripIR {
                date_mask,
                mask_start_date,
                ..
            } in trips.into_values()
            {
                assert_eq!(mask_start_date, "20250401");
                assert_ne!(date_mask, 0);
            }
        }
    }

    #[test]
    #[ignore]
    fn long_running_tests() {
        let schedule = setup_new_schedule!("schedule", None).unwrap();
        let schedule_abbrev = setup_new_schedule!(
            "schedule",
            Some((&"20250217".to_owned(), &"20250217".to_owned()))
        )
        .unwrap();
        let schedule_alt = setup_new_schedule!("schedule_alt", None).unwrap();

        let schedule_ir: ScheduleIR = ScheduleIR::try_from_schedule_with_dates(
            schedule.clone(),
            NaiveDate::from_ymd_opt(2025, 4, 28).unwrap(),
            1,
        );
        let schedule_ir2: ScheduleIR = ScheduleIR::try_from_schedule_with_dates(
            schedule_alt,
            NaiveDate::from_ymd_opt(2025, 4, 28).unwrap(),
            1,
        );

        test_schedule_ir(schedule.clone(), schedule_abbrev);
        test_from_ir(schedule_ir.clone());
        test_id(schedule_ir.clone());
        test_id_ne(schedule_ir.clone());
        test_diff_full(schedule_ir, schedule_ir2);

        test_ranges(schedule);
    }
}
