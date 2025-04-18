use std::collections::{HashMap, HashSet, hash_map::Entry};

use chrono::{Datelike, NaiveDate, Weekday};
use gtfs_parsing::schedule::{calendar::ExceptionType, trips::DirectionType};

use crate::shared::{
    db_transit::{Position, Shape, Stop, StopTime, Transfer},
    get_nyc_datetime,
};

// Create intermediate representations that use HashMap instead of Vec
#[derive(Debug, Clone)]
pub struct ScheduleIR {
    pub routes: HashMap<String, RouteIR>,
    pub shapes: HashMap<String, Shape>,
    pub stops: HashMap<String, Stop>,
}

#[derive(Debug, Clone)]
pub struct RouteIR {
    pub route_id: String,

    pub trips: HashMap<String, TripIR>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TripIR {
    pub trip_id: String,
    pub stop_times: HashMap<u32, StopTime>, // Keyed by stop sequence for convenience

    pub headsign: Option<String>,
    pub shape_id: Option<String>,
    pub direction: Option<u32>,
}

// StopTime only implements PartialEq but Eq is just a marker trait so we don't need to do anything
impl Eq for TripIR {}

impl ScheduleIR {
    fn try_from_schedule_with_date(
        value: gtfs_parsing::schedule::Schedule,
        date: NaiveDate,
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

        let today = date;
        let today_str = format!("{:04}{:02}{:02}", today.year(), today.month(), today.day());
        let today_dow = today.weekday();
        let mut service_count = 0;
        let mut service_exc_count = 0;
        let trip_len = s_trips.len();

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
            let mut active = false;
            if let Some(service) = s_services.get(service_id) {
                active = match today_dow {
                    Weekday::Mon => service.monday.into(),
                    Weekday::Tue => service.tuesday.into(),
                    Weekday::Wed => service.wednesday.into(),
                    Weekday::Thu => service.thursday.into(),
                    Weekday::Fri => service.friday.into(),
                    Weekday::Sat => service.saturday.into(),
                    Weekday::Sun => service.sunday.into(),
                } && service.start_date <= today_str
                    && service.end_date >= today_str;
            }
            if let Some(service_exceptions) = s_service_exceptions.get(service_id) {
                if let Some(service_exception) = service_exceptions.get(&today_str) {
                    active = service_exception.exception_type == ExceptionType::Added;
                }
            }

            if !active {
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
            };

            if let Some(route) = routes.get_mut(route_id) {
                route.trips.insert(trip_id, trip);
            }
        }
        //
        // panic!("{} {} {}", trip_len, service_count, service_exc_count);

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
        Self::try_from_schedule_with_date(value, get_nyc_datetime().date_naive())
    }
}

impl ScheduleIR {
    // In in this situation self is the newest
    pub fn get_diff(&self, prev: &Self) -> ScheduleUpdate {
        let (added_stops, removed_stop_ids) = self.get_stop_diffs(prev);
        let (added_shapes, removed_shape_ids) = self.get_shape_diffs(prev);

        let mut route_diffs: Vec<RouteTripsDiff> = Vec::new();
        for route_id in self.routes.keys() {
            if prev.routes.contains_key(route_id) {
                route_diffs.push(Self::get_route_trips_diff(
                    self.routes.get(route_id).unwrap(),
                    prev.routes.get(route_id).unwrap(),
                ));
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

    pub fn get_route_trips_diff(curr: &RouteIR, prev: &RouteIR) -> RouteTripsDiff {
        let mut added_trips: HashMap<String, TripIR> = HashMap::new();
        let mut removed_trip_ids: HashSet<String> = HashSet::new();

        let mut trip_diffs: Vec<TripStopTimesDiff> = Vec::new();

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

        RouteTripsDiff {
            route_id: curr.route_id.clone(),
            trip_diffs,
            added_trips,
            removed_trip_ids,
        }
    }

    pub fn get_trip_stop_times_diff(curr: &TripIR, prev: &TripIR) -> TripStopTimesDiff {
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

        TripStopTimesDiff {
            trip_id: curr.trip_id.clone(),
            added_stop_times,
            removed_stop_time_seq,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleUpdate {
    pub route_diffs: Vec<RouteTripsDiff>,

    pub added_shapes: HashMap<String, Shape>,
    pub removed_shape_ids: HashSet<String>,

    pub added_stops: HashMap<String, Stop>,
    pub removed_stop_ids: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RouteTripsDiff {
    pub route_id: String,

    pub added_trips: HashMap<String, TripIR>,
    pub removed_trip_ids: HashSet<String>,

    pub trip_diffs: Vec<TripStopTimesDiff>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TripStopTimesDiff {
    pub trip_id: String,

    pub added_stop_times: HashMap<u32, StopTime>,
    pub removed_stop_time_seq: HashSet<u32>, // Use stop_sequence for key
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

    use crate::shared::db_transit::{Position, Shape, Stop, StopTime};

    use super::{ScheduleIR, TripIR, TripStopTimesDiff};

    macro_rules! setup_new_schedule {
        ($bounds:expr) => {{
            let agency_reader = std::fs::File::open("./gtfs_data/schedule/agency.txt").unwrap();
            let stop_reader = std::fs::File::open("./gtfs_data/schedule/stops.txt").unwrap();
            let stop_time_reader =
                std::fs::File::open("./gtfs_data/schedule/stop_times.txt").unwrap();
            let service_reader = std::fs::File::open("./gtfs_data/schedule/calendar.txt").unwrap();
            let service_exception_reader =
                std::fs::File::open("./gtfs_data/schedule/calendar_dates.txt").unwrap();
            let shape_reader = std::fs::File::open("./gtfs_data/schedule/shapes.txt").unwrap();
            let transfer_reader =
                std::fs::File::open("./gtfs_data/schedule/transfers.txt").unwrap();
            let route_reader = std::fs::File::open("./gtfs_data/schedule/routes.txt").unwrap();
            let trip_reader = std::fs::File::open("./gtfs_data/schedule/trips.txt").unwrap();

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
    #[ignore]
    fn test_schedule_ir() {
        let schedule = setup_new_schedule!(None).unwrap();
        let schedule_abbrev =
            setup_new_schedule!(Some((&"20250217".to_owned(), &"20250217".to_owned()))).unwrap();

        let schedule_ir_abbrev = ScheduleIR::try_from_schedule_with_date(
            schedule_abbrev,
            NaiveDate::from_ymd_opt(2025, 2, 17).unwrap(),
        );
        let schedule_ir = ScheduleIR::try_from_schedule_with_date(
            schedule,
            NaiveDate::from_ymd_opt(2025, 2, 17).unwrap(),
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
        };
        let trip2 = TripIR {
            trip_id: "TestTrip2".to_owned(),
            stop_times: times2,
            headsign: Some("TestSign2".to_owned()),
            shape_id: Some("TestShape2".to_owned()),
            direction: Some(2),
        };

        let TripStopTimesDiff {
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
}
