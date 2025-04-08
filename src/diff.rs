use std::collections::{HashMap, HashSet, hash_map::Entry};

use chrono::{Datelike, NaiveDate, Weekday};
use gtfs_parsing::schedule::{calendar::ExceptionType, trips::DirectionType};

use crate::{
    error::ScheduleError,
    shared::{
        db_transit::{Position, Shape, Stop, StopTime, Transfer},
        get_nyc_datetime,
    },
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
    ) -> Result<Self, ScheduleError> {
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

            routes
                .get_mut(route_id)
                .ok_or(format!("Invalid route_id: {}", route_id))?
                .trips
                .insert(trip_id, trip);
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

        Ok(Self {
            routes,
            stops,
            shapes,
        })
    }
}

impl TryFrom<gtfs_parsing::schedule::Schedule> for ScheduleIR {
    type Error = ScheduleError;

    fn try_from(value: gtfs_parsing::schedule::Schedule) -> Result<Self, Self::Error> {
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

#[derive(Debug, Clone)]
pub struct ScheduleUpdate {
    pub route_diffs: Vec<RouteTripsDiff>,

    pub added_shapes: HashMap<String, Shape>,
    pub removed_shape_ids: HashSet<String>,

    pub added_stops: HashMap<String, Stop>,
    pub removed_stop_ids: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct RouteTripsDiff {
    pub route_id: String,

    pub added_trips: HashMap<String, TripIR>,
    pub removed_trip_ids: HashSet<String>,

    pub trip_diffs: Vec<TripStopTimesDiff>,
}

#[derive(Debug, Clone)]
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
    use chrono::NaiveDate;

    use super::ScheduleIR;

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

    #[test]
    fn test_schedule_ir() {
        let schedule = setup_new_schedule!(None).unwrap();
        let schedule_abbrev =
            setup_new_schedule!(Some((&"20250217".to_owned(), &"20250217".to_owned()))).unwrap();

        assert!(schedule.trips.len() != 0 && schedule_abbrev.trips.len() != 0);
        // panic!("{:?}", schedule_abbrev.service_exceptions);
        // let active_services_ids: HashSet<String> = schedule_abbrev
        //     .trips
        //     .values()
        //     .map(|t| t.service_id.clone())
        //     .collect();
        // let active_service_ids2: HashSet<String> = schedule_abbrev
        //     .services
        //     .keys()
        //     .chain(schedule_abbrev.service_exceptions.keys())
        //     .map(String::clone)
        //     .collect();
        // let mut av: Vec<String> = active_services_ids.into_iter().collect();
        // let mut av2: Vec<String> = active_service_ids2.into_iter().collect();
        //
        // av.sort();
        // av2.sort();
        //
        // panic!("1: {:?}\n2: {:?}", av, av2);

        let schedule_ir_abbrev = ScheduleIR::try_from_schedule_with_date(
            schedule_abbrev,
            NaiveDate::from_ymd_opt(2025, 2, 17).unwrap(),
        )
        .unwrap();
        let schedule_ir = ScheduleIR::try_from_schedule_with_date(
            schedule,
            NaiveDate::from_ymd_opt(2025, 2, 17).unwrap(),
        )
        .unwrap();

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
}
