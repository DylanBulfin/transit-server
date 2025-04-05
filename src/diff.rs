// Only care about diffing these collections:
// Route::trips
// Route::shapes
// Route::stops
// Trip::stop_times
//
// So, we only need to create diff methods for Route and Trip
//
// All other collections and entities are small enought that it shouldn't matter as much
//
// The routes collection should rarely change and since my application only supports certain
// routes anyway it probably won't matter.

use std::collections::{HashMap, HashSet, hash_map::Entry};

use crate::shared::db_transit::{Shape, Stop, StopTime};

// Create intermediate representations that use HashMap instead of Vec
#[derive(Debug, Clone)]
pub struct ScheduleIR {
    pub routes: HashMap<String, RouteIR>,
    pub shapes: HashMap<String, Shape>,
    pub stops: HashMap<String, Stop>,
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

#[derive(Debug, Clone)]
pub struct ScheduleUpdate {
    pub route_diffs: Vec<RouteTripsDiff>,

    pub added_shapes: HashMap<String, Shape>,
    pub removed_shape_ids: HashSet<String>,

    pub added_stops: HashMap<String, Stop>,
    pub removed_stop_ids: HashSet<String>,
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
