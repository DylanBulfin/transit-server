use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::server::db_transit::{ScheduleDiff, Shape, Stop, TripExt, TripIdTuple};

use super::ir::{ScheduleIR, TripIR};

impl ScheduleIR {
    // In in this situation self is the newest
    pub fn get_diff(&self, prev: &Self) -> ScheduleUpdate {
        let (added_stops, removed_stop_ids) = self.get_stop_diffs(prev);
        let (added_shapes, removed_shape_ids) = self.get_shape_diffs(prev);
        let (added_trips, removed_trip_ids) = self.get_trip_diffs(prev);

        ScheduleUpdate {
            added_trips,
            removed_trip_ids,
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

    pub fn get_trip_diffs(
        &self,
        prev: &Self,
    ) -> (HashMap<(String, String), TripIR>, HashSet<(String, String)>) {
        let mut added_trips: HashMap<(String, String), TripIR> = HashMap::new();
        let mut removed_trip_ids: HashSet<(String, String)> = HashSet::new();

        for route in self.routes.values() {
            for trip in route.trips.values() {
                if prev
                    .routes
                    .get(&route.route_id)
                    .expect("new route")
                    .trips
                    .contains_key(&trip.trip_id)
                {
                    if prev
                        .routes
                        .get(&route.route_id)
                        .expect("new route")
                        .trips
                        .get(&trip.trip_id)
                        != Some(trip)
                    {
                        // Updated entry, add to both lists
                        removed_trip_ids.insert((route.route_id.clone(), trip.trip_id.clone()));
                        added_trips
                            .insert((route.route_id.clone(), trip.trip_id.clone()), trip.clone());
                    }
                } else {
                    // This is an added entry
                    added_trips
                        .insert((route.route_id.clone(), trip.trip_id.clone()), trip.clone());
                }
            }
        }
        for route in prev.routes.values() {
            for trip in route.trips.values() {
                if !self
                    .routes
                    .get(&route.route_id)
                    .expect("new route")
                    .trips
                    .contains_key(&trip.trip_id)
                {
                    // Deleted entry
                    removed_trip_ids.insert((route.route_id.clone(), trip.trip_id.clone()));
                }
            }
        }

        (added_trips, removed_trip_ids)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleUpdate {
    // (route_id, trip_id)
    pub added_trips: HashMap<(String, String), TripIR>,
    pub removed_trip_ids: HashSet<(String, String)>,

    pub added_shapes: HashMap<String, Shape>,
    pub removed_shape_ids: HashSet<String>,

    pub added_stops: HashMap<String, Stop>,
    pub removed_stop_ids: HashSet<String>,
}

impl Default for ScheduleUpdate {
    fn default() -> Self {
        Self {
            added_trips: HashMap::new(),
            added_shapes: HashMap::new(),
            added_stops: HashMap::new(),
            removed_trip_ids: HashSet::new(),
            removed_stop_ids: HashSet::new(),
            removed_shape_ids: HashSet::new(),
        }
    }
}

impl From<ScheduleUpdate> for ScheduleDiff {
    fn from(value: ScheduleUpdate) -> Self {
        let ScheduleUpdate {
            added_trips,
            added_stops,
            added_shapes,
            removed_trip_ids,
            removed_stop_ids,
            removed_shape_ids,
        } = value;

        Self {
            added_trips: added_trips
                .into_iter()
                .map(|((_, id), tr)| TripExt {
                    trip: Some(tr.into()),
                    route_id: Some(id),
                })
                .collect(),
            removed_trip_ids: removed_trip_ids
                .into_iter()
                .map(|(rid, tid)| TripIdTuple {
                    trip_id: Some(tid),
                    route_id: Some(rid),
                })
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

fn get_diff_diff_mask(r1: bool, r2: bool, a1: bool, a2: bool) -> (bool, Option<bool>) {
    match (r1, r2, a1, a2) {
        (true, true, true, true)
        | (false, true, true, true)
        | (true, true, false, true)
        | (true, false, false, true)
        | (false, true, false, true) => (true, Some(true)),
        (true, false, true, false) => (true, Some(false)),
        (true, true, true, false) | (true, false, false, false) | (false, true, false, false) => {
            (true, None)
        }
        (false, false, true, false) => (false, Some(false)),
        (false, false, false, true) => (false, Some(true)),
        (false, false, false, false) | (false, true, true, false) => (false, None),
        (true, false, true, true) | (true, true, false, false) | (false, false, true, true) => {
            panic!("Unexpected situation with diffs")
        }
    }
}

fn get_all_ids<T, U>(
    added1: &HashMap<T, U>,
    added2: &HashMap<T, U>,
    removed1: &HashSet<T>,
    removed2: &HashSet<T>,
) -> HashSet<T>
where
    T: Clone + Hash + Eq,
{
    let all_ids_iter = removed1
        .union(removed2)
        .into_iter()
        .chain(added1.keys())
        .chain(added2.keys());

    let mut res = HashSet::new();

    for id in all_ids_iter {
        res.insert(id.clone());
    }

    res
}

impl ScheduleUpdate {
    pub fn combine(&self, other: &ScheduleUpdate) -> Self {
        let ScheduleUpdate {
            removed_shape_ids,
            removed_stop_ids,
            removed_trip_ids,
            added_shapes,
            added_stops,
            added_trips,
        } = self;

        let ScheduleUpdate {
            removed_shape_ids: other_removed_shape_ids,
            removed_stop_ids: other_removed_stop_ids,
            removed_trip_ids: other_removed_trip_ids,
            added_shapes: other_added_shapes,
            added_stops: other_added_stops,
            added_trips: other_added_trips,
        } = other;

        let mut final_added_shapes = HashMap::new();
        let mut final_added_stops = HashMap::new();
        let mut final_added_trips = HashMap::new();
        let mut final_removed_shape_ids = HashSet::new();
        let mut final_removed_trip_ids = HashSet::new();
        let mut final_removed_stop_ids = HashSet::new();

        let all_shape_ids = get_all_ids(
            added_shapes,
            other_added_shapes,
            removed_shape_ids,
            other_removed_shape_ids,
        );
        let all_stop_ids = get_all_ids(
            added_stops,
            other_added_stops,
            removed_stop_ids,
            other_removed_stop_ids,
        );
        let all_trip_ids = get_all_ids(
            added_trips,
            other_added_trips,
            removed_trip_ids,
            other_removed_trip_ids,
        );

        for shape_id in all_shape_ids {
            let (in_removed, added_status) = get_diff_diff_mask(
                removed_shape_ids.contains(&shape_id),
                other_removed_shape_ids.contains(&shape_id),
                added_shapes.contains_key(&shape_id),
                other_added_shapes.contains_key(&shape_id),
            );

            if in_removed {
                final_removed_shape_ids.insert(shape_id.clone());
            }

            match added_status {
                Some(false) => {
                    final_added_shapes.insert(
                        shape_id.clone(),
                        added_shapes.get(&shape_id).unwrap().clone(),
                    );
                }
                Some(true) => {
                    final_added_shapes.insert(
                        shape_id.clone(),
                        other_added_shapes.get(&shape_id).unwrap().clone(),
                    );
                }
                _ => {}
            }
        }

        for stop_id in all_stop_ids {
            let (in_removed, added_status) = get_diff_diff_mask(
                removed_stop_ids.contains(&stop_id),
                other_removed_stop_ids.contains(&stop_id),
                added_stops.contains_key(&stop_id),
                other_added_stops.contains_key(&stop_id),
            );

            if in_removed {
                final_removed_stop_ids.insert(stop_id.clone());
            }

            match added_status {
                Some(false) => {
                    final_added_stops
                        .insert(stop_id.clone(), added_stops.get(&stop_id).unwrap().clone());
                }
                Some(true) => {
                    final_added_stops.insert(
                        stop_id.clone(),
                        other_added_stops.get(&stop_id).unwrap().clone(),
                    );
                }
                _ => {}
            }
        }

        for trip_id in all_trip_ids {
            let (in_removed, added_status) = get_diff_diff_mask(
                removed_trip_ids.contains(&trip_id),
                other_removed_trip_ids.contains(&trip_id),
                added_trips.contains_key(&trip_id),
                other_added_trips.contains_key(&trip_id),
            );

            if in_removed {
                final_removed_trip_ids.insert(trip_id.clone());
            }

            match added_status {
                Some(false) => {
                    final_added_trips
                        .insert(trip_id.clone(), added_trips.get(&trip_id).unwrap().clone());
                }
                Some(true) => {
                    final_added_trips.insert(
                        trip_id.clone(),
                        other_added_trips.get(&trip_id).unwrap().clone(),
                    );
                }
                _ => {}
            }
        }

        Self {
            added_shapes: final_added_shapes,
            added_trips: final_added_trips,
            added_stops: final_added_stops,
            removed_trip_ids: final_removed_trip_ids,
            removed_shape_ids: final_removed_shape_ids,
            removed_stop_ids: final_removed_stop_ids,
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

        for (route_id, trip_id) in self.removed_trip_ids.iter() {
            response
                .routes
                .get_mut(route_id)
                .expect("Unable to find route in schedule")
                .trips
                .remove(trip_id);
        }
        for ids in self.added_trips.keys() {
            response
                .routes
                .get_mut(&ids.0)
                .expect("Unable to find route in schedule")
                .trips
                .insert(ids.1.clone(), self.added_trips.get(ids).unwrap().clone());
        }

        response
    }
}
