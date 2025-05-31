#![cfg(test)]

use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;
use gtfs_parsing::schedule::Schedule;

use crate::{
    diff::{core::ScheduleUpdate, ir::ScheduleIR},
    shared::db_transit::{FullSchedule, Position, Shape, Stop},
};

use super::ir::{RouteIR, TripIR};

macro_rules! setup_new_schedule {
    ($dir:expr, $bounds:expr) => {{
        let agency_reader =
            std::fs::File::open(format!("./gtfs_data/{}/agency.txt", $dir)).unwrap();
        let stop_reader = std::fs::File::open(format!("./gtfs_data/{}/stops.txt", $dir)).unwrap();
        let stop_time_reader =
            std::fs::File::open(format!("./gtfs_data/{}/stop_times.txt", $dir)).unwrap();
        let service_reader =
            std::fs::File::open(format!("./gtfs_data/{}/calendar.txt", $dir)).unwrap();
        let service_exception_reader =
            std::fs::File::open(format!("./gtfs_data/{}/calendar_dates.txt", $dir)).unwrap();
        let shape_reader = std::fs::File::open(format!("./gtfs_data/{}/shapes.txt", $dir)).unwrap();
        let transfer_reader =
            std::fs::File::open(format!("./gtfs_data/{}/transfers.txt", $dir)).unwrap();
        let route_reader = std::fs::File::open(format!("./gtfs_data/{}/routes.txt", $dir)).unwrap();
        let trip_reader = std::fs::File::open(format!("./gtfs_data/{}/trips.txt", $dir)).unwrap();

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

fn test_diff_full(schedule1: ScheduleIR, schedule2: ScheduleIR) {
    let two_minus_one = schedule2.get_diff(&schedule1);
    let one_minus_two = schedule1.get_diff(&schedule2);

    assert_eq!(one_minus_two.added_shapes.len(), 0);
    assert_eq!(one_minus_two.removed_shape_ids.len(), 0);
    assert_eq!(one_minus_two.added_stops.len(), 0);
    assert_eq!(one_minus_two.removed_stop_ids.len(), 0);
    assert_eq!(one_minus_two.added_trips.len(), 6647);
    assert_eq!(one_minus_two.removed_trip_ids.len(), 6644);

    assert_eq!(two_minus_one.added_shapes.len(), 0);
    assert_eq!(two_minus_one.removed_shape_ids.len(), 0);
    assert_eq!(two_minus_one.added_stops.len(), 0);
    assert_eq!(two_minus_one.removed_stop_ids.len(), 0);
    assert_eq!(two_minus_one.added_trips.len(), 6644);
    assert_eq!(two_minus_one.removed_trip_ids.len(), 6647);

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

    assert_eq!(diff1.added_trips.len(), 0);
    assert_eq!(diff1.removed_trip_ids.len(), 0);
    assert_eq!(diff1.added_stops.len(), 0);
    assert_eq!(diff1.removed_stop_ids.len(), 0);
    assert_eq!(diff1.added_shapes.len(), 0);
    assert_eq!(diff1.removed_shape_ids.len(), 0);

    assert_eq!(diff2.added_trips.len(), 0);
    assert_eq!(diff2.removed_trip_ids.len(), 0);
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

#[test]
fn test_combine() {
    let shape_id1: String = "ShapeId1".to_owned();
    let shape_id2: String = "ShapeId2".to_owned();
    let shape_id3: String = "ShapeId3".to_owned();

    let stop_id1: String = "StopId1".to_owned();
    let stop_id2: String = "StopId2".to_owned();
    let stop_id3: String = "StopId3".to_owned();

    let trip_id1: String = "TripId1".to_owned();
    let trip_id2: String = "TripId2".to_owned();
    let trip_id3: String = "TripId3".to_owned();

    let route_id1: String = "RouteId1".to_owned();
    let route_id2: String = "RouteId2".to_owned();
    let route_id3: String = "RouteId3".to_owned();

    let date_str: String = "20250401".to_owned();

    let test_shape1: Shape = Shape {
        shape_id: Some(shape_id1.clone()),
        points: vec![],
    };
    let test_shape2: Shape = Shape {
        shape_id: Some(shape_id2.clone()),
        points: vec![],
    };
    let test_shape3: Shape = Shape {
        shape_id: Some(shape_id3.clone()),
        points: vec![],
    };

    let test_stop2: Stop = Stop {
        stop_id: Some(stop_id2.clone()),
        stop_name: None,
        transfers_from: vec![],
        position: None,
        parent_stop_id: None,
        route_ids: vec![],
    };
    let test_stop3: Stop = Stop {
        stop_id: Some(stop_id3.clone()),
        stop_name: None,
        transfers_from: vec![],
        position: None,
        parent_stop_id: None,
        route_ids: vec![],
    };

    let test_trip1: TripIR = TripIR {
        trip_id: trip_id1.clone(),
        shape_id: None,
        stop_times: HashMap::new(),
        mask_start_date: date_str.clone(),
        date_mask: 1,
        headsign: None,
        direction: None,
    };
    let test_trip2: TripIR = TripIR {
        trip_id: trip_id2.clone(),
        shape_id: None,
        stop_times: HashMap::new(),
        mask_start_date: date_str.clone(),
        date_mask: 1,
        headsign: None,
        direction: None,
    };
    let test_trip3: TripIR = TripIR {
        trip_id: trip_id3.clone(),
        shape_id: None,
        stop_times: HashMap::new(),
        mask_start_date: date_str.clone(),
        date_mask: 1,
        headsign: None,
        direction: None,
    };

    let diff1 = ScheduleUpdate {
        removed_stop_ids: HashSet::from_iter(vec![stop_id1.clone()].into_iter()),
        removed_shape_ids: HashSet::from_iter(vec![].into_iter()),
        removed_trip_ids: HashSet::from_iter(
            vec![(route_id1.clone(), trip_id1.clone())].into_iter(),
        ),
        added_stops: HashMap::from_iter(vec![(stop_id2.clone(), test_stop2)].into_iter()),
        added_shapes: HashMap::from_iter(vec![
            (shape_id2.clone(), test_shape2),
            (shape_id1.clone(), test_shape1.clone()),
        ]),
        added_trips: HashMap::from_iter(vec![
            ((route_id2.clone(), trip_id2.clone()), test_trip2.clone()),
            ((route_id1.clone(), trip_id1.clone()), test_trip1.clone()),
        ]),
    };
    let diff2 = ScheduleUpdate {
        removed_stop_ids: HashSet::from_iter(vec![stop_id2.clone()].into_iter()),
        removed_shape_ids: HashSet::from_iter(vec![shape_id2.clone()].into_iter()),
        removed_trip_ids: HashSet::from_iter(
            vec![(route_id2.clone(), trip_id2.clone())].into_iter(),
        ),
        added_stops: HashMap::from_iter(vec![(stop_id3.clone(), test_stop3.clone())].into_iter()),
        added_shapes: HashMap::from_iter(vec![(shape_id3.clone(), test_shape3.clone())]),
        added_trips: HashMap::from_iter(vec![
            ((route_id3.clone(), trip_id3.clone()), test_trip3.clone()),
            ((route_id2.clone(), trip_id2.clone()), test_trip2.clone()),
        ]),
    };

    let combo = diff1.combine(&diff2);

    assert_eq!(
        combo.removed_stop_ids,
        HashSet::from_iter(vec![stop_id1].into_iter())
    );
    assert_eq!(
        combo.removed_shape_ids,
        HashSet::from_iter(vec![].into_iter())
    );
    assert_eq!(
        combo.removed_trip_ids,
        HashSet::from_iter(
            vec![
                (route_id1.clone(), trip_id1.clone()),
                (route_id2.clone(), trip_id2.clone())
            ]
            .into_iter()
        )
    );

    assert_eq!(
        combo.added_stops,
        HashMap::from_iter(vec![(stop_id3.clone(), test_stop3.clone())])
    );
    assert_eq!(
        combo.added_shapes,
        HashMap::from_iter(vec![
            (shape_id3.clone(), test_shape3.clone()),
            (shape_id1.clone(), test_shape1.clone())
        ])
    );
    assert_eq!(
        combo.added_trips,
        HashMap::from_iter(vec![
            ((route_id1.clone(), trip_id1.clone()), test_trip1.clone()),
            ((route_id2.clone(), trip_id2.clone()), test_trip2.clone()),
            ((route_id3.clone(), trip_id3.clone()), test_trip3.clone()),
        ])
    );
}
