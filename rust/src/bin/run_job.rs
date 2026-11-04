//! CLI the evals use to run a ported job over a fixture file.
//!
//!   run_job <job> <input.jsonl> [--restart-at N --replay M]
//!
//! Prints one JSON output record per line. `--restart-at N` checkpoints after N events,
//! restores into a fresh runner and resumes M events earlier, the way Kafka redelivers
//! events whose offsets weren't committed before a crash. Exits 3 if the job isn't ported.

use std::{env, fs, process};
use telemetry::jobs;
use telemetry::skeleton::{Event, Job, Runner};

fn run<J: Job>(events: &[Event], restart: Option<(usize, usize)>) -> Vec<serde_json::Value> {
    let mut runner = Runner::<J>::default();
    let mut start = 0;
    if let Some((at, replay)) = restart {
        let at = at.min(events.len());
        events[..at].iter().for_each(|e| runner.process(e));
        let checkpoint = runner.checkpoint();
        runner = Runner::restore(&checkpoint); // only the checkpoint survives the "crash"
        start = at.saturating_sub(replay);
    }
    events[start..].iter().for_each(|e| runner.process(e));
    runner.job.output()
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: run_job <job> <input.jsonl> [--restart-at N --replay M]");
        process::exit(2);
    }
    let flag = |name: &str| {
        let i = args.iter().position(|a| a == name)?;
        args.get(i + 1)?.parse::<usize>().ok()
    };
    let restart = flag("--restart-at").map(|at| (at, flag("--replay").unwrap_or(0)));

    let input = fs::read_to_string(&args[2]).expect("read input file");
    let events: Vec<Event> = input
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid event JSON"))
        .collect();

    // Register each ported job here.
    let output = match args[1].as_str() {
        "battery_health" => run::<jobs::battery_health::BatteryHealth>(&events, restart),
        "ota_status" => run::<jobs::ota_status::OtaStatus>(&events, restart),
        other => {
            eprintln!("job not ported yet: {other}");
            process::exit(3);
        }
    };
    for record in output {
        println!("{record}");
    }
}
