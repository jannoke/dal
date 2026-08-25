use std::fs::File;
use std::io::{self, BufRead};
use std::thread;
use std::time::Duration;

use crate::config::DalConfig;
use crate::db::{self, BandwidthData, Shared};
use crate::domain::{self, AvailDomains};
use crate::logger::{self, LogLevel};
use crate::parse::{self, LineData};

const FAILURE_SLEEP_MULTIPLIER: u64 = 2;
const FAILURE_SLEEP_MAX: u64 = 20;

/// Main-thread-only state - never touched by the signal-watcher thread, so
/// it needs no locking (this is the per-line hot path).
struct AppState {
    config: DalConfig,
    avail_domains: AvailDomains,
    /// Tracks the newest date SEEN IN THE INPUT STREAM so far, not
    /// wall-clock "today" - replaying an old logfile through DAL must not
    /// spuriously rotate on every line, only forward on the first line of
    /// each new date it encounters.
    current_date: u32,
    global_logfile: Option<File>,
}

fn log(shared: &Shared, level: LogLevel, message: &str) {
    let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
    logger::write_log(&mut state.dal_logfile, level, message);
}

/// Loads the initial domain list, retrying with linear backoff (capped at
/// `FAILURE_SLEEP_MAX` seconds) if the DB query fails - e.g. the DB isn't
/// reachable yet at Apache startup. Note this does not read stdin while
/// retrying, matching the original's behavior.
fn init_with_retry(shared: &Shared, config: &DalConfig) -> (AvailDomains, Option<File>) {
    let mut attempt: u32 = 0;
    loop {
        match domain::init_data(shared, config) {
            Ok(result) => return result,
            Err(()) => {
                attempt += 1;
                let sleep_time =
                    std::cmp::min((attempt as u64) * FAILURE_SLEEP_MULTIPLIER, FAILURE_SLEEP_MAX);
                log(
                    shared,
                    LogLevel::Warning,
                    &format!("General failure detected - sleeping for {sleep_time} seconds"),
                );
                thread::sleep(Duration::from_secs(sleep_time));
            }
        }
    }
}

/// The main event loop: blocks reading lines from stdin (the Apache pipe)
/// until EOF (pipe closed -> clean shutdown) or the process is killed by a
/// signal (handled on a separate thread, see signals.rs).
pub fn run(config: DalConfig, shared: Shared) {
    let (avail_domains, global_logfile) = init_with_retry(&shared, &config);

    let mut app = AppState {
        config,
        avail_domains,
        current_date: 0,
        global_logfile,
    };

    log(
        &shared,
        LogLevel::Info,
        &format!(
            "Datapanel apache logger (DAL) build {} (rust port)",
            env!("CARGO_PKG_VERSION")
        ),
    );

    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();

    loop {
        let line = match lines.next() {
            Some(Ok(l)) => l,
            Some(Err(_)) | None => {
                let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
                logger::write_log(
                    &mut state.dal_logfile,
                    LogLevel::Warning,
                    "input pipe seems to be broken - exiting",
                );
                db::commit_bandwidth(&mut state);
                return;
            }
        };

        if line.is_empty() {
            continue;
        }

        process_line(&mut app, &shared, &line);
    }
}

fn process_line(app: &mut AppState, shared: &Shared, raw_line: &str) {
    let parse_result = if app.config.log_type == "error" {
        parse::parse_error_line(raw_line)
    } else {
        parse::parse_common_line(raw_line)
    };

    let parsed_ok = parse_result.is_ok();
    let mut data = parse_result.unwrap_or_else(|()| LineData {
        domain: "default".to_string(),
        line: raw_line.to_string(),
        ..Default::default()
    });

    if !app.avail_domains.contains_key(&data.domain) {
        log(
            shared,
            LogLevel::Warning,
            &format!("lookup failed for domain {}", data.domain),
        );
        data.domain = "default".to_string();
        data.line = raw_line.to_string();
    }

    if !app.avail_domains.contains_key(&data.domain) {
        log(shared, LogLevel::Error, "DEFAULT lookup not working!");
        return;
    }

    let domain_id = app.avail_domains[&data.domain].domain_id;
    let date = parse::timetostr(data.time, false);

    if parsed_ok && date > app.current_date {
        log(shared, LogLevel::Info, &format!("date is now {date}"));
        domain::do_cleanup(
            &mut app.avail_domains,
            &mut app.global_logfile,
            &app.config.global_logfile,
            shared,
        );
        app.current_date = date;
    }

    let d_data = app
        .avail_domains
        .get_mut(&data.domain)
        .expect("presence checked above");
    domain::write_apache_log(
        &app.config,
        &data.domain,
        &data.line,
        d_data,
        &mut app.global_logfile,
        shared,
    );

    if app.config.log_type != "error" {
        db::update_bandwidth(
            shared,
            BandwidthData {
                id: 0,
                domain_id,
                date,
                rcvd: data.rcvd,
                sent: data.sent,
                time: 0,
                count: 1,
            },
        );
    }
}
