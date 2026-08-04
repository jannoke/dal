mod config;
mod db;
mod domain;
mod error;
mod logger;
mod parse;
mod run;
mod signals;

use std::fs::File;
use std::sync::{Arc, Mutex};

use error::DalError;
use logger::LogLevel;

fn main() {
    std::process::exit(match start() {
        Ok(()) => 0,
        Err(_) => 1,
    });
}

/// Mirrors `main()`'s try/catch: any startup failure (config file missing,
/// mandatory key missing, DB unreachable) is logged at FATAL - which itself
/// prints to stderr and, once `dal_logfile` is open, appends to it too,
/// exactly like the original's `writelog(LOG_FATAL, ...)` catch blocks -
/// and the process exits 1.
fn start() -> Result<(), DalError> {
    let args: Vec<String> = std::env::args().collect();
    let config_path = if args.len() == 2 {
        args[1].clone()
    } else {
        config::DEFAULT_CONFIG.to_string()
    };

    // dal_logfile isn't open yet at this point, matching the original
    // (config-file/key errors happen before read_config() opens it).
    let mut startup_log: Option<File> = None;

    let file = File::open(&config_path).map_err(|_| {
        let err = DalError::ConfigFileNotFound(config_path.clone());
        logger::write_log(&mut startup_log, LogLevel::Fatal, &err.to_string());
        err
    })?;

    let raw = config::RawConfig::parse(file);
    let dal_config = config::DalConfig::from_raw(&raw).map_err(|e| {
        logger::write_log(&mut startup_log, LogLevel::Fatal, &e.to_string());
        e
    })?;

    let mut dal_logfile = logger::open(&dal_config.dal_logfile);

    // Unlike the original (which opens the connection once, ignores the
    // result, and silently limps along with every later DB call failing),
    // a failed initial open is treated as fatal here: the daemon exits and
    // relies on its process supervisor (Apache restarting a dead piped-log
    // child) to retry - an explicit, approved deviation from the original.
    let conn = rusqlite::Connection::open(&dal_config.sqlite_db).map_err(|e| {
        let err = DalError::DbOpen(e);
        logger::write_log(&mut dal_logfile, LogLevel::Fatal, &err.to_string());
        err
    })?;

    let shared: db::Shared = Arc::new(Mutex::new(db::SharedState::new(
        conn,
        dal_config.commit_count,
        dal_config.commit_hard_limit,
        dal_logfile,
    )));

    signals::spawn(shared.clone()).map_err(DalError::Io)?;

    run::run(dal_config, shared);

    Ok(())
}
