use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection};

use crate::logger::{self, LogLevel};

/// Mirrors a row of the `bandwidth` table.
#[derive(Debug, Clone, Default)]
pub struct BandwidthData {
    pub id: i64,
    pub domain_id: u32,
    pub date: u32,
    pub rcvd: u64,
    pub sent: u64,
    pub time: u32,
    pub count: u32,
}

/// State touched by both the main thread (via `update_bandwidth`) and the
/// shutdown-signal thread (via a final `commit_bandwidth` flush) - see
/// signals.rs. Everything else (open per-domain file handles, `config`) is
/// main-thread-only and lives outside this struct.
pub struct SharedState {
    pub conn: Connection,
    /// domain_id -> pending aggregate. `BTreeMap` (not `HashMap`) so commit
    /// iteration order is deterministic and sorted by domain_id, matching
    /// the original's `std::map<int, bandwidth_data>`.
    pub commit_buffer: BTreeMap<u32, BandwidthData>,
    /// (domain_id, date) -> bandwidth.id
    pub bw_cache: HashMap<(u32, u32), i64>,
    pub commit_requests: u32,
    pub commit_count: u32,
    pub commit_hard_limit: u32,
    pub dal_logfile: Option<File>,
}

pub type Shared = Arc<Mutex<SharedState>>;

impl SharedState {
    pub fn new(
        conn: Connection,
        commit_count: u32,
        commit_hard_limit: u32,
        dal_logfile: Option<File>,
    ) -> Self {
        SharedState {
            conn,
            commit_buffer: BTreeMap::new(),
            bw_cache: HashMap::new(),
            commit_requests: 0,
            commit_count,
            commit_hard_limit,
            dal_logfile,
        }
    }
}

fn log(state: &mut SharedState, level: LogLevel, message: impl AsRef<str>) {
    logger::write_log(&mut state.dal_logfile, level, message.as_ref());
}

fn db_error(state: &mut SharedState, err: &rusqlite::Error) {
    log(state, LogLevel::Warning, format!("Database error: {err}"));
}

fn is_constraint_violation(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(ffi_err, _)
            if ffi_err.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

/// Prepares/executes `ROLLBACK`, then applies the hard-limit safety valve:
/// if we've failed to commit for `commit_hard_limit` accumulated requests,
/// drop the entire buffer to avoid unbounded memory growth.
pub fn rollback_bw_commit(state: &mut SharedState) {
    log(state, LogLevel::Warning, "Doing rollback for last commit");

    if let Err(e) = state.conn.execute_batch("ROLLBACK") {
        log(state, LogLevel::Warning, "Rollback failed");
        db_error(state, &e);
    }

    if state.commit_requests >= state.commit_hard_limit {
        log(
            state,
            LogLevel::Error,
            format!(
                "DAL has failed to update bandwidth data for last {} requests - dropping data to avoid memory leak",
                state.commit_hard_limit
            ),
        );
        state.commit_buffer.clear();
        state.commit_requests = 0;
    }
}

/// Flushes `commit_buffer` to the `bandwidth` table inside a single
/// transaction, reusing one prepared INSERT and one prepared UPDATE
/// statement across every buffered domain (same pattern as the original's
/// single `sqlite3_prepare` + repeated `sqlite3_step`/`sqlite3_reset`).
pub fn commit_bandwidth(state: &mut SharedState) {
    if state.commit_buffer.is_empty() {
        return;
    }

    log(state, LogLevel::Debug, "Starting to commit");

    if let Err(e) = state.conn.execute_batch("BEGIN") {
        log(state, LogLevel::Warning, "Commit begin failed");
        db_error(state, &e);
        return;
    }

    let result: rusqlite::Result<()> = (|| {
        let mut insert = state.conn.prepare(
            "INSERT INTO `bandwidth`(rcvd, sent, time, domain_id, date, count) VALUES(?,?,?,?,?,?)",
        )?;
        let mut update = state.conn.prepare(
            "UPDATE `bandwidth` SET rcvd = rcvd + ?, sent = sent + ?, time = time + ?, count = count + ? \
             WHERE id = ?",
        )?;

        for (domain_id, data) in state.commit_buffer.iter_mut() {
            if data.id == 0 {
                insert.execute(params![
                    data.rcvd as f64,
                    data.sent as f64,
                    data.time,
                    *domain_id,
                    data.date,
                    data.count,
                ])?;
                data.id = state.conn.last_insert_rowid();
                state.bw_cache.insert((*domain_id, data.date), data.id);
            } else {
                update.execute(params![
                    data.rcvd as f64,
                    data.sent as f64,
                    data.time,
                    data.count,
                    data.id,
                ])?;
            }
        }
        Ok(())
    })();

    match result {
        Err(e) if is_constraint_violation(&e) => {
            log(state, LogLevel::Error, "BUG: Tried to create duplicate data");
            rollback_bw_commit(state);
            state.commit_buffer.clear();
            state.commit_requests = 0;
            return;
        }
        Err(e) => {
            log(state, LogLevel::Warning, "Bandwidth update/insert failed");
            db_error(state, &e);
            rollback_bw_commit(state);
            return;
        }
        Ok(()) => {}
    }

    if let Err(e) = state.conn.execute_batch("COMMIT") {
        log(state, LogLevel::Warning, "Commit failed");
        db_error(state, &e);
        rollback_bw_commit(state);
        return;
    }

    state.commit_buffer.clear();
    state.commit_requests = 0;
    log(state, LogLevel::Debug, "Commit successful");
}

/// Merges one parsed line's bandwidth contribution into the pending commit
/// buffer, then commits once `commit_count` requests have accumulated.
/// `domain_id == 0` (the synthetic "default" domain) never accrues
/// bandwidth, matching the original.
pub fn update_bandwidth(shared: &Shared, mut data: BandwidthData) {
    if data.domain_id == 0 {
        return;
    }

    let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());

    let cache_key = (data.domain_id, data.date);
    if let Some(&cached_id) = state.bw_cache.get(&cache_key) {
        data.id = cached_id;
    } else {
        let mut found: Option<i64> = None;
        let select_result = (|| -> rusqlite::Result<()> {
            let mut stmt = state
                .conn
                .prepare("SELECT id FROM `bandwidth` WHERE domain_id = ? AND date = ? LIMIT 1")?;
            let mut rows = stmt.query(params![data.domain_id, data.date])?;
            if let Some(row) = rows.next()? {
                found = Some(row.get(0)?);
            }
            Ok(())
        })();

        if let Err(e) = select_result {
            db_error(&mut state, &e);
            return;
        }

        if let Some(id) = found {
            data.id = id;
            state.bw_cache.insert(cache_key, id);
        }
    }

    match state.commit_buffer.get_mut(&data.domain_id) {
        None => {
            state.commit_buffer.insert(data.domain_id, data.clone());
        }
        Some(existing) => {
            existing.rcvd += data.rcvd;
            existing.sent += data.sent;
            existing.time += data.time;
            existing.count += 1;
        }
    }

    state.commit_requests += 1;
    if state.commit_requests >= state.commit_count {
        commit_bandwidth(&mut state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> SharedState {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE bandwidth (
                id INTEGER PRIMARY KEY,
                rcvd REAL, sent REAL, time INTEGER,
                domain_id INTEGER, date INTEGER, count INTEGER
            );",
        )
        .unwrap();
        SharedState::new(conn, 100, 1000, None)
    }

    fn bw(domain_id: u32, date: u32) -> BandwidthData {
        BandwidthData {
            id: 0,
            domain_id,
            date,
            rcvd: 10,
            sent: 20,
            time: 0,
            count: 1,
        }
    }

    #[test]
    fn commit_requests_increments_exactly_once() {
        let mut state = setup();
        state.commit_count = 1000; // high enough that a commit never fires
        let shared: Shared = Arc::new(Mutex::new(state));
        update_bandwidth(&shared, bw(1, 20260101));
        assert_eq!(shared.lock().unwrap().commit_requests, 1);
    }

    #[test]
    fn commit_fires_at_exact_threshold() {
        let mut state = setup();
        state.commit_count = 3;
        let shared: Shared = Arc::new(Mutex::new(state));
        update_bandwidth(&shared, bw(1, 20260101));
        update_bandwidth(&shared, bw(2, 20260101));
        assert_eq!(shared.lock().unwrap().commit_requests, 2);
        assert!(!shared.lock().unwrap().commit_buffer.is_empty());

        update_bandwidth(&shared, bw(3, 20260101));
        let s = shared.lock().unwrap();
        assert_eq!(s.commit_requests, 0);
        assert!(s.commit_buffer.is_empty());
    }

    #[test]
    fn insert_then_cache_then_update() {
        let mut state = setup();
        state.commit_count = 1; // commit after every call
        let shared: Shared = Arc::new(Mutex::new(state));

        update_bandwidth(&shared, bw(1, 20260101));
        {
            let s = shared.lock().unwrap();
            let count: i64 = s
                .conn
                .query_row("SELECT count FROM bandwidth", [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, 1);
        }

        update_bandwidth(&shared, bw(1, 20260101));
        let s = shared.lock().unwrap();
        let (rcvd, count): (f64, i64) = s
            .conn
            .query_row("SELECT rcvd, count FROM bandwidth", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(rcvd, 20.0);
        assert_eq!(count, 2);

        // still only one row: second call updated via the cached id rather
        // than inserting a duplicate
        let rows: i64 = s
            .conn
            .query_row("SELECT COUNT(*) FROM bandwidth", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn hard_limit_drops_buffer_on_rollback() {
        let mut state = setup();
        state.commit_requests = 5;
        state.commit_hard_limit = 5;
        state.commit_buffer.insert(1, bw(1, 20260101));
        rollback_bw_commit(&mut state);
        assert!(state.commit_buffer.is_empty());
        assert_eq!(state.commit_requests, 0);
    }
}
