use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;

use crate::config::{DalConfig, SSL_SUFFIX, TYPE_SSL};
use crate::db::{self, Shared};
use crate::logger::{self, LogLevel};
use crate::parse::get_today;

/// Open file handles + recorded paths for one domain's log files.
#[derive(Default)]
pub struct DomainHandles {
    pub user_handle: Option<File>,
    pub handle: Option<File>,
    pub user_logfile: Option<String>,
    pub logfile: Option<String>,
}

/// One row loaded from `virtualhosts`, plus runtime file handles. The
/// synthetic `"default"` entry (domain_id 0) is the fallback used for
/// unparseable/unmatched-domain lines.
pub struct DomainData {
    pub domain_id: u32,
    pub domainname: String,
    pub basepath: String,
    pub uid: u32,
    pub gid: u32,
    /// Kept for parity with the original struct (bit 0 = SSL, already
    /// consumed to build the "-ssl" domain-name suffix at load time); not
    /// read again afterwards.
    #[allow(dead_code)]
    pub type_: u32,
    pub handles: DomainHandles,
}

pub type AvailDomains = HashMap<String, DomainData>;

/// Loads `virtualhosts` into a fresh `AvailDomains` map, adds the synthetic
/// `"default"` entry (opening `default_logfile` immediately), and opens the
/// global logfile. Returns `Err(())` on any DB or default-logfile-open
/// failure - the caller (`run()`'s startup loop) retries with backoff.
pub fn init_data(shared: &Shared, config: &DalConfig) -> Result<(AvailDomains, Option<File>), ()> {
    let mut avail_domains: AvailDomains = HashMap::new();

    type Row = (u32, String, String, u32, u32, u32);
    let rows: Vec<Row> = {
        let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
        let result = (|| -> rusqlite::Result<Vec<Row>> {
            let mut stmt = state.conn.prepare(
                "SELECT domain_id, domainname, basepath, uid, gid, type FROM `virtualhosts`",
            )?;
            let mut rows = stmt.query([])?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                out.push((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ));
            }
            Ok(out)
        })();

        match result {
            Ok(r) => r,
            Err(e) => {
                logger::write_log(
                    &mut state.dal_logfile,
                    LogLevel::Warning,
                    &format!("Database error: {e}"),
                );
                return Err(());
            }
        }
    };

    for (domain_id, mut domainname, basepath, uid, gid, type_) in rows {
        if type_ & TYPE_SSL != 0 {
            domainname.push_str(SSL_SUFFIX);
        }
        avail_domains.insert(
            domainname.clone(),
            DomainData {
                domain_id,
                domainname,
                basepath,
                uid,
                gid,
                type_,
                handles: DomainHandles::default(),
            },
        );
    }

    let default_handle = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.default_logfile)
        .ok();
    let default_opened = default_handle.is_some();

    avail_domains.insert(
        "default".to_string(),
        DomainData {
            domain_id: 0,
            domainname: "default".to_string(),
            basepath: String::new(),
            uid: 0,
            gid: 0,
            type_: 0,
            handles: DomainHandles {
                handle: default_handle,
                user_handle: None,
                user_logfile: Some(String::new()),
                logfile: Some(config.default_logfile.clone()),
            },
        },
    );

    if !default_opened {
        let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
        logger::write_log(
            &mut state.dal_logfile,
            LogLevel::Error,
            &format!("failed to open default log file {}", config.default_logfile),
        );
        return Err(());
    }

    let global_logfile = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.global_logfile)
        .ok();

    Ok((avail_domains, global_logfile))
}

/// Runs at each detected day-rollover: closes and drops every non-default
/// domain's open handles, unlinking the user-facing copy (logrotate is
/// expected to have already archived the internal copy - see the original's
/// comment), truncates+reopens the global logfile, clears the bandwidth
/// cache (dates changed), and flushes any pending commit.
pub fn do_cleanup(
    avail_domains: &mut AvailDomains,
    global_logfile: &mut Option<File>,
    global_logfile_path: &str,
    shared: &Shared,
) {
    for (name, d) in avail_domains.iter_mut() {
        if name == "default" {
            continue;
        }

        d.handles.handle = None;

        if d.handles.user_handle.take().is_some() {
            if let Some(path) = d.handles.user_logfile.take() {
                if !path.is_empty() {
                    if let Err(e) = std::fs::remove_file(&path) {
                        let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
                        logger::write_log(
                            &mut state.dal_logfile,
                            LogLevel::Warning,
                            &format!("failed to delete user logfile {path}: {e}"),
                        );
                    }
                }
            }
        }

        d.handles.logfile = None;
    }

    *global_logfile = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(global_logfile_path)
        .ok();

    let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
    state.bw_cache.clear();
    db::commit_bandwidth(&mut state);
}

/// Writes one parsed line to a domain's internal + user-facing logfiles
/// (opening them lazily on first use since the last rotation) and to the
/// global logfile. `line_domain` is the resolved domain name (already
/// "-ssl"-suffixed if applicable); `line_text` is the reformatted line body.
pub fn write_apache_log(
    config: &DalConfig,
    line_domain: &str,
    line_text: &str,
    d_data: &mut DomainData,
    global_logfile: &mut Option<File>,
    shared: &Shared,
) {
    if d_data.domainname != "default"
        && (d_data.handles.handle.is_none() || d_data.handles.user_handle.is_none())
    {
        let today = get_today(config.log_type == "error");
        let logpath = format!(
            "{}/{}/{}/{}_{}_{}.log",
            config.log_root, d_data.gid, line_domain, line_domain, config.log_type, today
        );
        let user_logpath = format!(
            "{}/{}/logs/{}_{}.log",
            d_data.basepath, d_data.gid, line_domain, config.log_type
        );

        let handle = OpenOptions::new().create(true).append(true).open(&logpath).ok();
        let user_handle = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&user_logpath)
            .ok();

        if handle.is_none() {
            let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
            logger::write_log(
                &mut state.dal_logfile,
                LogLevel::Warning,
                &format!("failed to open log file {logpath}"),
            );
            return;
        }

        if user_handle.is_none() {
            let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
            logger::write_log(
                &mut state.dal_logfile,
                LogLevel::Warning,
                &format!("failed to open user log file {user_logpath}"),
            );
            return;
        } else {
            // Original silently ignores chown()'s return value; we keep
            // going on failure too (don't abort logging) but log a warning
            // instead of swallowing it - one of the approved bug fixes.
            if let Err(e) = std::os::unix::fs::chown(&user_logpath, Some(d_data.uid), Some(d_data.gid)) {
                let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
                logger::write_log(
                    &mut state.dal_logfile,
                    LogLevel::Warning,
                    &format!("failed to chown user log file {user_logpath}: {e}"),
                );
            }
            let perms = std::fs::Permissions::from_mode(config.userlog_perm);
            if let Err(e) = std::fs::set_permissions(&user_logpath, perms) {
                let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
                logger::write_log(
                    &mut state.dal_logfile,
                    LogLevel::Warning,
                    &format!("failed to chmod user log file {user_logpath}: {e}"),
                );
            }
        }

        d_data.handles.handle = handle;
        d_data.handles.user_handle = user_handle;
        d_data.handles.user_logfile = Some(user_logpath);
        d_data.handles.logfile = Some(logpath);
    }

    if let Some(f) = d_data.handles.handle.as_mut() {
        let _ = writeln!(f, "{line_text}");
    }

    if d_data.domainname != "default" {
        if let Some(f) = d_data.handles.user_handle.as_mut() {
            let _ = writeln!(f, "{line_text}");
        }
    }

    if let Some(f) = global_logfile.as_mut() {
        let _ = writeln!(f, "[{line_domain}] {line_text}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SharedState;
    use rusqlite::Connection;
    use std::sync::{Arc, Mutex};

    fn scratch_path(name: &str) -> String {
        format!(
            "{}/dal_test_{}_{}",
            std::env::temp_dir().display(),
            std::process::id(),
            name
        )
    }

    #[test]
    fn init_data_loads_domains_and_default() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE virtualhosts (domain_id INTEGER, domainname TEXT, basepath TEXT, uid INTEGER, gid INTEGER, type INTEGER);
             INSERT INTO virtualhosts VALUES (1, 'example.com', '/home/example', 1000, 1000, 0);
             INSERT INTO virtualhosts VALUES (2, 'secure.com', '/home/secure', 1001, 1001, 1);",
        )
        .unwrap();

        let shared: Shared = Arc::new(Mutex::new(SharedState::new(conn, 100, 1000, None)));

        let default_logfile = scratch_path("default.log");
        let global_logfile = scratch_path("global.log");
        let config = DalConfig {
            log_root: "/tmp".into(),
            log_type: "custm".into(),
            default_logfile: default_logfile.clone(),
            dal_logfile: "/dev/null".into(),
            sqlite_db: ":memory:".into(),
            global_logfile: global_logfile.clone(),
            commit_count: 100,
            commit_hard_limit: 1000,
            userlog_perm: 0o444,
        };

        let (domains, global) = init_data(&shared, &config).unwrap();

        assert!(domains.contains_key("example.com"));
        assert!(domains.contains_key("secure.com-ssl"));
        assert!(domains.contains_key("default"));
        assert!(global.is_some());
        assert!(domains["default"].handles.handle.is_some());

        let _ = std::fs::remove_file(&default_logfile);
        let _ = std::fs::remove_file(&global_logfile);
    }

    #[test]
    fn init_data_fails_when_default_logfile_unwritable() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE virtualhosts (domain_id INTEGER, domainname TEXT, basepath TEXT, uid INTEGER, gid INTEGER, type INTEGER);",
        )
        .unwrap();
        let shared: Shared = Arc::new(Mutex::new(SharedState::new(conn, 100, 1000, None)));

        let config = DalConfig {
            log_root: "/tmp".into(),
            log_type: "custm".into(),
            default_logfile: "/nonexistent-directory/x/default.log".into(),
            dal_logfile: "/dev/null".into(),
            sqlite_db: ":memory:".into(),
            global_logfile: scratch_path("global2.log"),
            commit_count: 100,
            commit_hard_limit: 1000,
            userlog_perm: 0o444,
        };

        assert!(init_data(&shared, &config).is_err());
    }
}
