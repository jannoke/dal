use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};

use crate::error::DalError;

pub const DEFAULT_CONFIG: &str = "/etc/dp-ap-logger/logger.conf";
pub const DEFAULT_COMMIT_COUNT: u32 = 100;
pub const DEFAULT_COMMIT_HARD_LIMIT: u32 = 1000;
pub const TYPE_SSL: u32 = 1;
pub const SSL_SUFFIX: &str = "-ssl";

const DELIMITER: &str = "=";
const COMMENT: &str = "#";
const SENTRY: &str = "EndConfigFile";

/// Trims the exact whitespace set the original `ConfigFile::trim()` used
/// (space, newline, tab, vertical tab, carriage return, form feed).
fn cfg_trim(s: &str) -> &str {
    s.trim_matches(|c: char| matches!(c, ' ' | '\n' | '\t' | '\x0B' | '\r' | '\x0C'))
}

/// A parsed key=value config file, ported from the original `ConfigFile`
/// C++ library (comment stripping, multi-line value continuation, sentry
/// line). Only string/u32 reads are needed by DAL.
pub struct RawConfig {
    contents: HashMap<String, String>,
}

impl RawConfig {
    pub fn parse<R: Read>(reader: R) -> Self {
        let mut contents = HashMap::new();
        let mut lines = BufReader::new(reader).lines();
        let mut pending: Option<String> = None;

        loop {
            let mut line = match pending.take() {
                Some(l) => l,
                None => match lines.next() {
                    Some(Ok(l)) => l,
                    _ => break,
                },
            };

            if let Some(pos) = line.find(COMMENT) {
                line.truncate(pos);
            }

            if !SENTRY.is_empty() && line.contains(SENTRY) {
                break;
            }

            let delim_pos = match line.find(DELIMITER) {
                Some(p) => p,
                None => continue,
            };

            let key = line[..delim_pos].to_string();
            let mut value = line[delim_pos + DELIMITER.len()..].to_string();

            // Look ahead for continuation lines: stop at a blank line, a line
            // containing a new key (delimiter), the sentry, or end of stream.
            loop {
                let mut next = match lines.next() {
                    Some(Ok(l)) => l,
                    _ => break,
                };

                if cfg_trim(&next).is_empty() {
                    break;
                }

                if let Some(pos) = next.find(COMMENT) {
                    next.truncate(pos);
                }

                if next.find(DELIMITER).is_some() {
                    pending = Some(next);
                    break;
                }

                if !SENTRY.is_empty() && next.contains(SENTRY) {
                    pending = Some(next);
                    break;
                }

                if !cfg_trim(&next).is_empty() {
                    value.push('\n');
                }
                value.push_str(&next);
            }

            contents.insert(cfg_trim(&key).to_string(), cfg_trim(&value).to_string());
        }

        RawConfig { contents }
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.contents.get(key).map(|s| s.as_str())
    }

    pub fn read_string(&self, key: &str) -> Result<String, DalError> {
        self.get(key)
            .map(|s| s.to_string())
            .ok_or_else(|| DalError::KeyNotFound(key.to_string()))
    }

    pub fn read_u32(&self, key: &str, default: u32) -> u32 {
        self.get(key).and_then(|s| s.parse().ok()).unwrap_or(default)
    }
}

/// DAL's own settings, parsed out of a `RawConfig`. `server_id` (present in
/// sample .conf files) is intentionally not read anywhere - it was dead
/// config in the original too.
#[derive(Debug, Clone)]
pub struct DalConfig {
    pub log_root: String,
    pub log_type: String,
    pub default_logfile: String,
    pub dal_logfile: String,
    pub sqlite_db: String,
    pub global_logfile: String,
    pub commit_count: u32,
    pub commit_hard_limit: u32,
    pub userlog_perm: u32,
}

impl DalConfig {
    pub fn from_raw(raw: &RawConfig) -> Result<Self, DalError> {
        let log_root = raw.read_string("log_root")?;
        let log_type = raw.read_string("log_type")?;
        let default_logfile = raw.read_string("apache_logfile")?;
        let dal_logfile = raw.read_string("logfile")?;
        let sqlite_db = raw.read_string("sqlite_db")?;
        let global_logfile = raw.read_string("global_logfile")?;
        let commit_count = raw.read_u32("commit_count", DEFAULT_COMMIT_COUNT);
        let commit_hard_limit = raw.read_u32("commit_hard_limit", DEFAULT_COMMIT_HARD_LIMIT);

        let perm_str = raw.read_string("userlog_perm")?;
        let userlog_perm = u32::from_str_radix(perm_str.trim(), 8).map_err(|_| {
            DalError::InvalidValue {
                key: "userlog_perm".to_string(),
                reason: format!("not a valid octal number: `{perm_str}`"),
            }
        })?;

        Ok(DalConfig {
            log_root,
            log_type,
            default_logfile,
            dal_logfile,
            sqlite_db,
            global_logfile,
            commit_count,
            commit_hard_limit,
            userlog_perm,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_str(s: &str) -> RawConfig {
        RawConfig::parse(s.as_bytes())
    }

    #[test]
    fn mandatory_key_present() {
        let raw = parse_str("log_root = /var/log\n");
        assert_eq!(raw.read_string("log_root").unwrap(), "/var/log");
    }

    #[test]
    fn mandatory_key_missing() {
        let raw = parse_str("other = 1\n");
        match raw.read_string("log_root") {
            Err(DalError::KeyNotFound(k)) => assert_eq!(k, "log_root"),
            other => panic!("expected KeyNotFound, got {other:?}"),
        }
    }

    #[test]
    fn optional_key_default_vs_override() {
        let raw = parse_str("commit_count = 200\n");
        assert_eq!(raw.read_u32("commit_count", 100), 200);
        assert_eq!(raw.read_u32("commit_hard_limit", 1000), 1000);
    }

    #[test]
    fn full_line_and_trailing_comment_stripped() {
        let raw = parse_str("# a comment line\nlog_root = /var/log # trailing comment\n");
        assert_eq!(raw.read_string("log_root").unwrap(), "/var/log");
    }

    #[test]
    fn octal_userlog_perm() {
        let raw = parse_str(
            "log_root=/a\nlog_type=custm\napache_logfile=/b\nlogfile=/c\nsqlite_db=/d\n\
             global_logfile=/e\nuserlog_perm=0444\n",
        );
        let cfg = DalConfig::from_raw(&raw).unwrap();
        assert_eq!(cfg.userlog_perm, 0o444);
        assert_eq!(cfg.userlog_perm, 292);
    }

    #[test]
    fn multiline_continuation_joined_with_newline() {
        let raw = parse_str("log_root = first line\nsecond line\nthird line\n\nother = x\n");
        assert_eq!(
            raw.read_string("log_root").unwrap(),
            "first line\nsecond line\nthird line"
        );
        assert_eq!(raw.read_string("other").unwrap(), "x");
    }

    #[test]
    fn later_duplicate_key_wins() {
        let raw = parse_str("log_root = first\nlog_root = second\n");
        assert_eq!(raw.read_string("log_root").unwrap(), "second");
    }

    #[test]
    fn sentry_halts_parsing() {
        let raw = parse_str("log_root = /var/log\nEndConfigFile\nlog_type = custm\n");
        assert!(raw.read_string("log_root").is_ok());
        assert!(raw.read_string("log_type").is_err());
    }
}
