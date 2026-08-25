use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use chrono::Local;

const LOG_MIN_FILE_LEVEL: i32 = 1;
const LOG_MIN_OUT_LEVEL: i32 = 1;
const LOG_MIN_ERR_LEVEL: i32 = 2;

/// Mirrors the original's DEBUG/INFO/WARNING/ERROR/FATAL levels. Unlike the
/// C `writelog(int level, ...)`, an out-of-range level can't be constructed
/// here at all, so the original's "unrecognized level normalizes to INFO"
/// switch-fallthrough has no equivalent to reproduce - every caller already
/// passes one of these five variants.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Fatal,
}

impl LogLevel {
    fn numeric(self) -> i32 {
        match self {
            LogLevel::Debug => 0,
            LogLevel::Info => 1,
            LogLevel::Warning => 2,
            LogLevel::Error => 3,
            LogLevel::Fatal => 4,
        }
    }

    fn label(self) -> &'static str {
        match self {
            LogLevel::Debug => "Debug",
            LogLevel::Info => "Info",
            LogLevel::Warning => "Warning",
            LogLevel::Error => "Error",
            LogLevel::Fatal => "Fatal error",
        }
    }
}

/// Opens DAL's own operational logfile in append mode. Matches the
/// original: failure to open isn't fatal, `write_log` simply skips the
/// file-write branch if the handle isn't present.
pub fn open(path: &str) -> Option<File> {
    if Path::new(path).as_os_str().is_empty() {
        return None;
    }
    OpenOptions::new().create(true).append(true).open(path).ok()
}

/// Routes a pre-formatted message to DAL's own logfile (level >= INFO),
/// stderr (level >= WARNING), or stdout (INFO only) - matching the
/// original's numeric-threshold routing exactly. DEBUG messages go nowhere
/// in a non-debug build, which is intentional (preserved as-is), not a bug.
pub fn write_log(file: &mut Option<File>, level: LogLevel, message: &str) {
    let lvl = level.numeric();
    let line = format!("{} : {}", level.label(), message);

    if lvl >= LOG_MIN_FILE_LEVEL {
        if let Some(f) = file {
            let date = Local::now().format("[%a %b %e %H:%M:%S %Y] ");
            let _ = writeln!(f, "{date}{line}");
        }
    }

    if lvl >= LOG_MIN_ERR_LEVEL {
        eprintln!("{line}");
    } else if lvl >= LOG_MIN_OUT_LEVEL {
        println!("{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_match_original() {
        assert_eq!(LogLevel::Debug.label(), "Debug");
        assert_eq!(LogLevel::Info.label(), "Info");
        assert_eq!(LogLevel::Warning.label(), "Warning");
        assert_eq!(LogLevel::Error.label(), "Error");
        assert_eq!(LogLevel::Fatal.label(), "Fatal error");
    }

    #[test]
    fn thresholds_match_original() {
        // DEBUG goes nowhere (0 < LOG_MIN_FILE_LEVEL and 0 < LOG_MIN_OUT_LEVEL).
        assert!(LogLevel::Debug.numeric() < LOG_MIN_FILE_LEVEL);
        assert!(LogLevel::Debug.numeric() < LOG_MIN_OUT_LEVEL);
        // INFO reaches file + stdout, not stderr.
        assert!(LogLevel::Info.numeric() >= LOG_MIN_FILE_LEVEL);
        assert!(LogLevel::Info.numeric() >= LOG_MIN_OUT_LEVEL);
        assert!(LogLevel::Info.numeric() < LOG_MIN_ERR_LEVEL);
        // WARNING+ reaches file + stderr.
        assert!(LogLevel::Warning.numeric() >= LOG_MIN_ERR_LEVEL);
    }
}
