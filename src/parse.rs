use chrono::{Local, NaiveDate, TimeZone};

use crate::config::TYPE_SSL;

/// One parsed input log line.
#[derive(Debug, Default, Clone)]
pub struct LineData {
    pub sent: u64,
    pub rcvd: u64,
    pub type_: u32,
    pub time: i64,
    pub domain: String,
    pub line: String,
}

pub fn monthtoint(month: &str) -> u32 {
    match month {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => 0,
    }
}

/// Converts a local-time unix timestamp to an integer date, e.g. 20260804 (or
/// 20260800-style `%Y%m` -> 202608 if `short_date`).
pub fn timetostr(timestamp: i64, short_date: bool) -> u32 {
    let dt = Local
        .timestamp_opt(timestamp, 0)
        .single()
        .unwrap_or_else(|| Local.timestamp_opt(0, 0).unwrap());
    let s = if short_date {
        dt.format("%Y%m").to_string()
    } else {
        dt.format("%Y%m%d").to_string()
    };
    s.parse().unwrap_or(0)
}

pub fn get_today(short_date: bool) -> u32 {
    timetostr(Local::now().timestamp(), short_date)
}

fn mktime_local(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> Option<i64> {
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let naive = date.and_hms_opt(hour, min, sec)?;
    match Local.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Some(dt.timestamp()),
        chrono::LocalResult::Ambiguous(dt, _) => Some(dt.timestamp()),
        chrono::LocalResult::None => None,
    }
}

/// Parses a 28-char bracketed Apache-combined-style timestamp, e.g.
/// `[10/Aug/2026:14:23:01 +0000]`. Returns `None` if the length doesn't match
/// or any field fails to parse (mirrors the original's `time_t` == -1 sentinel).
pub fn parse_common_time(word: &str) -> Option<i64> {
    if word.len() != 28 {
        return None;
    }
    let mday: u32 = word.get(1..3)?.trim().parse().ok()?;
    let mon = monthtoint(word.get(4..7)?);
    let year: i32 = word.get(8..12)?.parse().ok()?;
    let hour: u32 = word.get(13..15)?.parse().ok()?;
    let min: u32 = word.get(16..18)?.parse().ok()?;
    let sec: u32 = word.get(19..21)?.parse().ok()?;
    if mon == 0 {
        return None;
    }
    mktime_local(year, mon, mday, hour, min, sec)
}

/// Parses a 24-char unbracketed Apache-error-log-style timestamp, e.g.
/// `Wed Aug 10 14:23:01 2026`.
pub fn parse_error_time(word: &str) -> Option<i64> {
    if word.len() != 24 {
        return None;
    }
    let mon = monthtoint(word.get(4..7)?);
    let mday: u32 = word.get(8..10)?.trim().parse().ok()?;
    let hour: u32 = word.get(11..13)?.parse().ok()?;
    let min: u32 = word.get(14..16)?.parse().ok()?;
    let sec: u32 = word.get(17..19)?.parse().ok()?;
    let year: i32 = word.get(20..24)?.parse().ok()?;
    if mon == 0 {
        return None;
    }
    mktime_local(year, mon, mday, hour, min, sec)
}

/// Parses DAL's custom CustomLog format: `<ssl> <sent> <rcvd> <domain> <timestamp> <rest...>`.
pub fn parse_common_line(line: &str) -> Result<LineData, ()> {
    let mut data = LineData::default();
    let mut prev_pos = 0usize;
    let mut element = 0u32;

    while element < 5 {
        let pos = match line[prev_pos..].find(' ') {
            Some(p) => prev_pos + p,
            None => return Err(()),
        };
        let word = &line[prev_pos..pos];

        match element {
            0 => {
                if word == "on" {
                    data.type_ = TYPE_SSL;
                }
            }
            1 => data.sent = word.parse().unwrap_or(0),
            2 => data.rcvd = word.parse().unwrap_or(0),
            3 => data.domain = word.to_string(),
            4 => {
                let pos2 = match line[pos + 1..].find(' ') {
                    Some(p) => pos + 1 + p,
                    None => return Err(()),
                };
                let word2 = &line[prev_pos..pos2];
                data.time = parse_common_time(word2).ok_or(())?;
                data.line = line[pos2 + 1..].to_string();
                element += 1;
                break;
            }
            _ => unreachable!(),
        }

        element += 1;
        prev_pos = pos + 1;
    }

    if element < 5 {
        return Err(());
    }

    if data.type_ & TYPE_SSL != 0 {
        data.domain.push_str(crate::config::SSL_SUFFIX);
    }

    Ok(data)
}

/// Parses DAL's ErrorLog format: `[timestamp] [domain] rest...`. The bracketed
/// timestamp occupies bytes 0..26 (`[` + 24-char timestamp + `]`), followed by a
/// space, then `[domain]`, matching the original's fixed-offset `substr()` calls.
/// Uses `.get()` throughout (rather than direct indexing) so malformed/short input
/// from an untrusted log stream fails gracefully instead of panicking.
pub fn parse_error_line(line: &str) -> Result<LineData, ()> {
    if line.get(0..1) != Some("[") || line.get(25..26) != Some("]") {
        return Err(());
    }

    let word = line.get(1..25).ok_or(())?;
    let time = parse_error_time(word).ok_or(())?;

    let tail = line.get(28..).ok_or(())?;
    let pos = 28 + tail.find(']').ok_or(())?;

    let domain = line.get(28..pos).ok_or(())?.to_string();

    let mut out_line = line.to_string();
    let remove_start = 27;
    let remove_len = domain.len() + 2;
    let remove_end = (remove_start + remove_len).min(out_line.len());
    if out_line.get(remove_start..remove_end).is_some() {
        out_line.replace_range(remove_start..remove_end, "");
    }

    Ok(LineData {
        sent: 0,
        rcvd: 0,
        type_: 0,
        time,
        domain,
        line: out_line,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monthtoint_all_months() {
        let months = [
            ("Jan", 1),
            ("Feb", 2),
            ("Mar", 3),
            ("Apr", 4),
            ("May", 5),
            ("Jun", 6),
            ("Jul", 7),
            ("Aug", 8),
            ("Sep", 9),
            ("Oct", 10),
            ("Nov", 11),
            ("Dec", 12),
        ];
        for (name, num) in months {
            assert_eq!(monthtoint(name), num);
        }
        assert_eq!(monthtoint("Xyz"), 0);
    }

    #[test]
    fn common_time_valid() {
        let word = "[10/Aug/2026:14:23:01 +0000]";
        assert_eq!(word.len(), 28);
        let ts = parse_common_time(word);
        assert!(ts.is_some());
    }

    #[test]
    fn common_time_wrong_length() {
        assert_eq!(parse_common_time("short"), None);
    }

    #[test]
    fn error_time_valid() {
        let word = "Wed Aug 10 14:23:01 2026";
        assert_eq!(word.len(), 24);
        let ts = parse_error_time(word);
        assert!(ts.is_some());
    }

    #[test]
    fn error_time_wrong_length() {
        assert_eq!(parse_error_time("short"), None);
    }

    #[test]
    fn common_line_ssl_on() {
        let ts = "[10/Aug/2026:14:23:01 +0000]";
        assert_eq!(ts.len(), 28);
        let line = format!("on 1234 5678 example.com {} GET /index.html HTTP/1.1", ts);
        let data = parse_common_line(&line).unwrap();
        assert_eq!(data.domain, "example.com-ssl");
        assert_eq!(data.sent, 1234);
        assert_eq!(data.rcvd, 5678);
        assert_eq!(data.line, "GET /index.html HTTP/1.1");
    }

    #[test]
    fn common_line_ssl_off() {
        let ts = "[10/Aug/2026:14:23:01 +0000]";
        let line = format!("off 1 2 example.com {} GET / HTTP/1.1", ts);
        let data = parse_common_line(&line).unwrap();
        assert_eq!(data.domain, "example.com");
    }

    #[test]
    fn common_line_missing_fields() {
        assert!(parse_common_line("only two fields").is_err());
    }

    #[test]
    fn error_line_well_formed() {
        // Layout (byte offsets): 0='[' 1..25=timestamp 25=']' 26=' ' 27='['
        // 28..39="example.com" 39=']' 40=' ' 41.."AH00000: ...
        let ts = "Wed Aug 10 14:23:01 2026";
        assert_eq!(ts.len(), 24);
        let line = format!("[{}] [example.com] AH00000: some error message", ts);
        let data = parse_error_line(&line).unwrap();
        assert_eq!(data.domain, "example.com");
        // erase(27, domain.len()+2) removes "[example.com]" (indices 27..40),
        // leaving the space before it (26) adjacent to the space that followed
        // the removed "]" (was at 40) -> two spaces survive in the output.
        assert_eq!(
            data.line,
            format!("[{}]  AH00000: some error message", ts)
        );
    }

    #[test]
    fn error_line_malformed() {
        assert!(parse_error_line("no brackets here").is_err());
    }
}
