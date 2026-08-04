use std::sync::atomic::{AtomicBool, Ordering};

use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

use crate::db::{self, Shared};
use crate::logger::{self, LogLevel};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Spawns a background thread that watches for SIGINT/SIGTERM and performs
/// the commit-and-exit itself, rather than merely setting a flag for the
/// main loop to poll. A poll-after-each-line design would miss a signal
/// that arrives while the main thread is blocked on a `read_line` with no
/// further input pending (e.g. Apache sent SIGTERM but hasn't closed the
/// pipe yet) - see the design notes in the port's implementation plan.
///
/// Mutex-held-ness on `shared` structurally replaces the original's
/// `is_commiting` flag: if a commit is in flight on the main thread, this
/// thread's `lock()` simply blocks until it releases, then flushes and
/// exits. A second signal arriving while the first is still flushing is
/// caught by `SHUTDOWN_REQUESTED` before it even attempts the lock, and
/// exits immediately (matches the original's "Forced shutdown" case).
pub fn spawn(shared: Shared) -> std::io::Result<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM])?;

    std::thread::spawn(move || {
        for _sig in signals.forever() {
            if SHUTDOWN_REQUESTED.swap(true, Ordering::SeqCst) {
                eprintln!("Forced shutdown - possible data corruption");
                std::process::exit(1);
            }

            let mut state = shared.lock().unwrap_or_else(|p| p.into_inner());
            logger::write_log(
                &mut state.dal_logfile,
                LogLevel::Info,
                "Received shutdown signal - commiting",
            );
            db::commit_bandwidth(&mut state);
            std::process::exit(1); // matches original: exit(1) even on clean shutdown
        }
    });

    Ok(())
}
