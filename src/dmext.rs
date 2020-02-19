use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::fmt::Display;
use std::os::raw::{c_char, c_int};
use std::slice;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use itertools::Itertools;

use crate::fluentd;
use crate::forwarder;

thread_local! {
    /// Thread-local cell holding the entry forwarding channel
    static SENDER: RefCell<Option<mpsc::Sender<fluentd::Entry>>> = RefCell::new(None);
    /// Thread-local cell holding the shutdown channel
    static DONE_RECV: RefCell<Option<mpsc::Receiver<()>>> = RefCell::new(None);
}

lazy_static! {
    static ref EMPTY_RET: CString = CString::new("").unwrap();
}

/// Parse an argc/argv structure to a HashMap, taking adjacent arguments as key-value pairs
fn map_from_args(n: c_int, v: *const *const c_char) -> HashMap<String, String> {
    unsafe {
        slice::from_raw_parts(v, n as usize)
            .iter()
            .map(|&ccs| CStr::from_ptr(ccs).to_string_lossy().into())
    }
    .tuples()
    .collect()
}

/// Parse an argc/argv structure to a Vec of strings
fn vec_from_args(n: c_int, v: *const *const c_char) -> Vec<String> {
    unsafe {
        slice::from_raw_parts(v, n as usize)
            .iter()
            .map(|&ccs| CStr::from_ptr(ccs).to_string_lossy().into())
    }
    .collect()
}

/// Fallback logging method used if we can't forward a log record.
fn fallback_log<Tz: chrono::TimeZone>(ts: &chrono::DateTime<Tz>, record: &HashMap<String, String>)
where
    Tz::Offset: Display,
{
    println!(
        "FLUENTD fluey_log() FALLBACK: {} {:?}",
        ts.to_rfc3339(),
        record
    );
}

/// Initialize forwarding channels and start the forwarding thread
///
/// Takes two arguments:
/// - host: address of the upstream fluentd server
/// - tag: fluentd tag for logs
#[no_mangle]
pub extern "C" fn fluey_start(n: c_int, v: *const *const c_char) -> *const c_char {
    if n != 2 {
        println!(
            "FLUENTD fluey_start(): Wrong number of arguments passed: got {}, expected 2",
            n
        );
    }
    let args = vec_from_args(n, v);
    let host = args[0].clone();
    let tag = args[1].clone();

    let (send, recv) = mpsc::channel();
    let (done_send, done_recv) = mpsc::channel();

    SENDER.with(|c| c.replace(Some(send)));
    DONE_RECV.with(|c| c.replace(Some(done_recv)));

    println!(
        "FLUENTD fluey_start(): Starting forwarding thread connected to '{}'",
        host
    );
    thread::spawn(move || {
        let mut forwarder = forwarder::Forwarder::new(tag, host, recv, done_send);
        forwarder.run();
    });

    EMPTY_RET.as_ptr()
}

/// Shuts down the forwarding thread
///
/// Waits up to 5 seconds for remaining logs to be flushed, then returns to DD
/// Arguments are ignored
#[no_mangle]
pub extern "C" fn fluey_stop(_: c_int, _: *const *const c_char) -> *const c_char {
    SENDER.with(|c| c.replace(None));

    DONE_RECV.with(|c| {
        let recv = match c.replace(None) {
            Some(r) => r,
            None => {
                println!("FLUENTD fluey_stop(): Communication to forwarding thread already broken; was fluey_start() run?");
                return
            },
        };

        println!("FLUENTD fluey_stop(): Waiting up to 5s for forwarding thread to flush and quit");
        let start = std::time::Instant::now();
        match recv.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => {
                let dur = std::time::Instant::now().duration_since(start);
                println!("FLUENTD fluey_stop(): Logs flushed after {:.4}s", dur.as_secs_f64());
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                println!("FLUENTD fluey_stop(): Forwarding thread failed to ACK shutdown within 5s, logs might not be flushed");
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => unimplemented!("unknown fatal error"),
        }
    });

    EMPTY_RET.as_ptr()
}

/// Queues a single Entry to be logged to fluentd
///
/// Arguments are a key-value map representing the fluentd record.
/// Uses current system timestamp.
/// If the forwarding thread is unreachable, logs the record to stdout.
#[no_mangle]
pub extern "C" fn fluey_log(n: c_int, v: *const *const c_char) -> *const c_char {
    let ts = chrono::Utc::now();

    let e = fluentd::Entry {
        time: fluentd::EventTime::new(&ts),
        record: map_from_args(n, v),
    };

    SENDER.with(|s| {
        let mut clear = false;

        match s.borrow_mut().as_ref() {
            Some(send) => {
                send.send(e).unwrap_or_else(|_| {
                    clear = true;
                    // need to recreate the record map since send() steals e
                    fallback_log(&ts, &map_from_args(n, v));
                });
            }
            None => fallback_log(&ts, &e.record),
        };

        // if the recv forwarder is dead, clear out the sender in TLS
        if clear {
            s.replace(None);
        }
    });

    EMPTY_RET.as_ptr()
}
