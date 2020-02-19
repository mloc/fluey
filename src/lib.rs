extern crate serde;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
extern crate byteorder;
extern crate chrono;
extern crate itertools;
extern crate rand;
extern crate rmp_serde;
extern crate serde_bytes;

use crate::byteorder::ByteOrder;
use crate::itertools::Itertools;
use rmp_serde::Serializer;
use serde::ser::SerializeMap;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fmt::Display;
use std::io::Write;
use std::net::TcpStream;
use std::os::raw::{c_char, c_int};
use std::slice;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

thread_local! {
    /// Thread-local cell holding the entry forwarding channel
    static SENDER: RefCell<Option<mpsc::Sender<Entry>>> = RefCell::new(None);
    /// Thread-local cell holding the shutdown channel
    static DONE_RECV: RefCell<Option<mpsc::Receiver<()>>> = RefCell::new(None);
}

lazy_static! {
    static ref EMPTY_RET: CString = CString::new("").unwrap();
}

/// Representation of a fluentd forward-mode message
///
/// See https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#forward-mode
#[derive(Debug, PartialEq, Serialize)]
struct Forward {
    tag: String,
    entries: Vec<Entry>,
    //option: HashMap<String, usize>,
    option: Options,
}

/// Representation of a single fluentd Entry
///
/// See https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#entry
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
struct Entry {
    time: EventTime,
    record: HashMap<String, String>,
}

/// Extension type used by fluentd to represent timestamps
///
/// See
/// https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
#[serde(rename = "_ExtStruct")]
struct EventTime((i8, serde_bytes::ByteBuf));

impl EventTime {
    fn new(time: &chrono::DateTime<impl chrono::offset::TimeZone>) -> Self {
        // build the msgpack extension struct, id 0
        let mut buf = [0; 8];
        byteorder::BE::write_u32(&mut buf, time.timestamp().try_into().unwrap());
        byteorder::BE::write_u32(&mut buf[4..], time.timestamp_subsec_nanos());
        EventTime((0, serde_bytes::ByteBuf::from(buf.to_vec())))
    }
}

/// Option struct used in forwarding messages
///
/// See
/// https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#option
#[derive(Debug, PartialEq, Clone, Default)]
struct Options {
    chunk: String,
    size: usize,
}

impl Serialize for Options {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("chunk", &self.chunk);
        s.serialize_entry("size", &self.size);
        s.end()
    }
}

/// Fluentd response
///
/// See https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#response
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
struct Response {
    ack: String,
}

/// Helper to block then fetch as many messages as possible from a Receiver, with an upper bound
fn mpsc_recv_many<V>(recv: &mpsc::Receiver<V>, n: usize) -> Option<Vec<V>> {
    let mut entries = match recv.recv() {
        Ok(e) => vec![e],
        Err(mpsc::RecvError) => return None,
    };
    recv.try_iter().take(n - 1).for_each(|e| entries.push(e));

    Some(entries)
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
        let mut stream = None;
        let mut forward = Forward {
            tag: tag,
            entries: Vec::new(),
            option: Options {
                chunk: "p8n9gmxTQVC8/nh2wlKKeQ==".into(),
                size: 0,
            },
        };
        let mut buf = Vec::new();

        loop {
            let entries = match mpsc_recv_many(&recv, 1000) {
                Some(e) => e,
                None => break,
            };

            //forward.option.size = entries.len();
            forward.entries = entries;
            let l = forward.entries.len();

            buf.clear();
            forward.serialize(&mut Serializer::new(&mut buf)).unwrap();
            println!("{:?}", buf);

            loop {
                let stream = &mut stream;
                let res: Result<(), Box<dyn Error>> = (|| {
                    if let None = stream.as_ref() {
                        let ns = TcpStream::connect(&host)?;
                        ns.set_read_timeout(Some(Duration::from_secs(10)))?;
                        *stream = Some(ns);
                    }
                    let mut sr = stream.as_ref().unwrap();
                    sr.write_all(&buf)?;
                    let resp: Response = rmp_serde::from_read(sr)?;
                    if resp.ack != forward.option.chunk {
                        return Err("bad ack".into());
                    }
                    Ok(())
                })();

                println!("{:?}", res);
                if res.is_ok() {
                    break;
                } else {
                    *stream = None;
                }
                thread::sleep(Duration::from_millis(1000));
            }
            println!("yay, sent {}", l);

            thread::sleep(Duration::from_millis(500));
        }

        done_send
            .send(())
            .unwrap_or_else(|_| unimplemented!("shutdown timed out"));
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

    let e = Entry {
        time: EventTime::new(&ts),
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

        // if the recv thread is dead, clear out the sender in TLS
        if clear {
            s.replace(None);
        }
    });

    EMPTY_RET.as_ptr()
}
