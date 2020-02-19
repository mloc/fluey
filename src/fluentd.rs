use std::collections::HashMap;
use std::convert::TryInto;

use byteorder::ByteOrder;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

/// Representation of a fluentd forward-mode message
///
/// See https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#forward-mode
#[derive(Debug, PartialEq, Serialize)]
pub struct Forward {
    pub tag: String,
    pub entries: Vec<Entry>,
    pub option: Options,
}

/// Representation of a single fluentd Entry
///
/// See https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#entry
#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct Entry {
    pub time: EventTime,
    pub record: HashMap<String, String>,
}

/// Extension type used by fluentd to represent timestamps
///
/// See
/// https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format
#[derive(Debug, PartialEq, Clone, Serialize)]
#[serde(rename = "_ExtStruct")]
pub struct EventTime((i8, serde_bytes::ByteBuf));

impl EventTime {
    pub fn new(time: &chrono::DateTime<impl chrono::offset::TimeZone>) -> Self {
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
pub struct Options {
    pub chunk: String,
    pub size: usize,
}

impl Serialize for Options {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_map(Some(2))?;
        s.serialize_entry("chunk", &self.chunk)?;
        s.serialize_entry("size", &self.size)?;
        s.end()
    }
}

/// Fluentd response
///
/// See https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#response
#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct Response {
    pub ack: String,
}
