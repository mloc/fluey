use std::io;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use rmp_serde::Serializer;
use serde::Serialize;
use snafu::{ensure, ResultExt, Snafu};

use crate::fluentd;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Could not write buffer to stream: {}", source))]
    WriteBuffer { source: std::io::Error },
    #[snafu(display("Could not deserialize message from stream: {}", source))]
    ReadMessage { source: rmp_serde::decode::Error },
    #[snafu(display("ACK in response is incorrect: got {} want {}", got, want))]
    IncorrectAck { got: String, want: String },
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

pub struct Forwarder {
    tag: String,
    host: String,
    recv: mpsc::Receiver<fluentd::Entry>,
    done_send: mpsc::Sender<()>,
}

impl Forwarder {
    pub fn new(
        tag: String,
        host: String,
        recv: mpsc::Receiver<fluentd::Entry>,
        done_send: mpsc::Sender<()>,
    ) -> Self {
        Self {
            tag: tag,
            host: host,
            recv: recv,
            done_send: done_send,
        }
    }

    pub fn run(&mut self) {
        let mut conn = Connection::new(self.host.clone());
        let mut forward = fluentd::Forward {
            tag: self.tag.clone(),
            entries: Vec::new(),
            option: fluentd::Options {
                chunk: "fluey".into(),
                size: 0,
            },
        };
        let mut buf = Vec::new();

        loop {
            let entries = match mpsc_recv_many(&self.recv, 1000) {
                Some(e) => e,
                None => break,
            };

            forward.option.size = entries.len();
            forward.entries = entries;

            buf.clear();
            forward.serialize(&mut Serializer::new(&mut buf)).unwrap();

            conn.act(|mut s| -> Result<(), Error> {
                s.write_all(&buf).context(WriteBuffer {})?;
                let resp: fluentd::Response = rmp_serde::from_read(s).context(ReadMessage {})?;

                ensure!(
                    resp.ack == forward.option.chunk,
                    IncorrectAck {
                        got: &resp.ack,
                        want: &forward.option.chunk
                    }
                );

                Ok(())
            });

            thread::sleep(Duration::from_millis(500));
        }

        self.done_send
            .send(())
            .unwrap_or_else(|_| panic!("shutdown timed out"));
    }
}

struct Connection {
    host: String,
    stream: Option<TcpStream>,
}

impl<'a> Connection {
    fn new(host: String) -> Self {
        Self {
            host: host,
            stream: None,
        }
    }

    fn act<F, E>(&mut self, f: F)
    where
        F: Fn(&TcpStream) -> Result<(), E>,
    {
        loop {
            let h = &self.host;
            let s = self.stream.get_or_insert_with(|| Self::connect_stream(h));

            if let Ok(()) = f(s) {
                return;
            }

            self.stream = None
        }
    }

    fn connect_stream(host: &str) -> TcpStream {
        loop {
            let res: Result<TcpStream, io::Error> = (|| {
                let ns = TcpStream::connect(host)?;
                ns.set_read_timeout(Some(Duration::from_secs(10)))?;
                Ok(ns)
            })();

            if let Ok(s) = res {
                return s;
            }

            // TODO backoff
            thread::sleep(Duration::from_millis(1000));
        }
    }
}
