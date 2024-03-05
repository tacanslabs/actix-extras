use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::{collections::VecDeque, io, task};

use actix::prelude::*;
use actix_rt::net::TcpStream;
use actix_service::boxed::{self, BoxService};
use actix_service::Service;
use actix_tls::connect::{ConnectError, ConnectInfo, Connection, ConnectorService};
use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::BytesMut;
use derive_more::From;
use futures_core::ready;
use lazy_static::lazy_static;
use log::{error, info, warn};
use redis_async::resp::FromResp;
use redis_async::{
    error::Error as RespError,
    resp::{RespCodec, RespValue},
    resp_array,
};
use regex::Regex;
use tokio::io::AsyncWriteExt;
use tokio::{
    io::{split, WriteHalf},
    sync::oneshot,
};
use tokio_util::codec::{Encoder, Framed, FramedRead};

use crate::Error;

const REDIS_URL_REGEX: &str = r#"(redis://)?(:?(?P<password>.*)@)?(?P<addr>(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}|localhost):\d{1,5})(/(?P<index>.*))?"#;
lazy_static! {
    static ref RE: Regex = Regex::new(REDIS_URL_REGEX).unwrap();
}

/// Command for sending data to Redis.
#[derive(Debug)]
pub struct Command(pub RespValue);

impl Message for Command {
    type Result = Result<RespValue, Error>;
}

/// Redis communication actor.
pub struct RedisActor {
    addr: String,
    password: Option<String>,
    index: Option<String>,
    connector: BoxService<ConnectInfo<String>, Connection<String, TcpStream>, ConnectError>,
    backoff: ExponentialBackoff,
    cell: Option<actix::io::FramedWrite<RespValue, WriteHalf<TcpStream>, RespCodec>>,
    queue: VecDeque<oneshot::Sender<Result<RespValue, Error>>>,
}

impl RedisActor {
    /// Start new `Supervisor` with `RedisActor`.
    pub fn start<S: Into<String>>(addr: S) -> Addr<RedisActor> {
        let addr = addr.into();

        let (addr, password, index) = Self::parse_url(addr);

        let backoff = ExponentialBackoff {
            max_elapsed_time: None,
            ..Default::default()
        };

        Supervisor::start(move |_| RedisActor {
            addr,
            password,
            index,
            connector: boxed::service(ConnectorService::default()),
            cell: None,
            backoff,
            queue: VecDeque::new(),
        })
    }

    pub fn parse_url(addr: String) -> (String, Option<String>, Option<String>) {
        let url = RE
            .captures(addr.as_ref())
            .expect("Cannot parse Url from {addr:?}");

        let addr = url
            .name("addr")
            .expect("No HOST:PORT in redis url")
            .as_str()
            .to_string();
        let password = url.name("password").map(|m| m.as_str().to_string());
        let index = url.name("index").map(|m| m.as_str().to_string());
        (addr, password, index)
    }
}

impl Actor for RedisActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        let req = ConnectInfo::new(self.addr.to_owned());
        self.connector
            .call(req)
            .into_actor(self)
            .map(|res, act, _ctx| {
                res.map(|connection| {
                    let (stream, _addr) = connection.into_parts();

                    info!("Connected to redis server: {}", act.addr);

                    stream
                })
            })
            .then(|res, act, _ctx| {
                let password = act.password.clone();
                let index = act.index.clone();
                async move {
                    match res {
                        Ok(mut stream) => {
                            if let Some(password) = password {
                                let command = resp_array!["AUTH", password];
                                let mut buf = BytesMut::new();

                                RespCodec
                                    .encode(command, &mut buf)
                                    .map_err(ConnectError::Io)?;

                                stream.write(&buf).await.map_err(ConnectError::Io)?;

                                let mut framed_stream = Framed::new(stream, RespCodec);
                                let auth_response = StreamReader::from(&mut framed_stream)
                                    .await
                                    .map(String::from_resp)
                                    .map_err(io::Error::other)
                                    .map_err(ConnectError::Io)?;

                                stream = framed_stream.into_inner();

                                info!("Auth response {auth_response:?}");
                            }

                            if let Some(index) = index {
                                let command = resp_array!["SELECT", index];
                                let mut buf = BytesMut::new();

                                RespCodec
                                    .encode(command, &mut buf)
                                    .map_err(ConnectError::Io)?;

                                stream.write(&buf).await.map_err(ConnectError::Io)?;

                                let mut framed_stream = Framed::new(stream, RespCodec);
                                let select_response = StreamReader::from(&mut framed_stream)
                                    .await
                                    .map(String::from_resp)
                                    .map_err(io::Error::other)
                                    .map_err(ConnectError::Io)?;

                                stream = framed_stream.into_inner();

                                info!("Select response {select_response:?}");
                            }

                            Ok(split(stream))
                        }
                        Err(e) => Err(e),
                    }
                }
                .into_actor(act)
            })
            .map(|res, act, ctx| match res {
                Ok((reader, writer)) => {
                    let writer = actix::io::FramedWrite::new(writer, RespCodec, ctx);
                    act.cell = Some(writer);
                    ctx.add_stream(FramedRead::new(reader, RespCodec));
                    act.backoff.reset();
                }
                Err(err) => {
                    error!("Can not connect to redis server: {}", err);
                    // re-connect with backoff time.
                    // we stop current context, supervisor will restart it.
                    if let Some(timeout) = act.backoff.next_backoff() {
                        ctx.run_later(timeout, |_, ctx| ctx.stop());
                    }
                }
            })
            .wait(ctx);
    }
}

impl Supervised for RedisActor {
    fn restarting(&mut self, _: &mut Self::Context) {
        self.cell.take();
        for tx in self.queue.drain(..) {
            let _ = tx.send(Err(Error::Disconnected));
        }
    }
}

impl actix::io::WriteHandler<io::Error> for RedisActor {
    fn error(&mut self, err: io::Error, _: &mut Self::Context) -> Running {
        warn!("Redis connection dropped: {} error: {}", self.addr, err);
        Running::Stop
    }
}

impl StreamHandler<Result<RespValue, RespError>> for RedisActor {
    fn handle(&mut self, msg: Result<RespValue, RespError>, ctx: &mut Self::Context) {
        match msg {
            Err(e) => {
                if let Some(tx) = self.queue.pop_front() {
                    let _ = tx.send(Err(e.into()));
                }
                ctx.stop();
            }
            Ok(val) => {
                if let Some(tx) = self.queue.pop_front() {
                    let _ = tx.send(Ok(val));
                }
            }
        }
    }
}

impl Handler<Command> for RedisActor {
    type Result = ResponseFuture<Result<RespValue, Error>>;

    fn handle(&mut self, msg: Command, _: &mut Self::Context) -> Self::Result {
        let (tx, rx) = oneshot::channel();
        if let Some(ref mut cell) = self.cell {
            self.queue.push_back(tx);
            cell.write(msg.0);
        } else {
            let _ = tx.send(Err(Error::NotConnected));
        }

        Box::pin(async move { rx.await.map_err(|_| Error::Disconnected)? })
    }
}

pin_project_lite::pin_project! {
    #[derive(From)]
    struct StreamReader<'a> {
        #[pin]
        reader: &'a mut Framed<TcpStream, RespCodec>
    }
}

impl Future for StreamReader<'_> {
    type Output = Result<RespValue, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match ready!(self.project().reader.poll_next(cx)?) {
            Some(res) => Poll::Ready(Ok(res)),
            None => Poll::Ready(Err(Error::Disconnected)),
        }
    }
}
