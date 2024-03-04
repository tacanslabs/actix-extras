use std::ops::Deref;
use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    io,
    pin::Pin,
    rc::Rc,
    task::{self, Poll},
};

use actix::prelude::*;
use actix_codec::{AsyncRead, AsyncWrite, Encoder, Framed, ReadBuf};
use actix_rt::net::{ActixStream, TcpStream};
use actix_service::{
    boxed::{service, BoxService},
    fn_service, Service,
};
use actix_tls::connect::{ConnectError, ConnectInfo, Connection, ConnectorService, Host};
use backoff::{backoff::Backoff, ExponentialBackoff};
use bytes::BytesMut;
use derive_more::{Deref, DerefMut};
use futures_core::ready;
use log::{debug, error, info, warn};
use redis_async::{
    error::Error as RespError,
    resp::{RespCodec, RespValue},
    resp_array,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

use crate::Error;

/// Command for sending data to Redis.
#[derive(Debug)]
pub struct Command(pub RespValue);

impl Message for Command {
    type Result = Result<RespValue, Error>;
}



#[derive(Clone, Debug, Deref)]
pub struct RedisUrl {
    #[deref(forward)]
    pub addr: String,
    pub password: Option<String>,
    pub index: Option<String>,
}

impl Host for RedisUrl {
    fn hostname(&self) -> &str {
        self.split_once(':')
            .map(|(hostname, _)| hostname)
            .unwrap_or(self)
    }

    fn port(&self) -> Option<u16> {
        self.split_once(':').and_then(|(_, port)| port.parse().ok())
    }
}

/// Redis communication actor.
pub struct RedisActor {
    url: RedisUrl,
    connector: BoxService<ConnectInfo<RedisUrl>, Connection<RedisUrl, TcpStream>, ConnectError>,
    resolver: BoxService<
        Connection<RedisUrl, TcpStream>,
        Connection<RedisUrl, RcStream<dyn ActixStream>>,
        io::Error,
    >,
    backoff: ExponentialBackoff,
    cell: Option<actix::io::FramedWrite<RespValue, RcStream<dyn ActixStream>, RespCodec>>,
    queue: VecDeque<oneshot::Sender<Result<RespValue, Error>>>,
}

impl RedisActor {
    /// Start new `Supervisor` with `RedisActor`.
    pub fn start<S: Into<String>>(addr: S) -> Addr<RedisActor> {
        let addr = addr.into();

        let url = Self::parse_url(addr);
        debug!("Redis Url {url:#?}");

        let resolver = service(fn_service(|req: Connection<RedisUrl, TcpStream>| async {
            debug!("Resolver requested");
            let (stream, addr) = req.into_parts();
            let stream = Rc::new(RefCell::new(stream)) as _;
            let stream = RcStream(stream);
            Ok(Connection::new(addr, stream))
        }));

        let backoff = ExponentialBackoff {
            max_elapsed_time: None,
            ..Default::default()
        };

        Supervisor::start(move |_| RedisActor {
            url,
            connector: service(ConnectorService::default()),
            resolver,
            cell: None,
            backoff,
            queue: VecDeque::new(),
        })
    }

    pub fn parse_url(addr: String) -> RedisUrl {
        let parser = url_parse::core::Parser::new(None);

        let url = parser
            .parse(addr.as_ref())
            .expect("Cannot parse Url from {addr:?}");

        let addr = format!("{}:{}", url.host_str().unwrap(), url.port.unwrap());
        let password = url.password();
        let index = url
            .path_segments()
            .unwrap()
            .first()
            .and_then(|index| if index.is_empty() { None } else { Some(index) })
            .cloned();

        RedisUrl {
            addr,
            password,
            index,
        }
    }
}

impl Actor for RedisActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        let req = ConnectInfo::new(self.url.clone());
        debug!("Started RedisActor with request: {req:#?}");
        self.connector
            .call(req)
            .into_actor(self)
            .map(|res, act, _| {
                debug!("Got response from connector");
                res.map(|conn| act.resolver.call(conn))
            })
            .then(|res, act, _| async { res?.await.map_err(ConnectError::Io) }.into_actor(act))
            .map(|res, act, _| {
                res.map(|conn| {
                    info!("Connected to redis server: {:#?}", act.url);
                    let stream = conn.into_parts().0;
                    stream
                })
            })
            .then(|res, act, _| {
                let url = act.url.clone();

                async move {
                    let stream = res?;

                    // split stream and construct stream reader.
                    let mut writer = stream.clone();
                    let mut reader = Framed::new(stream, RespCodec);

                    // do authentication if needed.
                    if let Some(password) = url.password {
                        info!("Authenticating to redis server");

                        let auth_command = resp_array!["AUTH", password];
                        let mut buf = BytesMut::new();

                        RespCodec
                            .encode(auth_command, &mut buf)
                            .map_err(ConnectError::Io)?;

                        writer.write(&mut buf).await.map_err(ConnectError::Io)?;

                        let res = StreamReader {
                            reader: &mut reader,
                        }
                            .await
                            .map_err(|_| ConnectError::Unresolved)?;

                        if let RespValue::Error(err) = res {
                            error!("Authentication failed with redis server");
                            return Err(ConnectError::Io(io::Error::new(
                                io::ErrorKind::Other,
                                err,
                            )));
                        }
                    };

                    // select index if needed.
                    if let Some(index) = url.index {
                        info!("Index selection to redis server");

                        let select_command = resp_array!["SELECT", index];
                        let mut buf = BytesMut::new();

                        RespCodec
                            .encode(select_command, &mut buf)
                            .map_err(ConnectError::Io)?;

                        writer.write(&mut buf).await.map_err(ConnectError::Io)?;

                        let res = StreamReader {
                            reader: &mut reader,
                        }
                            .await
                            .map_err(|_| ConnectError::Unresolved)?;

                        if let RespValue::Error(err) = res {
                            error!("Index selection failed with redis server");
                            return Err(ConnectError::Io(io::Error::new(
                                io::ErrorKind::Other,
                                err,
                            )));
                        }
                    };

                    Ok((reader, writer))
                }
                    .into_actor(act)
            })
            .map(|res, act, ctx| match res {
                Ok((reader, writer)) => {
                    let writer = actix::io::FramedWrite::new(writer, RespCodec, ctx);
                    act.cell = Some(writer);
                    ctx.add_stream(reader);
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
        warn!("Redis connection dropped: {:#?} error: {}", self.url, err);
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
    struct StreamReader<'a> {
        #[pin]
        reader: &'a mut Framed<RcStream<dyn ActixStream>, RespCodec>
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

#[derive(Deref, DerefMut)]
struct RcStream<Io: ?Sized>(Rc<RefCell<Io>>);

impl<Io: ?Sized> Clone for RcStream<Io> {
    fn clone(&self) -> Self {
        Self(self.deref().clone())
    }
}

impl<Io: AsyncRead + Unpin + ?Sized> AsyncRead for RcStream<Io> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.borrow_mut()).as_mut().poll_read(cx, buf)
    }
}

impl<Io: AsyncWrite + Unpin + ?Sized> AsyncWrite for RcStream<Io> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.borrow_mut()).as_mut().poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.borrow_mut()).as_mut().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.borrow_mut()).as_mut().poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.borrow_mut())
            .as_mut()
            .poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.borrow_mut().is_write_vectored()
    }
}
