use std::{error::Error, fmt::Display, pin::Pin, task::Poll};

use crate::{
    TransportError,
    stream::{Stream, StreamError},
};
use futures::{FutureExt, future::BoxFuture};
use iroh::endpoint::{RecvStream, SendStream};
use libp2p::core::StreamMuxer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
pub struct ConnectionError {
    kind: ConnectionErrorKind,
}

#[derive(Debug)]
pub enum ConnectionErrorKind {
    Accept(String),
    Open(String),
    Stream(String),
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConnectionError: {:?}", self.kind)
    }
}

impl Error for ConnectionError {}

impl From<iroh::endpoint::ConnectionError> for ConnectionError {
    fn from(err: iroh::endpoint::ConnectionError) -> Self {
        Self {
            kind: ConnectionErrorKind::Accept(err.to_string()),
        }
    }
}

impl From<&str> for ConnectionError {
    fn from(err: &str) -> Self {
        Self {
            kind: ConnectionErrorKind::Accept(err.to_string()),
        }
    }
}

impl From<StreamError> for ConnectionError {
    fn from(err: StreamError) -> Self {
        Self {
            kind: ConnectionErrorKind::Stream(err.to_string()),
        }
    }
}

pub struct Connection {
    connection: iroh::endpoint::Connection,
    incoming: Option<BoxFuture<'static, Result<(SendStream, RecvStream), ConnectionError>>>,
    outgoing: Option<BoxFuture<'static, Result<(SendStream, RecvStream), ConnectionError>>>,
    closing: Option<BoxFuture<'static, iroh::endpoint::ConnectionError>>,
    closed: BoxFuture<'static, iroh::endpoint::ConnectionError>,
}

pub struct Connecting {
    pub connecting:
        BoxFuture<'static, Result<(libp2p::PeerId, iroh::endpoint::Connection), TransportError>>,
}

impl Connection {
    pub fn new(connection: iroh::endpoint::Connection) -> Self {
        tracing::debug!("Connection::new - Creating new connection wrapper");
        let closed_connection = connection.clone();
        let closed = async move { closed_connection.closed().await }.boxed();
        Self {
            connection,
            incoming: None,
            outgoing: None,
            closing: None,
            closed,
        }
    }
}

impl StreamMuxer for Connection {
    type Substream = Stream;
    type Error = ConnectionError;

    fn poll_inbound(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Self::Substream, Self::Error>> {
        let this = self.get_mut();

        let incoming = this.incoming.get_or_insert_with(|| {
            tracing::debug!("Connection::poll_inbound - Setting up incoming stream future");
            let connection = this.connection.clone();
            async move {
                tracing::debug!("Connection::poll_inbound - Accepting bidirectional stream");
                match connection.accept_bi().await {
                    Ok((s, mut r)) => {
                        tracing::debug!("Connection::poll_inbound - Bidirectional stream accepted, reading handshake byte");
                        r.read_u8().await.map_err(|e| {
                            tracing::error!("Connection::poll_inbound - Failed to read handshake byte: {}", e);
                            ConnectionError::from("Failed to read from stream")
                        })?;
                        tracing::debug!("Connection::poll_inbound - Handshake byte read successfully");
                        Ok((s, r))
                    },
                    Err(e) => {
                        tracing::error!("Connection::poll_inbound - Failed to accept bidirectional stream: {}", e);
                        Err(ConnectionError::from("Iroh handshake failed during accept"))
                    }
                }
             }.boxed()
        });

        let (send, recv) = futures::ready!(incoming.poll_unpin(cx))?;
        this.incoming.take();
        tracing::debug!("Connection::poll_inbound - Inbound stream ready, creating Stream wrapper");
        Poll::Ready(Stream::new(send, recv).map_err(Into::into))
    }

    fn poll_outbound(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Self::Substream, Self::Error>> {
        let this = self.get_mut();

        let outgoing = this.outgoing.get_or_insert_with(|| {
            tracing::debug!("Connection::poll_outbound - Setting up outgoing stream future");
            let connection = this.connection.clone();
            async move {
                tracing::debug!("Connection::poll_outbound - Opening bidirectional stream");
                match connection.open_bi().await {
                    Ok((mut s, r)) => {
                        tracing::debug!("Connection::poll_outbound - Bidirectional stream opened, writing handshake byte");
                        // one byte iroh-handshake since accept only connects after open and write, not just open
                        s.write_u8(0).await.map_err(|e| {
                            tracing::error!("Connection::poll_outbound - Failed to write handshake byte: {}", e);
                            ConnectionError::from("Failed to write to stream")
                        })?;
                        tracing::debug!("Connection::poll_outbound - Handshake byte written successfully");
                        Ok((s, r))
                    }
                    Err(e) => {
                        tracing::error!("Connection::poll_outbound - Failed to open bidirectional stream: {}", e);
                        Err(ConnectionError::from("Iroh handshake failed during open"))
                    }
                }
            }.boxed()
        });

        let (send, recv) = futures::ready!(outgoing.poll_unpin(cx))?;
        this.outgoing.take();
        tracing::debug!(
            "Connection::poll_outbound - Outbound stream ready, creating Stream wrapper"
        );
        Poll::Ready(Stream::new(send, recv).map_err(Into::into))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        let closing = this.closing.get_or_insert_with(|| {
            tracing::debug!("Connection::poll_close - Closing connection");
            this.connection.close(From::from(0u32), &[]);
            let connection = this.connection.clone();
            async move {
                tracing::debug!("Connection::poll_close - Waiting for connection to close");
                connection.closed().await
            }
            .boxed()
        });

        let close_err = futures::ready!(closing.poll_unpin(cx));
        // After calling connection.close(0, &[]) the closed() future resolves
        // with LocallyClosed or ApplicationClosed(code=0). Both indicate a
        // clean shutdown. Anything else is a real error.
        match &close_err {
            iroh::endpoint::ConnectionError::LocallyClosed => {
                tracing::debug!(
                    "Connection::poll_close - Connection closed successfully (locally)"
                );
                Poll::Ready(Ok(()))
            }
            iroh::endpoint::ConnectionError::ApplicationClosed(close)
                if close.error_code == 0u32.into() =>
            {
                tracing::debug!(
                    "Connection::poll_close - Connection closed successfully (application, code 0)"
                );
                Poll::Ready(Ok(()))
            }
            _ => {
                tracing::error!(
                    "Connection::poll_close - Failed to close connection: {}",
                    close_err
                );
                Poll::Ready(Err(close_err.into()))
            }
        }
    }

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<libp2p::core::muxing::StreamMuxerEvent, Self::Error>> {
        // Report closure even when libp2p applies backpressure to inbound streams.
        let error = futures::ready!(self.get_mut().closed.poll_unpin(cx));
        Poll::Ready(Err(error.into()))
    }
}

impl Future for Connecting {
    type Output = Result<(libp2p::PeerId, libp2p::core::muxing::StreamMuxerBox), TransportError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        tracing::debug!("Connecting::poll - Polling connection future");
        let (peer_id, conn) = match self.connecting.poll_unpin(cx) {
            Poll::Ready(Ok((peer_id, conn))) => {
                tracing::debug!("Connecting::poll - Connection established");
                (peer_id, conn)
            }
            Poll::Ready(Err(e)) => {
                tracing::error!("Connecting::poll - Connection failed: {}", e);
                return Poll::Ready(Err(e));
            }
            Poll::Pending => {
                tracing::trace!("Connecting::poll - Connection still pending");
                return Poll::Pending;
            }
        };

        let muxer = Connection::new(conn);

        tracing::debug!("Connecting::poll - Connection muxer created");
        Poll::Ready(Ok((
            peer_id,
            libp2p::core::muxing::StreamMuxerBox::new(muxer),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{future::poll_fn, task::ArcWake};
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::Context,
        time::Duration,
    };

    #[derive(Default)]
    struct ClosureWake(AtomicBool);

    impl ArcWake for ClosureWake {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.store(true, Ordering::SeqCst);
        }
    }

    async fn local_endpoint(secret_key: iroh::SecretKey) -> iroh::Endpoint {
        let config = iroh::endpoint::QuicTransportConfig::builder()
            .enable_segmentation_offload(false)
            .build();
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(secret_key)
            .alpns(vec![b"/test/muxer-lifecycle".to_vec()])
            .clear_ip_transports()
            .bind_addr(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .portmapper_config(iroh::endpoint::PortmapperConfig::Disabled)
            .net_report_config(iroh::endpoint::NetReportConfig::minimal())
            .transport_config(config)
            .bind()
            .await
            .unwrap()
    }

    async fn connect_pair(
        caller: &iroh::Endpoint,
        receiver: &iroh::Endpoint,
    ) -> (iroh::endpoint::Connection, iroh::endpoint::Connection) {
        let addr = iroh::EndpointAddr::new(receiver.id()).with_ip_addr(receiver.bound_sockets()[0]);
        let (outgoing, incoming) =
            tokio::join!(caller.connect(addr, b"/test/muxer-lifecycle"), async {
                receiver.accept().await.unwrap().await.unwrap()
            },);
        (outgoing.unwrap(), incoming)
    }

    async fn exchange_byte(caller: &mut Connection, receiver: &mut Connection) {
        let (outgoing, incoming) = tokio::join!(
            poll_fn(|cx| Pin::new(&mut *caller).poll_outbound(cx)),
            poll_fn(|cx| Pin::new(&mut *receiver).poll_inbound(cx)),
        );
        let mut outgoing = outgoing.unwrap();
        let mut incoming = incoming.unwrap();
        futures::AsyncWriteExt::write_all(&mut outgoing, &[42])
            .await
            .unwrap();
        let mut received = [0];
        futures::AsyncReadExt::read_exact(&mut incoming, &mut received)
            .await
            .unwrap();
        assert_eq!(received, [42]);
    }

    #[tokio::test]
    async fn remote_endpoint_close_wakes_and_invalidates_all_muxers() {
        let receiver_key = iroh::SecretKey::generate();
        let caller = local_endpoint(iroh::SecretKey::generate()).await;
        let receiver = local_endpoint(receiver_key.clone()).await;
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            let mut connections = Vec::new();
            let mut peer_muxers = Vec::new();
            let mut muxers = Vec::new();
            for _ in 0..3 {
                let (outgoing, incoming) = connect_pair(&caller, &receiver).await;
                assert!(connections.iter().all(|conn: &iroh::endpoint::Connection| {
                    conn.stable_id() != outgoing.stable_id()
                }));
                let mut muxer = Connection::new(outgoing.clone());
                let mut peer_muxer = Connection::new(incoming);
                exchange_byte(&mut muxer, &mut peer_muxer).await;
                peer_muxers.push(peer_muxer);
                connections.push(outgoing);
                muxers.push((muxer, Arc::new(ClosureWake::default())));
            }
            // A second wrapper must independently observe the same QUIC connection closing.
            muxers.push((
                Connection::new(connections[0].clone()),
                Arc::new(ClosureWake::default()),
            ));
            for (muxer, wake) in &mut muxers {
                let waker = futures::task::waker(wake.clone());
                assert!(
                    Pin::new(muxer)
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                wake.0.store(false, Ordering::SeqCst);
            }

            assert!(
                connections
                    .iter()
                    .all(|connection| connection.close_reason().is_none())
            );
            receiver.close().await;
            for connection in &connections {
                assert!(matches!(
                    connection.closed().await,
                    iroh::endpoint::ConnectionError::ApplicationClosed(close)
                        if close.error_code == 0u32.into()
                ));
            }
            let woken = muxers
                .iter()
                .filter(|(_, wake)| wake.0.load(Ordering::SeqCst))
                .count();
            let mut closure_errors = 0;
            for (muxer, wake) in &mut muxers {
                let waker = futures::task::waker(wake.clone());
                let mut cx = Context::from_waker(&waker);
                if matches!(Pin::new(muxer).poll(&mut cx), Poll::Ready(Err(_))) {
                    closure_errors += 1;
                }
            }
            let mut late_muxer = Connection::new(connections[0].clone());
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let late_closed =
                matches!(Pin::new(&mut late_muxer).poll(&mut cx), Poll::Ready(Err(_)));
            assert!(matches!(
                Pin::new(&mut late_muxer).poll_outbound(&mut cx),
                Poll::Ready(Err(_))
            ));
            assert!(matches!(
                Pin::new(&mut late_muxer).poll_inbound(&mut cx),
                Poll::Ready(Err(_))
            ));

            let restarted = local_endpoint(receiver_key).await;
            assert_eq!(receiver.id(), restarted.id());
            let (outgoing, incoming) = connect_pair(&caller, &restarted).await;
            let mut fresh = Connection::new(outgoing);
            let mut fresh_peer = Connection::new(incoming);
            exchange_byte(&mut fresh, &mut fresh_peer).await;
            poll_fn(|cx| Pin::new(&mut fresh).poll_close(cx))
                .await
                .unwrap();
            restarted.close().await;
            (woken, closure_errors, late_closed)
        })
        .await;
        tokio::join!(caller.close(), receiver.close());
        let (woken, closure_errors, late_closed) =
            result.expect("local QUIC lifecycle exceeded 3 seconds");
        eprintln!(
            "transport_closed=3; muxer_closure_errors={closure_errors}/4; closure_wakes={woken}/4; late_closed={late_closed}; same_identity_restart_exchange=ok; local_close=ok"
        );
        assert_eq!(
            closure_errors, 4,
            "known-closed QUIC connections must invalidate every muxer"
        );
        assert_eq!(
            woken, 4,
            "remote closure must wake every pending muxer without a timer repoll"
        );
        assert!(
            late_closed,
            "a muxer first polled after closure must immediately fail"
        );
    }
}
