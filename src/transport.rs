use std::fmt::Display;
use std::sync::{Arc, Mutex};

use futures::{FutureExt, future::BoxFuture};
use iroh::{EndpointId, protocol::ProtocolHandler};
use libp2p::PeerId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    connection::{Connecting, Connection},
    helper, node_id_to_peerid,
};

/// Filter callback for incoming peer connections.
/// Return `true` to accept the connection, `false` to reject it.
pub type PeerFilter = Arc<dyn Fn(&EndpointId) -> bool + Send + Sync>;

/// Configuration for the iroh-backed libp2p transport.
pub struct TransportConfig {
    /// Relay mode. Defaults to `RelayMode::Default` (n0.computer relays).
    pub relay_mode: iroh::RelayMode,
    /// Connection timeout for dial attempts. Defaults to 300s.
    pub timeout: std::time::Duration,
    /// Optional filter: return `true` to accept a peer, `false` to reject.
    /// If `None`, all peers are accepted.
    pub peer_filter: Option<PeerFilter>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            relay_mode: iroh::RelayMode::Default,
            timeout: std::time::Duration::from_secs(300),
            peer_filter: None,
        }
    }
}

impl std::fmt::Debug for TransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportConfig")
            .field("relay_mode", &format!("{:?}", self.relay_mode))
            .field("timeout", &self.timeout)
            .field("peer_filter", &self.peer_filter.as_ref().map(|_| "Some(<fn>)"))
            .finish()
    }
}

#[derive(Debug)]
pub struct Transport {
    _secret_key: iroh::SecretKey,
    protocol: Protocol,
    endpoint: iroh::Endpoint,

    pub node_id: EndpointId,
    pub peer_id: libp2p::PeerId,

    pub timeout: std::time::Duration,
    listener_id: Option<libp2p::core::transport::ListenerId>,
    _router: Option<iroh::protocol::Router>,
    transport_events_rx:
        UnboundedReceiver<libp2p::core::transport::TransportEvent<Connecting, TransportError>>,
    transport_events_tx:
        UnboundedSender<libp2p::core::transport::TransportEvent<Connecting, TransportError>>,
}

#[derive(Clone)]
pub struct Protocol {
    peer_filter: Option<PeerFilter>,
    shared_listener_id: Arc<Mutex<Option<libp2p::core::transport::ListenerId>>>,
    transport_tx:
        UnboundedSender<libp2p::core::transport::TransportEvent<Connecting, TransportError>>,
    local_node_id: EndpointId,
}

impl std::fmt::Debug for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Protocol")
            .field("peer_filter", &self.peer_filter.as_ref().map(|_| "<fn>"))
            .field("shared_listener_id", &self.shared_listener_id)
            .field("local_node_id", &self.local_node_id)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct TransportError {
    kind: TransportErrorKind,
}

#[derive(Clone, Debug)]
pub enum TransportErrorKind {
    Dial(String),
    Listen(String),
}

impl Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TransportError: {:?}", self.kind)
    }
}

impl From<iroh::endpoint::BindError> for TransportError {
    fn from(err: iroh::endpoint::BindError) -> Self {
        Self {
            kind: TransportErrorKind::Listen(err.to_string()),
        }
    }
}

impl From<&str> for TransportError {
    fn from(err: &str) -> Self {
        Self {
            kind: TransportErrorKind::Listen(err.to_string()),
        }
    }
}

impl std::error::Error for TransportError {}

impl Transport {
    /// Create a transport with the default configuration (n0.computer relays, 300s timeout).
    pub async fn new(keypair: Option<&libp2p::identity::Keypair>) -> Result<Self, TransportError> {
        Self::with_config(keypair, TransportConfig::default()).await
    }

    /// Create a transport with custom configuration.
    pub async fn with_config(
        keypair: Option<&libp2p::identity::Keypair>,
        config: TransportConfig,
    ) -> Result<Self, TransportError> {
        tracing::debug!("Transport::with_config - Creating new transport");
        let (transport_events_tx, transport_events_rx) = tokio::sync::mpsc::unbounded_channel();

        let (secret_key, peer_id) = if let Some(kp) = keypair {
            tracing::debug!("Transport::with_config - Using provided keypair");
            let sk = helper::libp2p_keypair_to_iroh_secret(kp).ok_or_else(|| TransportError {
                kind: TransportErrorKind::Listen(
                    "Failed to convert libp2p keypair to iroh secret key".to_string(),
                ),
            })?;
            let pid = libp2p::PeerId::from(kp.public());
            tracing::debug!(
                "Transport::with_config - Peer ID: {}, Node ID: {:?}",
                pid,
                sk.public()
            );
            (sk, pid)
        } else {
            tracing::debug!("Transport::with_config - Generating new keypair");
            let sk = iroh::SecretKey::generate(&mut rand::rng());
            let node_id = sk.public();
            let node_id_bytes = node_id.as_bytes();
            let ed25519_pubkey = libp2p::identity::ed25519::PublicKey::try_from_bytes(
                node_id_bytes,
            )
            .map_err(|e| TransportError {
                kind: TransportErrorKind::Listen(format!(
                    "Failed to create libp2p public key from iroh node id: {e}"
                )),
            })?;
            let libp2p_pubkey = libp2p::identity::PublicKey::from(ed25519_pubkey);
            let pid = libp2p::PeerId::from_public_key(&libp2p_pubkey);
            tracing::debug!(
                "Transport::with_config - Generated Peer ID: {}, Node ID: {:?}",
                pid,
                node_id
            );
            (sk, pid)
        };

        let relay_mode = config.relay_mode;
        let peer_filter = config.peer_filter;
        let (waiter_tx, mut waiter_rx) =
            tokio::sync::mpsc::channel::<Result<(Protocol, iroh::Endpoint), TransportError>>(1);

        tokio::spawn({
            let transport_events_tx = transport_events_tx.clone();
            let secret_key = secret_key.clone();
            async move {
                tracing::debug!("Transport::with_config - Spawned task: Initializing iroh endpoint");
                if let Ok(endpoint) = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
                    .secret_key(secret_key)
                    .relay_mode(relay_mode)
                    .bind()
                    .await
                    .map_err(|e| TransportError {
                        kind: TransportErrorKind::Listen(e.to_string()),
                    })
                {
                    tracing::debug!("Transport::with_config - Iroh endpoint created successfully");
                    let protocol = Protocol::new(endpoint.clone(), transport_events_tx, peer_filter);

                    if waiter_tx.send(Ok((protocol, endpoint))).await.is_ok() {
                        tracing::debug!("Transport::with_config - Protocol sent to waiter channel");
                        return;
                    }
                }

                tracing::error!("Transport::with_config - Failed to initialize iroh endpoint");
                waiter_tx
                    .send(Err(TransportError {
                        kind: TransportErrorKind::Listen(
                            "Failed to initialize iroh endpoint".to_string(),
                        ),
                    }))
                    .await
                    .expect("fatal: failed to send error through channel");
            }
        });

        let (protocol, endpoint) = waiter_rx.recv().await.ok_or_else(|| TransportError {
            kind: TransportErrorKind::Listen(
                "Failed to receive transport from initialization".to_string(),
            ),
        })??;

        tracing::debug!("Transport::with_config - Transport created successfully");
        Ok(Transport {
            transport_events_tx,
            transport_events_rx,
            _secret_key: secret_key.clone(),
            node_id: secret_key.public(),
            peer_id,
            timeout: config.timeout,
            protocol,
            endpoint,
            listener_id: None,
            _router: None,
        })
    }

    /// Returns a handle to the underlying iroh endpoint.
    ///
    /// Use this to read the local EndpointId, configure connection
    /// acceptance policies, or access connection metrics.
    pub fn endpoint(&self) -> &iroh::Endpoint {
        &self.endpoint
    }

    /// Gracefully close the transport and its underlying iroh endpoint.
    pub async fn close(&self) {
        self.endpoint.close().await;
    }
}

impl Protocol {
    const ALPN: &'static [u8] = b"/iroh/libp2p-transport/0.1.0";
    pub fn new(
        endpoint: iroh::Endpoint,
        transport_tx: UnboundedSender<
            libp2p::core::transport::TransportEvent<Connecting, TransportError>,
        >,
        peer_filter: Option<PeerFilter>,
    ) -> Self {
        tracing::debug!("Protocol::new - Creating protocol handler");
        let local_node_id = endpoint.id();
        let shared_listener_id = Arc::new(Mutex::new(None));

        Self {
            peer_filter,
            shared_listener_id,
            transport_tx,
            local_node_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_creates_transport_with_default_config() {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let transport = Transport::new(Some(&keypair)).await;
        assert!(transport.is_ok());
        let t = transport.unwrap();
        assert_eq!(t.timeout, std::time::Duration::from_secs(300));
    }

    #[tokio::test]
    async fn with_config_custom_timeout() {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let config = TransportConfig {
            timeout: std::time::Duration::from_secs(60),
            ..Default::default()
        };
        let transport = Transport::with_config(Some(&keypair), config).await;
        assert!(transport.is_ok());
        let t = transport.unwrap();
        assert_eq!(t.timeout, std::time::Duration::from_secs(60));
    }

    #[tokio::test]
    async fn with_config_default_equals_new() {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let config = TransportConfig::default();
        let transport = Transport::with_config(Some(&keypair), config).await;
        assert!(transport.is_ok());
        let t = transport.unwrap();
        assert_eq!(t.timeout, std::time::Duration::from_secs(300));
    }

    #[tokio::test]
    async fn endpoint_returns_valid_id() {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let transport = Transport::new(Some(&keypair)).await.unwrap();
        let endpoint = transport.endpoint();
        assert_eq!(endpoint.id(), transport.node_id);
    }

    #[tokio::test]
    async fn with_config_relay_disabled() {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let config = TransportConfig {
            relay_mode: iroh::RelayMode::Disabled,
            ..Default::default()
        };
        let transport = Transport::with_config(Some(&keypair), config).await;
        assert!(transport.is_ok());
    }
}

impl libp2p::Transport for Transport {
    type Output = (PeerId, libp2p::core::muxing::StreamMuxerBox);

    type Error = TransportError;

    type ListenerUpgrade = Connecting;

    type Dial = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn listen_on(
        &mut self,
        id: libp2p::core::transport::ListenerId,
        _addr: libp2p::Multiaddr,
    ) -> Result<(), libp2p::core::transport::TransportError<Self::Error>> {
        tracing::debug!(
            "Transport::listen_on - Listener ID: {:?}, Address: {:?}",
            id,
            _addr
        );

        if self.listener_id.is_some() {
            tracing::warn!("Transport::listen_on - Listener already exists");
            return Err(libp2p::core::transport::TransportError::Other(
                TransportError {
                    kind: TransportErrorKind::Listen(
                        "Listener already exists for this transport".to_string(),
                    ),
                },
            ));
        }

        tracing::debug!(
            "Transport::listen_on - Creating router with ALPN: {:?}",
            std::str::from_utf8(Protocol::ALPN)
        );
        let router = iroh::protocol::Router::builder(self.endpoint.clone())
            .accept(Protocol::ALPN, self.protocol.clone())
            .spawn();

        self._router = Some(router);
        self.listener_id = Some(id);

        // Also inform the protocol so accept() can read the listener_id
        *self.protocol.shared_listener_id.lock().unwrap() = Some(id);

        let iroh_addr = helper::iroh_node_id_to_multiaddr(&self.node_id);
        tracing::debug!(
            "Transport::listen_on - Sending NewAddress event: {}",
            iroh_addr
        );
        self.transport_events_tx
            .send(libp2p::core::transport::TransportEvent::NewAddress {
                listener_id: id,
                listen_addr: iroh_addr,
            })
            .map_err(|e| {
                tracing::error!(
                    "Transport::listen_on - Failed to send NewAddress event: {}",
                    e
                );
                libp2p::core::transport::TransportError::Other(TransportError {
                    kind: TransportErrorKind::Listen(format!(
                        "Failed to send NewAddress event: {e}"
                    )),
                })
            })
    }

    fn remove_listener(&mut self, id: libp2p::core::transport::ListenerId) -> bool {
        if let Some(current_id) = self.listener_id {
            if current_id == id {
                self.listener_id = None;
                self._router = None;
                *self.protocol.shared_listener_id.lock().unwrap() = None;
                return true;
            }
        }
        false
    }

    fn dial(
        &mut self,
        addr: libp2p::Multiaddr,
        _opts: libp2p::core::transport::DialOpts,
    ) -> Result<Self::Dial, libp2p::core::transport::TransportError<Self::Error>> {
        tracing::debug!("Transport::dial - Dialing address: {}", addr);
        let node_id = helper::multiaddr_to_iroh_node_id(&addr).ok_or_else(|| {
            tracing::error!(
                "Transport::dial - Failed to extract EndpointId from multiaddr: {}",
                addr
            );
            libp2p::core::transport::TransportError::Other(TransportError {
                kind: TransportErrorKind::Dial(
                    "Failed to extract iroh EndpointId from multiaddr".to_string(),
                ),
            })
        })?;
        tracing::debug!("Transport::dial - Extracted EndpointId: {:?}", node_id);
        let timeout = self.timeout;
        let endpoint = self.endpoint.clone();

        Ok(async move {
            tracing::debug!(
                "Transport::dial - Connecting to {:?} with ALPN {:?} (timeout: {:?})",
                node_id,
                std::str::from_utf8(Protocol::ALPN),
                timeout
            );
            let connecting = endpoint.connect(node_id, Protocol::ALPN);
            let conn = tokio::time::timeout(timeout, connecting)
                .await
                .map_err(|_| {
                    tracing::error!("Transport::dial - Connection timed out after {:?}", timeout);
                    TransportError {
                        kind: TransportErrorKind::Dial(format!(
                            "Connection timed out after {timeout:?}"
                        )),
                    }
                })?
                .map_err(|e| {
                    tracing::error!("Transport::dial - Connection failed: {}", e);
                    TransportError {
                        kind: TransportErrorKind::Dial(e.to_string()),
                    }
                })?;
            let remote_id = conn.remote_id();

            let peer_id = node_id_to_peerid(&remote_id).ok_or(TransportError {
                kind: TransportErrorKind::Dial(
                    "Failed to convert EndpointId to peerid".to_string(),
                ),
            })?;

            tracing::debug!("Transport::dial - Connection established to {:?}", peer_id);
            Ok((
                peer_id,
                libp2p::core::muxing::StreamMuxerBox::new(Connection::new(conn)),
            ))
        }
        .boxed())
    }

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<libp2p::core::transport::TransportEvent<Self::ListenerUpgrade, Self::Error>>
    {
        let this = self.get_mut();
        match this.transport_events_rx.poll_recv(cx) {
            std::task::Poll::Ready(Some(event)) => std::task::Poll::Ready(event),
            std::task::Poll::Ready(None) => std::task::Poll::Pending,
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl ProtocolHandler for Protocol {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        tracing::debug!("Protocol::accept - Accepting incoming connection");
        let remote_node_id = connection.remote_id();
        tracing::debug!("Protocol::accept - Remote node ID: {:?}", remote_node_id);

        // Check peer filter before accepting the connection
        if let Some(ref filter) = self.peer_filter {
            if !filter(&remote_node_id) {
                tracing::info!(
                    "Protocol::accept - Peer {:?} rejected by peer_filter",
                    remote_node_id
                );
                connection.close(From::from(1u32), b"rejected by peer filter");
                return Err(iroh::protocol::AcceptError::from_err(
                    TransportError::from("Peer rejected by filter"),
                ));
            }
        }

        let peer_id =
            node_id_to_peerid(&remote_node_id).ok_or(iroh::protocol::AcceptError::from_err(
                TransportError::from("Failed to convert EndpointId to PeerId"),
            ))?;

        let remote_multi = helper::iroh_node_id_to_multiaddr(&remote_node_id);
        let local_multi = helper::iroh_node_id_to_multiaddr(&self.local_node_id);

        tracing::debug!("Protocol::accept - Remote multiaddr: {}", remote_multi);
        tracing::debug!("Protocol::accept - Local multiaddr: {}", local_multi);

        let listener_id = self
            .shared_listener_id
            .lock()
            .unwrap()
            .ok_or_else(|| {
                tracing::error!("Protocol::accept - Listener ID not set");
                iroh::protocol::AcceptError::from_err(TransportError::from("Listener ID should be set"))
            })?;

        tracing::debug!("Protocol::accept - Listener ID: {:?}", listener_id);

        self.transport_tx
            .send(libp2p::core::transport::TransportEvent::Incoming {
                listener_id,
                upgrade: Connecting {
                    connecting: async move {
                        tracing::debug!("Protocol::accept - Connection upgrade resolving");
                        Ok((peer_id, connection))
                    }
                    .boxed(),
                },
                local_addr: local_multi,
                send_back_addr: remote_multi,
            })
            .map_err(|e| {
                tracing::error!("Protocol::accept - Failed to send Incoming event: {}", e);
                iroh::protocol::AcceptError::from_err(TransportError::from(
                    e.to_string().as_str(),
                ))
            })
    }
}
