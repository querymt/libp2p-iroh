mod connection;
mod helper;
mod stream;
mod transport;

pub use connection::{Connecting, Connection, ConnectionError, ConnectionErrorKind};
pub use helper::*;
pub use stream::{Stream, StreamError, StreamErrorKind};
pub use transport::{PeerFilter, Transport, TransportConfig, TransportError, TransportErrorKind};

pub use libp2p::Transport as TransportTrait;

/// Re-export iroh types needed by consumers for peer filtering and identity.
pub mod iroh_types {
    pub use iroh::EndpointId;
}
