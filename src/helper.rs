use iroh::EndpointId;
use libp2p::Multiaddr;

pub(crate) fn multiaddr_to_iroh_node_id(addr: &Multiaddr) -> Option<EndpointId> {
    tracing::debug!(
        "helper::multiaddr_to_iroh_node_id - Converting multiaddr: {}",
        addr
    );
    // Try to extract node_id from /p2p/ protocol component
    for protocol in addr.iter() {
        if let libp2p::multiaddr::Protocol::P2p(peer_id) = protocol {
            tracing::debug!(
                "helper::multiaddr_to_iroh_node_id - Found P2p protocol with peer_id: {}",
                peer_id
            );
            if let Some(node_id) = peer_id_to_node_id(&peer_id) {
                tracing::debug!(
                    "helper::multiaddr_to_iroh_node_id - Converted to EndpointId: {:?}",
                    node_id
                );
                return Some(node_id);
            } else {
                tracing::warn!(
                    "helper::multiaddr_to_iroh_node_id - Failed to convert PeerId to EndpointId"
                );
            }
        }
    }

    tracing::warn!("helper::multiaddr_to_iroh_node_id - No valid P2p protocol found in multiaddr");
    None
}

pub fn peer_id_to_node_id(peer_id: &libp2p::PeerId) -> Option<EndpointId> {
    tracing::debug!(
        "helper::peer_id_to_node_id - Converting PeerId: {}",
        peer_id
    );
    let bytes = peer_id.to_bytes();
    tracing::debug!(
        "helper::peer_id_to_node_id - PeerId bytes length: {}",
        bytes.len()
    );
    if bytes.len() != 38 {
        tracing::warn!(
            "helper::peer_id_to_node_id - Invalid byte length: expected 38, got {}",
            bytes.len()
        );
        return None;
    }
    if let Ok(byte_array) = <[u8; 32]>::try_from(&bytes[6..]) {
        if let Ok(node_id) = EndpointId::from_bytes(&byte_array) {
            tracing::debug!(
                "helper::peer_id_to_node_id - Successfully converted to EndpointId: {:?}",
                node_id
            );
            return Some(node_id);
        } else {
            tracing::warn!("helper::peer_id_to_node_id - Failed to create EndpointId from bytes");
        }
    } else {
        tracing::warn!(
            "helper::peer_id_to_node_id - Failed to extract 32-byte array from PeerId bytes"
        );
    }
    None
}

pub(crate) fn libp2p_keypair_to_iroh_secret(
    keypair: &libp2p::identity::Keypair,
) -> Option<iroh::SecretKey> {
    if let Ok(ed25519) = keypair.clone().try_into_ed25519() {
        let secret = ed25519.secret();
        let secret_key = iroh::SecretKey::from_bytes(secret.as_ref().try_into().ok()?);
        return Some(secret_key);
    }
    None
}

pub fn iroh_node_id_to_multiaddr(node_id: &EndpointId) -> Multiaddr {
    tracing::debug!(
        "helper::iroh_node_id_to_multiaddr - Converting EndpointId: {:?}",
        node_id
    );
    let mut addr = Multiaddr::empty();
    addr.push(libp2p::multiaddr::Protocol::P2p(
        libp2p::identity::ed25519::PublicKey::try_from_bytes(node_id.as_bytes())
            .map(|pk| {
                let peer_id =
                    libp2p::PeerId::from_public_key(&libp2p::identity::PublicKey::from(pk));
                tracing::debug!(
                    "helper::iroh_node_id_to_multiaddr - Converted to PeerId: {}",
                    peer_id
                );
                peer_id
            })
            .expect("Failed to convert iroh EndpointId to libp2p PeerId"),
    ));

    tracing::debug!(
        "helper::iroh_node_id_to_multiaddr - Created multiaddr: {}",
        addr
    );
    addr
}

pub fn node_id_to_peerid(node_id: &EndpointId) -> Option<libp2p::PeerId> {
    let pubkey_bytes = node_id.to_vec();
    let libp2p_pubkey =
        libp2p::identity::ed25519::PublicKey::try_from_bytes(pubkey_bytes.as_slice()).ok()?;

    Some(libp2p::PeerId::from_public_key(
        &libp2p::identity::PublicKey::from(libp2p_pubkey),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keypair_to_iroh_secret_roundtrip() {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let secret = libp2p_keypair_to_iroh_secret(&keypair).expect("conversion should succeed");
        let node_id = secret.public();

        // Convert the original keypair's public key to the same EndpointId via PeerId roundtrip
        let peer_id = libp2p::PeerId::from(keypair.public());
        let node_id_from_peer = peer_id_to_node_id(&peer_id).expect("peer_id -> node_id");

        assert_eq!(node_id, node_id_from_peer);
    }

    #[test]
    fn peer_id_node_id_roundtrip() {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let peer_id = libp2p::PeerId::from(keypair.public());

        let node_id = peer_id_to_node_id(&peer_id).expect("peer_id -> node_id");
        let peer_id_back = node_id_to_peerid(&node_id).expect("node_id -> peer_id");

        assert_eq!(peer_id, peer_id_back);
    }

    #[test]
    fn multiaddr_node_id_roundtrip() {
        let secret = iroh::SecretKey::generate();
        let node_id = secret.public();

        let multiaddr = iroh_node_id_to_multiaddr(&node_id);
        let node_id_back = multiaddr_to_iroh_node_id(&multiaddr).expect("multiaddr -> node_id");

        assert_eq!(node_id, node_id_back);
    }

    #[test]
    fn multiaddr_with_prefix_extracts_node_id() {
        // Kademlia may store addresses with IP/port prefixes. The transport
        // should still extract the /p2p/ component.
        let secret = iroh::SecretKey::generate();
        let node_id = secret.public();
        let peer_id = node_id_to_peerid(&node_id).expect("node_id -> peer_id");

        let addr: Multiaddr = format!("/ip4/0.0.0.0/udp/0/p2p/{peer_id}")
            .parse()
            .expect("valid multiaddr");

        let extracted = multiaddr_to_iroh_node_id(&addr).expect("should extract node_id");
        assert_eq!(node_id, extracted);
    }

    #[test]
    fn multiaddr_without_p2p_returns_none() {
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/8080".parse().unwrap();
        assert!(multiaddr_to_iroh_node_id(&addr).is_none());
    }

    #[test]
    fn empty_multiaddr_returns_none() {
        let addr = Multiaddr::empty();
        assert!(multiaddr_to_iroh_node_id(&addr).is_none());
    }

    #[test]
    fn iroh_secret_preserves_key_material() {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let ed25519 = keypair.clone().try_into_ed25519().unwrap();
        let original_secret_bytes = ed25519.secret().as_ref().to_vec();

        let iroh_secret = libp2p_keypair_to_iroh_secret(&keypair).unwrap();
        let iroh_secret_bytes = iroh_secret.to_bytes();

        assert_eq!(original_secret_bytes, iroh_secret_bytes.as_slice());
    }
}
