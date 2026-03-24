//! Integration test: Verify that the peer_filter callback rejects connections.

use futures::StreamExt;
use libp2p::kad::store::MemoryStore;
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{Multiaddr, StreamProtocol};
use std::sync::Arc;
use std::time::Duration;

use libp2p_iroh::{Transport, TransportConfig, TransportTrait};

#[derive(NetworkBehaviour)]
struct TestBehaviour {
    kademlia: libp2p::kad::Behaviour<MemoryStore>,
}

fn make_swarm(keypair: &libp2p::identity::Keypair, transport: Transport) -> Swarm<TestBehaviour> {
    let peer_id = keypair.public().to_peer_id();
    let transport = transport.boxed();

    let kad_config = libp2p::kad::Config::new(StreamProtocol::new("/test/kad/1.0.0"));
    let store = MemoryStore::new(peer_id);
    let mut kademlia = libp2p::kad::Behaviour::with_config(peer_id, store, kad_config);
    kademlia.set_mode(Some(libp2p::kad::Mode::Server));

    let behaviour = TestBehaviour { kademlia };

    Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_executor(Box::new(|fut| {
            tokio::spawn(fut);
        }))
        .with_idle_connection_timeout(Duration::from_secs(60)),
    )
}

#[tokio::test]
async fn peer_filter_rejects_connection() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("libp2p_iroh=debug")
        .try_init();

    let kp_a = libp2p::identity::Keypair::generate_ed25519();
    let kp_b = libp2p::identity::Keypair::generate_ed25519();

    // Node A has a peer_filter that rejects all peers
    let config_a = TransportConfig {
        peer_filter: Some(Arc::new(|_node_id| {
            false // reject everyone
        })),
        ..Default::default()
    };
    let transport_a = Transport::with_config(Some(&kp_a), config_a).await.unwrap();
    let transport_b = Transport::new(Some(&kp_b)).await.unwrap();

    let peer_a = kp_a.public().to_peer_id();

    let mut swarm_a = make_swarm(&kp_a, transport_a);
    let mut swarm_b = make_swarm(&kp_b, transport_b);

    swarm_a.listen_on(Multiaddr::empty()).unwrap();
    swarm_b.listen_on(Multiaddr::empty()).unwrap();

    // Wait for DNS registration
    let wait_until = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(wait_until) => break,
            _ = swarm_a.select_next_some() => {}
            _ = swarm_b.select_next_some() => {}
        }
    }

    // B dials A -- should be rejected by A's peer_filter
    let addr_a: Multiaddr = format!("/p2p/{peer_a}").parse().unwrap();
    swarm_b.dial(addr_a).unwrap();

    // The peer_filter rejects at the protocol (ALPN) level, not the QUIC level.
    // So B may briefly establish a QUIC connection, but A's protocol handler
    // will reject it and close the connection. We verify that:
    // 1. A never fires ConnectionEstablished (the protocol layer rejects before that)
    // 2. B either gets OutgoingConnectionError or a brief connection that quickly closes
    let check_timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(check_timeout);

    let mut a_got_connection = false;

    loop {
        tokio::select! {
            _ = &mut check_timeout => {
                break;
            }
            event = swarm_a.select_next_some() => {
                if let SwarmEvent::ConnectionEstablished { .. } = event {
                    a_got_connection = true;
                }
            }
            _ = swarm_b.select_next_some() => {}
        }
    }

    assert!(
        !a_got_connection,
        "A should not have accepted any connections (peer_filter rejects all)"
    );
}

#[tokio::test]
async fn peer_filter_none_accepts_all() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("libp2p_iroh=debug")
        .try_init();

    let kp_a = libp2p::identity::Keypair::generate_ed25519();
    let kp_b = libp2p::identity::Keypair::generate_ed25519();

    // No peer_filter = accept all (default)
    let transport_a = Transport::new(Some(&kp_a)).await.unwrap();
    let transport_b = Transport::new(Some(&kp_b)).await.unwrap();

    let peer_a = kp_a.public().to_peer_id();

    let mut swarm_a = make_swarm(&kp_a, transport_a);
    let mut swarm_b = make_swarm(&kp_b, transport_b);

    swarm_a.listen_on(Multiaddr::empty()).unwrap();
    swarm_b.listen_on(Multiaddr::empty()).unwrap();

    // Wait for DNS registration
    let wait_until = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(wait_until) => break,
            _ = swarm_a.select_next_some() => {}
            _ = swarm_b.select_next_some() => {}
        }
    }

    // B dials A -- should succeed
    let addr_a: Multiaddr = format!("/p2p/{peer_a}").parse().unwrap();
    swarm_b.dial(addr_a).unwrap();

    let timeout = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                panic!("Timeout waiting for connection -- peer_filter=None should accept all");
            }
            event = swarm_a.select_next_some() => {
                if let SwarmEvent::ConnectionEstablished { .. } = event {
                    return; // success
                }
            }
            event = swarm_b.select_next_some() => {
                if let SwarmEvent::ConnectionEstablished { .. } = event {
                    return; // success
                }
            }
        }
    }
}
