//! Integration test: Two nodes connect over iroh transport and perform
//! Kademlia DHT operations (put_record / get_record).
//!
//! This is the Phase 0 risk-reduction test from PLAN.md -- it verifies
//! that Kademlia works with `/p2p/`-only addresses over iroh transport.

use futures::StreamExt;
use libp2p::kad::store::MemoryStore;
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{Multiaddr, StreamProtocol};
use std::time::Duration;

use libp2p_iroh::{Transport, TransportTrait};

#[derive(NetworkBehaviour)]
struct TestBehaviour {
    kademlia: libp2p::kad::Behaviour<MemoryStore>,
}

fn make_swarm(keypair: &libp2p::identity::Keypair, transport: Transport) -> Swarm<TestBehaviour> {
    let peer_id = keypair.public().to_peer_id();
    let transport = transport.boxed();

    let mut kad_config = libp2p::kad::Config::new(StreamProtocol::new("/test/kad/1.0.0"));
    kad_config.set_query_timeout(Duration::from_secs(30));

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
async fn two_nodes_connect_and_establish_connection() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("libp2p_iroh=debug")
        .try_init();

    let kp_a = libp2p::identity::Keypair::generate_ed25519();
    let kp_b = libp2p::identity::Keypair::generate_ed25519();

    let transport_a = Transport::new(Some(&kp_a)).await.unwrap();
    let transport_b = Transport::new(Some(&kp_b)).await.unwrap();

    let peer_a = kp_a.public().to_peer_id();
    let peer_b = kp_b.public().to_peer_id();

    let mut swarm_a = make_swarm(&kp_a, transport_a);
    let mut swarm_b = make_swarm(&kp_b, transport_b);

    swarm_a.listen_on(Multiaddr::empty()).unwrap();
    swarm_b.listen_on(Multiaddr::empty()).unwrap();

    // Wait for endpoints to register with relay and publish addresses to DNS.
    // Poll swarms so internal events are processed while we wait.
    let wait_until = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(wait_until) => break,
            _ = swarm_a.select_next_some() => {}
            _ = swarm_b.select_next_some() => {}
        }
    }

    // B dials A
    let addr_a: Multiaddr = format!("/p2p/{peer_a}").parse().unwrap();
    swarm_b.dial(addr_a).unwrap();

    let timeout = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(timeout);

    let mut a_connected = false;
    let mut b_connected = false;

    loop {
        tokio::select! {
            _ = &mut timeout => {
                panic!("Timeout waiting for connection establishment");
            }
            event = swarm_a.select_next_some() => {
                if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                    assert_eq!(peer_id, peer_b);
                    a_connected = true;
                }
            }
            event = swarm_b.select_next_some() => {
                if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                    assert_eq!(peer_id, peer_a);
                    b_connected = true;
                }
            }
        }

        if a_connected && b_connected {
            break;
        }
    }
}

#[tokio::test]
async fn two_nodes_kademlia_put_get_record() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("libp2p_iroh=debug")
        .try_init();

    let kp_a = libp2p::identity::Keypair::generate_ed25519();
    let kp_b = libp2p::identity::Keypair::generate_ed25519();

    let transport_a = Transport::new(Some(&kp_a)).await.unwrap();
    let transport_b = Transport::new(Some(&kp_b)).await.unwrap();

    let peer_a = kp_a.public().to_peer_id();
    let _peer_b = kp_b.public().to_peer_id();

    let mut swarm_a = make_swarm(&kp_a, transport_a);
    let mut swarm_b = make_swarm(&kp_b, transport_b);

    swarm_a.listen_on(Multiaddr::empty()).unwrap();
    swarm_b.listen_on(Multiaddr::empty()).unwrap();

    // Wait for endpoints to register with relay and publish addresses to DNS.
    let wait_until = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(wait_until) => break,
            _ = swarm_a.select_next_some() => {}
            _ = swarm_b.select_next_some() => {}
        }
    }

    // B dials A
    let addr_a: Multiaddr = format!("/p2p/{peer_a}").parse().unwrap();
    swarm_b.dial(addr_a).unwrap();

    let timeout = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(timeout);

    // Phase 1: Wait for connection + routing table update on both sides
    let mut a_routing_updated = false;
    let mut b_routing_updated = false;

    loop {
        tokio::select! {
            _ = &mut timeout => {
                panic!("Timeout waiting for Kademlia routing update (a={a_routing_updated}, b={b_routing_updated})");
            }
            event = swarm_a.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        swarm_a.behaviour_mut().kademlia.add_address(
                            &peer_id,
                            endpoint.get_remote_address().clone(),
                        );
                    }
                    SwarmEvent::Behaviour(TestBehaviourEvent::Kademlia(
                        libp2p::kad::Event::RoutingUpdated { .. },
                    )) => {
                        a_routing_updated = true;
                    }
                    _ => {}
                }
            }
            event = swarm_b.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        swarm_b.behaviour_mut().kademlia.add_address(
                            &peer_id,
                            endpoint.get_remote_address().clone(),
                        );
                    }
                    SwarmEvent::Behaviour(TestBehaviourEvent::Kademlia(
                        libp2p::kad::Event::RoutingUpdated { .. },
                    )) => {
                        b_routing_updated = true;
                    }
                    _ => {}
                }
            }
        }
        if a_routing_updated && b_routing_updated {
            break;
        }
    }

    // Phase 2: A puts a record
    let record = libp2p::kad::Record::new(b"test-key".to_vec(), b"test-value".to_vec());
    swarm_a
        .behaviour_mut()
        .kademlia
        .put_record(record, libp2p::kad::Quorum::One)
        .unwrap();

    let mut put_done = false;
    let mut get_done = false;

    loop {
        tokio::select! {
            _ = &mut timeout => {
                if !put_done {
                    panic!("Timeout waiting for PUT record");
                } else {
                    panic!("Timeout waiting for GET record");
                }
            }
            event = swarm_a.select_next_some() => {
                if let SwarmEvent::Behaviour(TestBehaviourEvent::Kademlia(
                    libp2p::kad::Event::OutboundQueryProgressed { result, .. },
                )) = event
                {
                    match result {
                        libp2p::kad::QueryResult::PutRecord(Ok(_)) => {
                            put_done = true;
                            // Now B does a GET
                            swarm_b.behaviour_mut().kademlia.get_record(
                                libp2p::kad::RecordKey::new(&b"test-key".to_vec()),
                            );
                        }
                        libp2p::kad::QueryResult::PutRecord(Err(e)) => {
                            panic!("PUT failed: {e:?}");
                        }
                        _ => {}
                    }
                }
            }
            event = swarm_b.select_next_some() => {
                if let SwarmEvent::Behaviour(TestBehaviourEvent::Kademlia(
                    libp2p::kad::Event::OutboundQueryProgressed { result, .. },
                )) = event
                {
                    match result {
                        libp2p::kad::QueryResult::GetRecord(Ok(
                            libp2p::kad::GetRecordOk::FoundRecord(peer_record),
                        )) => {
                            let val = String::from_utf8_lossy(&peer_record.record.value);
                            assert_eq!(val, "test-value");
                            get_done = true;
                        }
                        libp2p::kad::QueryResult::GetRecord(Err(e)) => {
                            panic!("GET failed: {e:?}");
                        }
                        _ => {}
                    }
                }
            }
        }

        if get_done {
            break;
        }
    }
}
