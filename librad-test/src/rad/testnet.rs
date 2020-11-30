// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{
    future::Future,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ops::Deref,
};

use futures::{
    future,
    stream::{self, StreamExt},
};
use tempfile::{tempdir, TempDir};

use librad::{
    git,
    keys::SecretKey,
    net::{
        discovery,
        peer::{Gossip, Peer, PeerApi, PeerConfig},
        protocol::ProtocolEvent,
    },
    paths::Paths,
    peer::PeerId,
};

lazy_static! {
    static ref LOCALHOST_ANY: SocketAddr =
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0));
}

pub struct TestPeer {
    pub _tmp: TempDir,
    pub peer: Peer<SecretKey>,
    pub key: SecretKey,
}

impl Deref for TestPeer {
    type Target = Peer<SecretKey>;

    fn deref(&self) -> &Self::Target {
        &self.peer
    }
}

impl AsRef<Peer<SecretKey>> for TestPeer {
    fn as_ref(&self) -> &Peer<SecretKey> {
        self
    }
}

pub async fn boot(seeds: Vec<(PeerId, SocketAddr)>) -> anyhow::Result<TestPeer> {
    let tmp = tempdir()?;
    let paths = Paths::from_root(tmp.path())?;
    let key = SecretKey::new();
    let listen_addr = *LOCALHOST_ANY;
    let gossip_params = Default::default();
    let disco = discovery::Static::new(seeds);
    let storage_config = Default::default();

    git::storage::Storage::init(&paths, key)?;

    let config = PeerConfig {
        signer: key,
        paths,
        listen_addr,
        gossip_params,
        disco,
        storage_config,
    };

    config
        .try_into_peer()
        .await
        .map(|peer| TestPeer {
            _tmp: tmp,
            peer,
            key,
        })
        .map_err(|e| e.into())
}

/// Setup a testnet with the given number of peers.
/// Peer X+1 has peer X as a seed peer.
pub async fn setup(num_peers: usize) -> anyhow::Result<Vec<TestPeer>> {
    if num_peers < 1 {
        return Ok(vec![]);
    }

    let mut peers = Vec::with_capacity(num_peers);
    let mut seed_addrs = None;
    for _ in 0..num_peers {
        let peer = boot(seed_addrs.take().into_iter().collect()).await?;
        seed_addrs = Some((peer.peer_id(), peer.listen_addr()));
        peers.push(peer)
    }

    Ok(peers)
}

pub async fn setup_disconnected(num_peers: usize) -> anyhow::Result<Vec<TestPeer>> {
    if num_peers < 1 {
        return Ok(vec![]);
    }

    let mut peers = Vec::with_capacity(num_peers);
    for _ in 0..num_peers {
        let peer = boot(vec![]).await?;
        peers.push(peer)
    }

    Ok(peers)
}

pub async fn run_on_testnet<F, Fut, A>(peers: Vec<TestPeer>, min_connected: usize, mut f: F) -> A
where
    F: FnMut(Vec<(PeerApi<SecretKey>, SecretKey)>) -> Fut,
    Fut: Future<Output = A>,
{
    let num_peers = peers.len();

    // move out tempdirs, so they don't get dropped
    let (_tmps, peers_and_keys) = peers
        .into_iter()
        .map(|TestPeer { _tmp, peer, key }| (_tmp, (peer, key)))
        .unzip::<_, _, Vec<_>, Vec<_>>();

    // unzip2, anyone?
    let (peers, keys) = peers_and_keys.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();

    let (apis, runners) = peers
        .into_iter()
        .map(|peer| peer.accept().unwrap())
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let events = {
        let mut events = Vec::with_capacity(num_peers);
        for api in &apis {
            events.push(api.protocol().subscribe().await);
        }
        events
    };
    let converged = wait_converged(events, min_connected);

    let (abort_handle, abort_reg) = future::AbortHandle::new_pair();
    tokio::task::spawn(future::Abortable::new(
        future::select_all(runners),
        abort_reg,
    ));
    converged.await;

    let res = f(apis.into_iter().zip(keys).collect()).await;
    abort_handle.abort();

    res
}

pub async fn wait_converged<S>(events: Vec<S>, min_connected: usize)
where
    S: futures::Stream<Item = ProtocolEvent<Gossip>> + Unpin,
{
    if min_connected < 2 {
        return;
    }

    stream::select_all(events)
        .scan(0, |connected, event| {
            if let ProtocolEvent::Connected(_) = event {
                *connected += 1;
            }

            future::ready(if *connected < min_connected {
                Some(event)
            } else {
                None
            })
        })
        .collect::<Vec<_>>()
        .await;
}
