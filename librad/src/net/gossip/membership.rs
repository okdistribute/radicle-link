// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{collections::BTreeMap, fmt::Debug, iter, time::Duration};

use nonempty::NonEmpty;
use rand::seq::IteratorRandom as _;
use thiserror::Error;

use super::{rpc, PartialPeerInfo, PeerAdvertisement, PeerInfo};
use crate::PeerId;

#[derive(Debug, Clone)]
pub struct MembershipParams {
    /// Maximum number of active connections.
    pub max_active: usize,
    /// Maximum number of passive connections.
    pub max_passive: usize,
    /// The number of hops a `ForwardJoin` or `Shuffle` should be propagated.
    pub active_random_walk_length: usize,
    /// The number of hops after which a `ForwardJoin` causes the sender to be
    /// inserted into the passive view.
    pub passive_random_walk_length: usize,
    /// The maximum number of peers to include in a shuffle.
    pub shuffle_sample_size: usize,
    /// Interval in which to perform a shuffle.
    pub shuffle_interval: Duration,
    /// Interval in which to attempt to promote a passive peer.
    pub promote_interval: Duration,
}

impl Default for MembershipParams {
    fn default() -> Self {
        Self {
            max_active: 5,
            max_passive: 30,
            active_random_walk_length: 6,
            passive_random_walk_length: 3,
            shuffle_sample_size: 7,
            shuffle_interval: Duration::from_secs(30),
            promote_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub enum Tick<StreamId, Addr>
where
    Addr: Clone + Ord,
{
    Broadcast {
        recipients: Vec<(PeerId, StreamId)>,
        message: rpc::Membership<Addr>,
    },

    Reply {
        to: PeerInfo<Addr>,
        message: rpc::Membership<Addr>,
    },

    Connect {
        to: PeerInfo<Addr>,
    },

    Demote {
        peer: PeerId,
        stream: StreamId,
    },

    Forget {
        peer: PeerId,
    },

    Ticks {
        ticks: Vec<Tick<StreamId, Addr>>,
    },
}

#[derive(Debug)]
pub struct Shuffle<StreamId, Addr>
where
    Addr: Clone + Ord,
{
    pub recipient: (PeerId, StreamId),
    pub sample: Vec<PeerInfo<Addr>>,
    pub ttl: usize,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("already connected peer sent join")]
    JoinWhileConnected,
}

#[derive(Debug)]
struct Active<StreamId, Addr>
where
    Addr: Clone + Ord,
{
    stream_id: StreamId,
    info: PartialPeerInfo<Addr>,
}

/// The classic [HyParView] membership protocol.
///
/// [HyParView]: https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf
pub struct Hpv<Rng, StreamId, Addr>
where
    Addr: Clone + Ord,
{
    local_id: PeerId,
    params: MembershipParams,
    rng: Rng,
    active: BTreeMap<PeerId, Active<StreamId, Addr>>,
    passive: BTreeMap<PeerId, PeerInfo<Addr>>,
}

impl<Rng, StreamId, Addr> Hpv<Rng, StreamId, Addr>
where
    Rng: rand::Rng,
    StreamId: Clone + Debug + PartialEq,
    Addr: Clone + Debug + Ord,
{
    pub fn new(local_id: PeerId, rng: Rng) -> Self {
        Self {
            local_id,
            params: Default::default(),
            rng,
            active: Default::default(),
            passive: Default::default(),
        }
    }

    pub fn with_params(self, params: MembershipParams) -> Self {
        Self { params, ..self }
    }

    pub fn num_active(&self) -> usize {
        self.active.len()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn connection_lost(
        &mut self,
        remote_peer: PeerId,
        stream_id: StreamId,
    ) -> Option<Tick<StreamId, Addr>> {
        use std::collections::btree_map::Entry::*;
        use Tick::*;

        tracing::warn!("connection lost");
        match self.active.entry(remote_peer) {
            Occupied(entry) if entry.get().stream_id == stream_id => Some(entry.remove()),
            _ => None,
        }
        .and_then(|Active { info, .. }| {
            let mut ticks = Vec::with_capacity(2);
            for ejected in info
                .sequence()
                .into_iter()
                .filter_map(|info| self.add_passive(info))
            {
                ticks.push(Forget {
                    peer: ejected.peer_id,
                });
            }

            // Try to fill active from passive
            // NB: may choose `remote_peer` again
            if let Some(candidates) = self.choose_passive_to_promote() {
                for to in candidates {
                    ticks.push(Connect { to });
                }
            }

            (!ticks.is_empty()).then_some(Ticks { ticks })
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn connection_established(
        &mut self,
        info: PartialPeerInfo<Addr>,
        stream_id: StreamId,
    ) -> Option<Tick<StreamId, Addr>> {
        tracing::info!("connection established");
        self.promote(info, stream_id)
    }

    pub fn shuffle(&mut self) -> Option<Shuffle<StreamId, Addr>> {
        self.random_active().and_then(|(peer_id, stream_id)| {
            let sample = self
                .sample()
                .filter(|info| info.peer_id != peer_id)
                .collect::<Vec<_>>();
            if sample.is_empty() {
                None
            } else {
                Some(Shuffle {
                    recipient: (peer_id, stream_id),
                    sample,
                    ttl: self.params.active_random_walk_length,
                })
            }
        })
    }

    pub fn choose_passive_to_promote(&mut self) -> Option<NonEmpty<PeerInfo<Addr>>> {
        NonEmpty::from_vec(
            self.passive
                .values()
                .choose_multiple(&mut self.rng, self.params.max_active - self.active.len())
                .into_iter()
                .cloned()
                .collect(),
        )
    }

    pub fn broadcast_recipients(
        &self,
        exclude: Option<PeerId>,
    ) -> impl Iterator<Item = (PeerId, StreamId)> + '_ {
        self.active
            .iter()
            .filter_map(
                move |(peer_id, Active { stream_id, .. })| match exclude.as_ref() {
                    Some(ex) if ex == peer_id => None,
                    None | Some(_) => Some((*peer_id, stream_id.clone())),
                },
            )
    }

    #[tracing::instrument(skip(self))]
    pub fn apply(
        &mut self,
        remote_peer: PeerId,
        remote_addr: Addr,
        stream_id: StreamId,
        rpc: rpc::Membership<Addr>,
    ) -> Result<Option<Tick<StreamId, Addr>>, Error> {
        use rpc::Membership::*;
        use Tick::*;

        tracing::debug!(
            active = self.active.len(),
            passive = self.passive.len(),
            "enter"
        );

        let res = match rpc {
            Join(_) if self.is_active(&remote_peer) => Err(Error::JoinWhileConnected),
            Join(ad) => {
                let info = peer_info_from(remote_peer, ad, remote_addr);

                let mut ticks = Vec::new();
                if let Some(tick) = self.promote(info.clone(), stream_id) {
                    ticks.push(tick);
                }

                let fwd = self
                    .broadcast_recipients(Some(remote_peer))
                    .collect::<Vec<_>>();
                if !fwd.is_empty() {
                    ticks.push(Broadcast {
                        recipients: fwd,
                        message: ForwardJoin {
                            joined: info,
                            ttl: self.params.active_random_walk_length,
                        },
                    })
                }

                Ok((!ticks.is_empty()).then_some(Ticks { ticks }))
            },

            ForwardJoin { joined, ttl }
                if (ttl == 0 || !self.is_active_full())
                    && !self.active.contains_key(&joined.peer_id) =>
            {
                Ok(Some(Connect { to: joined }))
            },
            ForwardJoin { joined, ttl } => {
                let mut ticks = Vec::with_capacity(2);

                if ttl == self.params.passive_random_walk_length {
                    if let Some(ejected) = self.add_passive(joined.clone()) {
                        ticks.push(Forget {
                            peer: ejected.peer_id,
                        })
                    }
                }

                let recipients = self
                    .broadcast_recipients(Some(remote_peer))
                    .collect::<Vec<_>>();
                if !recipients.is_empty() {
                    ticks.push(Broadcast {
                        recipients,
                        message: ForwardJoin {
                            joined,
                            ttl: ttl.saturating_sub(1),
                        },
                    })
                }

                Ok((!ticks.is_empty()).then_some(Ticks { ticks }))
            },

            Neighbour { info, need_friends } => {
                let info = peer_info_from(remote_peer, info, remote_addr);

                if need_friends.is_some() || !self.is_active_full() {
                    Ok(self.promote(info, stream_id))
                } else {
                    Ok(Some(Demote {
                        peer: info.peer_id,
                        stream: stream_id,
                    }))
                }
            },

            Shuffle { origin, peers, ttl } if ttl == 0 => {
                let mut ticks = Vec::with_capacity(1 + peers.len());
                if let Some(sample) = NonEmpty::from_vec(self.sample().collect()) {
                    ticks.push(Reply {
                        to: origin,
                        message: ShuffleReply {
                            peers: sample.into_iter().collect(),
                        },
                    });
                }
                for ejected in peers.into_iter().filter_map(|info| self.add_passive(info)) {
                    ticks.push(Forget {
                        peer: ejected.peer_id,
                    })
                }

                Ok((!ticks.is_empty()).then_some(Ticks { ticks }))
            },
            Shuffle {
                mut origin,
                peers,
                ttl,
            } => {
                let sender_is_origin = origin.peer_id == remote_peer;
                if sender_is_origin {
                    origin.seen_addrs.insert(remote_addr);
                }

                Ok(NonEmpty::from_vec(
                    self.broadcast_recipients(sender_is_origin.then_some(remote_peer))
                        .collect(),
                )
                .map(|recipients| Broadcast {
                    recipients: recipients.into_iter().collect(),
                    message: Shuffle {
                        origin,
                        peers,
                        ttl: ttl.saturating_sub(1),
                    },
                }))
            },

            ShuffleReply { peers } => {
                let ticks = peers
                    .into_iter()
                    .filter_map(|info| self.add_passive(info))
                    .map(|ejected| Forget {
                        peer: ejected.peer_id,
                    })
                    .collect::<Vec<_>>();

                Ok((!ticks.is_empty()).then_some(Ticks { ticks }))
            },
        };

        tracing::debug!(
            active = self.active.len(),
            passive = self.passive.len(),
            "exit"
        );
        tracing::trace!("out ticks: {:?}", res);

        res
    }

    fn is_active(&self, peer: &PeerId) -> bool {
        self.active.contains_key(peer)
    }

    fn is_active_full(&self) -> bool {
        self.active.len() >= self.params.max_active
    }

    fn is_passive_full(&self) -> bool {
        self.passive.len() >= self.params.max_passive
    }

    fn random_active(&mut self) -> Option<(PeerId, StreamId)> {
        self.active
            .values()
            .choose(&mut self.rng)
            .map(|Active { stream_id, info }| (info.peer_id, stream_id.clone()))
    }

    fn sample(&mut self) -> impl Iterator<Item = PeerInfo<Addr>> + '_ {
        let mut sample = self
            .active
            .values()
            .filter_map(|Active { info, .. }| info.clone().sequence())
            .choose_multiple(&mut self.rng, self.params.shuffle_sample_size);
        if sample.len() < self.params.shuffle_sample_size {
            sample.extend(
                self.passive
                    .values()
                    .choose_multiple(
                        &mut self.rng,
                        self.params.shuffle_sample_size - sample.len(),
                    )
                    .into_iter()
                    .cloned(),
            );
        }

        sample.into_iter()
    }

    fn promote<I>(&mut self, info: I, stream_id: StreamId) -> Option<Tick<StreamId, Addr>>
    where
        I: Into<PartialPeerInfo<Addr>>,
    {
        use Tick::*;

        let info = info.into();
        self.add_active(Active { stream_id, info })
            .map(|Active { stream_id, info }| {
                let ejected_active = info.peer_id;
                let ejected_passive = info
                    .sequence()
                    .into_iter()
                    .filter_map(|info| self.add_passive(info));

                Ticks {
                    ticks: iter::once(Demote {
                        peer: ejected_active,
                        stream: stream_id,
                    })
                    .chain(ejected_passive.map(|info| Forget { peer: info.peer_id }))
                    .collect(),
                }
            })
    }

    fn add_active(&mut self, active: Active<StreamId, Addr>) -> Option<Active<StreamId, Addr>> {
        if active.info.peer_id == self.local_id {
            None
        } else {
            let eject = if self.is_active_full() {
                self.active.keys().choose(&mut self.rng).copied()
            } else {
                None
            };

            let ejected = eject.and_then(|peer| self.active.remove(&peer));
            self.passive.remove(&active.info.peer_id);
            self.active.insert(active.info.peer_id, active);
            ejected
        }
    }

    fn add_passive(&mut self, info: PeerInfo<Addr>) -> Option<PeerInfo<Addr>> {
        // NB: not checking if already in passive set -- we assume `info` is
        // more accurate, so allow to overwrite it.
        if info.peer_id == self.local_id || self.is_active(&info.peer_id) {
            return None;
        }

        let eject = if self.is_passive_full() {
            // TODO: passive should be a priority map
            self.passive.keys().choose(&mut self.rng).copied()
        } else {
            None
        };

        let ejected = eject.and_then(|peer| self.passive.remove(&peer));
        self.passive.insert(info.peer_id, info);
        ejected
    }
}

fn peer_info_from<Addr>(
    remote_peer: PeerId,
    advertised: PeerAdvertisement<Addr>,
    remote_addr: Addr,
) -> PeerInfo<Addr>
where
    Addr: Clone + Ord,
{
    PeerInfo {
        peer_id: remote_peer,
        advertised_info: advertised,
        seen_addrs: [remote_addr].iter().cloned().collect(),
    }
}
