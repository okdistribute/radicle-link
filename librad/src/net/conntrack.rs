// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use futures::lock::Mutex as AsyncMutex;

use crate::{
    net::{
        connection::{HasStableId, RemotePeer},
        quic,
    },
    peer::PeerId,
};

pub type Connections = Conntrack<quic::Connection>;
pub type Streams<S> = Conntrack<SyncStream<S>>;

pub struct Conntrack<T>(BTreeMap<PeerId, T>);

impl<T> Default for Conntrack<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T> Conntrack<T>
where
    T: HasStableId + RemotePeer,
{
    pub fn get(&self, peer: &PeerId) -> Option<&T> {
        self.0.get(peer)
    }

    pub fn get_id(&self, peer: &PeerId, id: &T::Id) -> Option<&T> {
        match self.0.get(peer) {
            Some(found) if &found.stable_id() == id => Some(found),
            _ => None,
        }
    }

    pub fn has_connection(&self, to: &PeerId) -> bool {
        self.0.get(to).and(Some(true)).unwrap_or(false)
    }

    pub fn insert(&mut self, conn: T) -> Option<T> {
        let peer = conn.remote_peer_id();
        self.0.insert(peer, conn)
    }

    pub fn remove(&mut self, conn: &T) -> bool {
        self.remove_id(&conn.remote_peer_id(), &conn.stable_id())
            .is_some()
    }

    pub fn remove_id(&mut self, peer: &PeerId, id: &T::Id) -> Option<T> {
        match self.0.get(&peer) {
            Some(found) if &found.stable_id() == id => self.0.remove(&peer),
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&PeerId, &T)> {
        self.0.iter()
    }
}

pub struct SyncStream<S>
where
    S: HasStableId,
{
    id: S::Id,
    peer: PeerId,
    inner: Arc<AsyncMutex<S>>,
}

impl<S> From<S> for SyncStream<S>
where
    S: HasStableId + RemotePeer,
{
    fn from(s: S) -> Self {
        Self {
            id: s.stable_id(),
            peer: s.remote_peer_id(),
            inner: Arc::new(AsyncMutex::new(s)),
        }
    }
}

impl<S> Clone for SyncStream<S>
where
    S: HasStableId,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            peer: self.peer,
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> Deref for SyncStream<S>
where
    S: HasStableId,
{
    type Target = AsyncMutex<S>;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<S> HasStableId for SyncStream<S>
where
    S: HasStableId,
{
    type Id = S::Id;

    fn stable_id(&self) -> Self::Id {
        self.id
    }
}

impl<S> RemotePeer for SyncStream<S>
where
    S: HasStableId + RemotePeer,
{
    fn remote_peer_id(&self) -> PeerId {
        self.peer
    }
}
