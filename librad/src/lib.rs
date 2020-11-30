// Copyright © 2019-2020 The Radicle Foundation <hello@radicle.foundation>
//
// This file is part of radicle-link, distributed under the GPLv3 with Radicle
// Linking Exception. For full terms see the included LICENSE file.

#![warn(clippy::extra_unused_lifetimes)]
#![feature(backtrace)]
#![feature(bool_to_option)]
#![feature(btree_drain_filter)]
#![feature(core_intrinsics)]
#![feature(ip)]
#![feature(never_type)]
#![feature(try_trait)]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate radicle_macros;

pub extern crate radicle_git_ext as git_ext;
pub extern crate radicle_keystore as keystore;
pub extern crate radicle_std_ext as std_ext;

pub mod git;
pub mod hash;
pub mod identities;
pub mod internal;
pub mod keys;
pub mod meta;
pub mod net;
pub mod paths;
pub mod peer;
pub mod signer;
pub mod uri;

// Re-exports
pub use peer::PeerId;
pub use radicle_macros::*;

#[cfg(test)]
mod test;

#[cfg(test)]
#[macro_use]
extern crate futures_await_test;
#[cfg(test)]
#[macro_use]
extern crate assert_matches;
