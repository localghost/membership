#![deny(missing_docs)]

//! Implementation of SWIM protocol.
//!
//! Please refer to [SWIM paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) for detailed description.
//!
//! # Examples
//! ```
//! use membership::{Node, ProtocolConfig};
//! use std::net::SocketAddr;
//! use failure::_core::str::FromStr;
//! use failure::_core::time::Duration;
//!
//! let mut ms1 = Node::new(SocketAddr::from_str("127.0.0.1:2345").unwrap(), Default::default());
//! let mut ms2 = Node::new(SocketAddr::from_str("127.0.0.1:3456").unwrap(), Default::default());
//! ms1.join(SocketAddr::from_str("127.0.0.1:3456").unwrap()).unwrap();
//! ms2.join(SocketAddr::from_str("127.0.0.1:2345").unwrap()).unwrap();
//! std::thread::sleep(Duration::from_secs(ProtocolConfig::default().protocol_period * 2));
//! println!("{:?}", ms1.get_members().unwrap());
//! println!("{:?}", ms2.get_members().unwrap());
//! ms1.stop().unwrap();
//! ms2.stop().unwrap();
//! ```

pub use crate::node::Node;
pub use crate::protocol_config::ProtocolConfig;

/// Alias for backward compatibility. Please use [Node](struct.Node.html) instead.
#[deprecated(since = "0.0.6", note = "Please use `Node` instead.")]
pub type Membership = Node;

mod least_disseminated_members;
mod message;
mod node;
mod protocol_config;
mod result;
mod sync_node;
mod unique_circular_buffer;
