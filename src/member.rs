use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sha1::{Digest, Sha1};
//use std::cmp::Ordering;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

type Incarnation = u64;
pub(crate) type Id = [u8; 20];

#[derive(Debug, Clone)]
pub(crate) struct Member {
    pub(crate) id: Id,
    pub(crate) address: SocketAddr,
    pub(crate) incarnation: Incarnation,
}

impl Member {
    pub(crate) fn new(address: SocketAddr) -> Self {
        Member {
            id: generate_id(address),
            address,
            incarnation: 0,
        }
    }
}

//impl PartialOrd for Member {
//    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//        if self.id != other.id {
//            return None;
//        }
//        self.incarnation.partial_cmp(&other.incarnation)
//    }
//}
//
impl PartialEq for Member {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.incarnation == other.incarnation
    }
}

fn generate_id(address: SocketAddr) -> Id {
    let mut hasher = Sha1::new();
    match address {
        SocketAddr::V4(v4) => {
            hasher.input(v4.ip().octets());
            hasher.input(v4.port().to_be_bytes());
        }
        SocketAddr::V6(v6) => {
            hasher.input(v6.ip().octets());
            hasher.input(v6.port().to_be_bytes())
        }
    };
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => hasher.input(duration.as_millis().to_be_bytes()),
        Err(e) => hasher.input(SmallRng::from_entropy().gen::<u64>().to_be_bytes()),
    }
    hasher.result().into()
}
