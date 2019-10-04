use std::cmp::Ordering;
use std::net::SocketAddr;

type Incarnation = u64;

#[derive(Debug)]
pub(crate) struct Member {
    address: SocketAddr,
    incarnation: Incarnation,
}

impl PartialOrd for Member {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.address != other.address {
            return None;
        }
        self.incarnation.partial_cmp(&other.incarnation)
    }
}

impl PartialEq for Member {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.incarnation == other.incarnation
    }
}
