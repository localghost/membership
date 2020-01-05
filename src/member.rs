use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sha1::{Digest, Sha1};
//use std::cmp::Ordering;
use failure::_core::array::TryFromSliceError;
use failure::_core::fmt::{Error, Formatter};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

type Incarnation = u64;

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) struct MemberId([u8; 20]);

impl MemberId {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.0
    }

    fn get_id_str(&self) -> String {
        self.0
            .iter()
            .map(|b| format!("{:x}", b))
            .fold(String::new(), |acc, val| acc + &val)
    }
}

impl std::fmt::Display for MemberId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.get_id_str())
    }
}

impl std::fmt::Debug for MemberId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.get_id_str())
    }
}

impl TryFrom<&[u8]> for MemberId {
    type Error = TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match <[u8; 20]>::try_from(value) {
            Ok(array) => Ok(MemberId(array)),
            Err(e) => Err(e),
        }
    }
}

impl Hash for MemberId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Member {
    pub(crate) id: MemberId,
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

fn generate_id(address: SocketAddr) -> MemberId {
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
    MemberId(hasher.result().into())
}
