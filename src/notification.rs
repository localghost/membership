use crate::member::Member;
use failure::_core::cmp::Ordering;

#[derive(Debug, Clone)]
pub(crate) enum Notification {
    Alive { member: Member },
    Suspect { member: Member },
    Confirm { member: Member },
}

impl PartialEq for Notification {
    fn eq(&self, other: &Self) -> bool {
        self.member() == other.member()
    }
}

impl PartialOrd for Notification {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.member().id != other.member().id {
            return None;
        }
        match self {
            Notification::Alive { member } => match other {
                Notification::Alive { member: other_member } => {
                    member.incarnation.partial_cmp(&other_member.incarnation)
                }
                Notification::Suspect { member: other_member } => {
                    if other_member.incarnation >= member.incarnation {
                        Some(Ordering::Less)
                    } else {
                        Some(Ordering::Greater)
                    }
                }
                Notification::Confirm { .. } => Some(Ordering::Less),
            },
            Notification::Suspect { member } => match other {
                Notification::Suspect { member: other_member } => {
                    member.incarnation.partial_cmp(&other_member.incarnation)
                }
                Notification::Alive { member: other_member } => {
                    if member.incarnation >= other_member.incarnation {
                        Some(Ordering::Greater)
                    } else {
                        Some(Ordering::Less)
                    }
                }
                Notification::Confirm { .. } => Some(Ordering::Less),
            },
            Notification::Confirm { .. } => match other {
                Notification::Alive { .. } | Notification::Suspect { .. } => Some(Ordering::Greater),
                Notification::Confirm { .. } => Some(Ordering::Equal),
            },
        }
    }
}
impl Notification {
    fn member(&self) -> &Member {
        match self {
            Notification::Alive { member } | Notification::Confirm { member } | Notification::Suspect { member } => {
                member
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::member::MemberId;
    use std::convert::TryFrom;
    use std::net::SocketAddr;
    use std::str::FromStr;

    #[test]
    fn test_comparison() {
        let alive = Notification::Alive {
            member: Member {
                address: SocketAddr::from_str("127.0.0.1:1234").unwrap(),
                incarnation: 1,
                id: MemberId::try_from([0u8; 20].as_ref()).unwrap(),
            },
        };
        let suspect = Notification::Suspect {
            member: Member {
                address: SocketAddr::from_str("127.0.0.1:1234").unwrap(),
                incarnation: 1,
                id: MemberId::try_from([0u8; 20].as_ref()).unwrap(),
            },
        };
        let confirm = Notification::Confirm {
            member: Member {
                address: SocketAddr::from_str("127.0.0.1:1234").unwrap(),
                incarnation: 1,
                id: MemberId::try_from([0u8; 20].as_ref()).unwrap(),
            },
        };
        assert!(alive < suspect);
        assert!(suspect < confirm);
        assert!(alive < confirm);
        assert!(suspect > alive);
    }
}
