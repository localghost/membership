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
    pub(crate) fn member(&self) -> &Member {
        match self {
            Notification::Alive { member } | Notification::Confirm { member } | Notification::Suspect { member } => {
                member
            }
        }
    }

    pub(crate) fn is_suspect(&self) -> bool {
        match *self {
            Notification::Suspect { .. } => true,
            _ => false,
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
    fn test_compare_same_member_with_same_incarnation() {
        let address = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let member_id = MemberId::try_from([0u8; 20].as_ref()).unwrap();

        let alive = Notification::Alive {
            member: Member {
                address,
                incarnation: 1,
                id: member_id,
            },
        };
        let suspect = Notification::Suspect {
            member: Member {
                address,
                incarnation: 1,
                id: member_id,
            },
        };
        let confirm = Notification::Confirm {
            member: Member {
                address,
                incarnation: 1,
                id: member_id,
            },
        };
        assert!(alive < suspect);
        assert!(suspect < confirm);
        assert!(alive < confirm);
        assert!(suspect > alive);
    }

    #[test]
    fn test_compare_same_member_with_different_incarnation() {
        let address = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let member_id = MemberId::try_from([0u8; 20].as_ref()).unwrap();

        let alive = Notification::Alive {
            member: Member {
                address,
                incarnation: 3,
                id: member_id,
            },
        };
        let suspect = Notification::Suspect {
            member: Member {
                address,
                incarnation: 2,
                id: member_id,
            },
        };
        let confirm = Notification::Confirm {
            member: Member {
                address,
                incarnation: 1,
                id: member_id,
            },
        };
        let suspect_with_higher_incarnation = Notification::Suspect {
            member: Member {
                address,
                incarnation: 3,
                id: member_id
            }
        };
        assert!(alive > suspect);
        assert!(suspect < confirm);
        assert!(alive < confirm);
        assert!(suspect < alive);
        assert!(suspect < suspect_with_higher_incarnation);
        assert!(alive < suspect_with_higher_incarnation);
    }

    #[test]
    fn test_compare_different_members() {
        let address = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let member_id1 = MemberId::try_from([1u8; 20].as_ref()).unwrap();
        let member_id2 = MemberId::try_from([2u8; 20].as_ref()).unwrap();
        let member_id3 = MemberId::try_from([3u8; 20].as_ref()).unwrap();

        let alive = Notification::Alive {
            member: Member {
                address,
                incarnation: 1,
                id: member_id1,
            },
        };
        let suspect = Notification::Suspect {
            member: Member {
                address,
                incarnation: 1,
                id: member_id2,
            },
        };
        let confirm = Notification::Confirm {
            member: Member {
                address,
                incarnation: 1,
                id: member_id3,
            },
        };
        assert_eq!(alive.partial_cmp(&suspect), None);
        assert_eq!(suspect.partial_cmp(&confirm), None);
        assert_eq!(alive.partial_cmp(&confirm), None);
    }
}
