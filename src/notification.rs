use crate::member::Member;
use failure::_core::cmp::Ordering;

#[derive(Debug)]
pub(crate) enum Notification {
    Alive { member: Member },
    Suspect { member: Member },
    Confirm { member: Member },
}

impl From<&[u8]> for Notification {
    fn from(buffer: &[u8]) -> Self {
        let member = Member::from(&buffer[1..]);
        match buffer[0] {
            x if x == NotificationType::Alive as u8 => Notification::Alive { member },
            x if x == NotificationType::Suspect as u8 => Notification::Suspect { member },
            x if x == NotificationType::Confirm as u8 => Notification::Confirm { member },
            _ => panic!("No such message type"),
        }
    }
}

impl PartialEq for Notification {
    fn eq(&self, other: &Self) -> bool {
        self.member() == other.member()
    }
}

impl PartialOrd for Notification {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.member().address != other.member().address {
            return None;
        }
        match self {
            Notification::Alive { member } => match other {
                Notification::Alive { member: other_member } | Notification::Suspect { member: other_member } => {
                    member.incarnation.partial_cmp(&other_member.incarnation)
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
            Notification::Confirm { member } => match other {
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

#[repr(u8)]
enum NotificationType {
    Alive = 1,
    Suspect,
    Confirm,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::SocketAddr;
    use std::str::FromStr;

    #[test]
    fn test_comparison() {
        let alive = Notification::Alive {
            member: Member {
                address: SocketAddr::from_str("127.0.0.1:1234").unwrap(),
                incarnation: 1,
            },
        };
        let confirm = Notification::Confirm {
            member: Member {
                address: SocketAddr::from_str("127.0.0.1:1234").unwrap(),
                incarnation: 1,
            },
        };
        assert!(alive < confirm);
    }
}
