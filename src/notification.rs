use crate::member::Member;

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

#[repr(u8)]
enum NotificationType {
    Alive = 1,
    Suspect,
    Confirm,
}
