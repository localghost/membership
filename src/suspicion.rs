use crate::member::Member;
use std::time::Instant;

pub(crate) struct Suspicion {
    pub(crate) created: Instant,
    // FIXME: Actually, only member id and incarnation number are needed.
    pub(crate) member: Member,
}

impl Suspicion {
    pub(crate) fn new(member: Member) -> Self {
        Suspicion {
            created: Instant::now(),
            member,
        }
    }
}
