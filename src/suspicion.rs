use crate::member::Member;
use std::time::Instant;

pub(crate) struct Suspicion {
    pub(crate) created: Instant,
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
