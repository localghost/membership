use crate::member::Id as MemberId;
use std::time::Instant;

pub(crate) struct Suspicion {
    pub(crate) created: Instant,
    pub(crate) member: MemberId,
}

impl Suspicion {
    pub(crate) fn new(member: MemberId) -> Self {
        Suspicion {
            created: Instant::now(),
            member,
        }
    }
}
