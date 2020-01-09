use crate::member::MemberId;
use std::time::Instant;

pub(crate) struct Suspicion {
    pub(crate) created: Instant,
    pub(crate) member_id: MemberId,
}

impl Suspicion {
    pub(crate) fn new(member_id: MemberId) -> Self {
        Suspicion {
            created: Instant::now(),
            member_id,
        }
    }
}
