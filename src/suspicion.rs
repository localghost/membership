use crate::member::Member;
use std::time::Instant;

pub(crate) struct Suspicion {
    start: Instant,
    member: Member,
}
