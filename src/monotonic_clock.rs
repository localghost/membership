use std::time::Instant;

pub(crate) trait MonotonicClock {
    fn now(&self) -> Instant;
}

pub(crate) struct MonotonicClockImpl {}

impl MonotonicClockImpl {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) fn now(&self) -> Instant {
        Instant::now()
    }
}
