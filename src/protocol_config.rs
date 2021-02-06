#![deny(missing_docs)]

/// Configuration for the membership protocol.
#[derive(Debug)]
pub struct ProtocolConfig {
    /// Number of seconds between checking new member.
    pub protocol_period: u64,

    /// Number of seconds to wait for response from a peer.
    ///
    /// Must be significantly (e.g. four times) smaller than `protocol_period`.
    pub ack_timeout: u8,

    /// Maximum number of members selected for indirect probing.
    ///
    /// When probed member does not respond in `ack_timeout`, `num_indirect` other members are asked to check
    /// the probed member.
    pub num_indirect: u8,

    /// Number of seconds to keep member as suspected before removing it.
    pub suspect_timeout: u64,

    /// Number of seconds until failed join request will be retried.
    pub join_retry_timeout: u64,

    /// Number of times a notification is disseminated to other group members.
    pub notification_dissemination_times: u64,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            protocol_period: 5,
            ack_timeout: 1,
            num_indirect: 3,
            suspect_timeout: 15,
            join_retry_timeout: 3,
            notification_dissemination_times: 20,
        }
    }
}
