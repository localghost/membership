#![deny(missing_docs)]

/// Configuration for the membership protocol.
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
    pub suspect_timeout: u8,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            protocol_period: 5,
            ack_timeout: 1,
            num_indirect: 3,
        }
    }
}
