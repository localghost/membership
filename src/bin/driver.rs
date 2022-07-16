use tracing::info;
use membership::{Node, ProtocolConfig};
use std::net::SocketAddr;
use structopt::StructOpt;
use tracing_subscriber;

#[derive(StructOpt)]
struct Options {
    #[structopt(short = "j", long = "join-address")]
    join_address: Option<SocketAddr>,

    #[structopt(short = "b", long = "bind-address", default_value = "127.0.0.1:2345")]
    bind_address: SocketAddr,

    // FIXME: this should not be public, fix dependencies between the two configs, make clear which is about protocol
    // and which about client properties.
    #[structopt(flatten)]
    proto_config: ProtocolOptions,
}

#[derive(StructOpt)]
struct ProtocolOptions {
    #[structopt(short = "o", long = "proto-period", default_value = "5")]
    pub protocol_period: u64,

    #[structopt(short = "a", long = "ack-timeout", default_value = "1")]
    pub ack_timeout: u8,

    #[structopt(long = "num-indirect", default_value = "3")]
    pub num_indirect: u8,
}

impl From<ProtocolOptions> for ProtocolConfig {
    fn from(options: ProtocolOptions) -> Self {
        ProtocolConfig {
            protocol_period: options.protocol_period,
            ack_timeout: options.ack_timeout,
            num_indirect: options.num_indirect,
            suspect_timeout: options.protocol_period * 2,
            ..Default::default()
        }
    }
}

fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt().with_max_level(tracing::Level::DEBUG).finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    // env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    let config = Options::from_args();
    let mut membership = Node::new(config.bind_address, ProtocolConfig::from(config.proto_config));
    info!("Running at: {}", config.bind_address);
    // membership.set_logger(TerminalLoggerBuilder::new().build()?);
    match config.join_address {
        Some(address) => membership.join(address),
        None => membership.start(),
    }?;
    membership.wait()
}
