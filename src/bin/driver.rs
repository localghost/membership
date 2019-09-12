use failure::Error;
use membership::{Membership, ProtocolConfig};
use std::net::SocketAddr;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    #[structopt(short = "j", long = "join-address", default_value = "127.0.0.1:2345")]
    join_address: SocketAddr,

    #[structopt(short = "b", long = "bind-address", default_value = "127.0.0.1:2345")]
    bind_address: SocketAddr,

    // FIXME: this should not be public, fix dependencies between the two configs, make clear which is about protocol
    // and which about client properties.
    #[structopt(flatten)]
    proto_config: ProtocolConfig,
}

fn main() -> Result<(), Error> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    let config = Config::from_args();
    //    let proto_config = ProtocolConfig::from_args();
    //    Gossip::new(config.proto_config).join(IpAddr::from_str(&config.join_address).unwrap());
    let mut membership = Membership::new(config.bind_address, config.proto_config);
    membership.join(config.join_address)?;
    membership.wait()
}
