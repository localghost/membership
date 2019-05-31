use std::net::IpAddr;
use std::str::FromStr;
use structopt::StructOpt;
use membership::{Config, Gossip};

fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    let config = Config::from_args();
//    let proto_config = ProtocolConfig::from_args();
    Gossip::new(config.proto_config).join(IpAddr::from_str(&config.join_address).unwrap());
}