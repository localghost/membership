use membership::{Config, Membership};
use structopt::StructOpt;

fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    let config = Config::from_args();
    //    let proto_config = ProtocolConfig::from_args();
    //    Gossip::new(config.proto_config).join(IpAddr::from_str(&config.join_address).unwrap());
    let mut membership = Membership::new(config.bind_address, config.proto_config);
    membership.join(config.join_address);
    membership.wait();
}
