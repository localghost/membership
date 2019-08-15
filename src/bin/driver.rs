use std::net::IpAddr;
use std::str::FromStr;
use structopt::StructOpt;
use membership::{Config, Gossip, Gossip2};

fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    let config = Config::from_args();
//    let proto_config = ProtocolConfig::from_args();
//    Gossip::new(config.proto_config).join(IpAddr::from_str(&config.join_address).unwrap());
    let mut gossip2 = Gossip2::new(config.proto_config);
    gossip2.join(IpAddr::from_str(&config.join_address).unwrap());
    std::thread::sleep(std::time::Duration::from_secs(5));
//    println!("{:?}", gossip2.get_members());
    println!("exiting");
    gossip2.stop();
    gossip2.wait();
}