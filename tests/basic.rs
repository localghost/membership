use failure::Error;
use membership;
use membership::{Membership, ProtocolConfig};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

mod common;

#[test]
fn all_members_alive() {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| {
        let addresses = common::create_interfaces(3);
        let mut mss = common::start_memberships(&addresses);

        std::thread::sleep(Duration::from_secs(10));

        for ms in &mss {
            assert_eq!(addresses, common::get_member_addresses(ms));
        }
        mss.iter_mut().for_each(|m| m.stop().unwrap());
    });
}

#[test]
fn dead_node_discovered() {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| {
        let addresses = common::create_interfaces(3);
        let mut mss = common::start_memberships(&addresses);

        std::thread::sleep(Duration::from_secs(10));

        for ms in &mss {
            assert_eq!(addresses, common::get_member_addresses(ms));
        }

        mss.pop().unwrap().stop();

        std::thread::sleep(Duration::from_secs(10));

        let expected_addresses = addresses.iter().rev().skip(1).rev().cloned().collect::<Vec<_>>();
        for ms in &mss {
            assert_eq!(expected_addresses, common::get_member_addresses(ms));
        }

        mss.iter_mut().for_each(|m| m.stop().unwrap());
    });
}

// FIXME: Add proper checks.
#[test]
fn different_ports() -> Result<(), Error> {
    let mut ms1 = Membership::new(SocketAddr::from_str("127.0.0.1:2345")?, Default::default());
    let mut ms2 = Membership::new(SocketAddr::from_str("127.0.0.1:3456")?, Default::default());
    ms1.join(SocketAddr::from_str("127.0.0.1:3456")?)?;
    ms2.join(SocketAddr::from_str("127.0.0.1:2345")?)?;
    std::thread::sleep(Duration::from_secs(ProtocolConfig::default().protocol_period * 2));
    println!("{:?}", ms1.get_members()?);
    println!("{:?}", ms2.get_members()?);
    ms1.stop()?;
    ms2.stop()
}
