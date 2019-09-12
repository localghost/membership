use failure;
use membership;
use membership::{Membership, ProtocolConfig};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

mod common;

type TestResult = std::result::Result<(), failure::Error>;

#[test]
fn all_members_alive() -> TestResult {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| -> TestResult {
        let addresses = common::create_interfaces(3);
        let mut mss = common::start_memberships(&addresses);

        std::thread::sleep(Duration::from_secs(10));

        for ms in &mss {
            assert_eq!(addresses, common::get_member_addresses(ms));
        }

        common::stop_memberships(&mut mss)?;

        Ok(())
    })
}

#[test]
fn dead_node_discovered() -> TestResult {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| -> TestResult {
        let addresses = common::create_interfaces(3);
        let mut mss = common::start_memberships(&addresses);

        std::thread::sleep(Duration::from_secs(10));

        for ms in &mss {
            assert_eq!(addresses, common::get_member_addresses(ms));
        }

        common::stop_memberships(&mut [mss.pop().unwrap()])?;

        std::thread::sleep(Duration::from_secs(10));

        let expected_addresses = addresses.iter().rev().skip(1).rev().cloned().collect::<Vec<_>>();
        for ms in &mss {
            assert_eq!(expected_addresses, common::get_member_addresses(ms));
        }

        common::stop_memberships(&mut mss)?;

        Ok(())
    })
}

#[test]
fn different_ports() -> TestResult {
    common::in_namespace(|| -> TestResult {
        let address1 = SocketAddr::from_str("127.0.0.1:2345")?;
        let address2 = SocketAddr::from_str("127.0.0.1:3456")?;
        let mut ms1 = Membership::new(address1, Default::default());
        let mut ms2 = Membership::new(address2, Default::default());

        ms1.join(address2)?;
        ms2.join(address1)?;
        std::thread::sleep(Duration::from_secs(ProtocolConfig::default().protocol_period * 2));

        common::assert_eq_unordered(&[address1, address2], &ms1.get_members()?);

        common::stop_memberships(&mut [ms1, ms2])
    })
}
