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
        let (addresses, mut members) = common::create_members(3);
        common::join_neighbours(&mut members)?;

        std::thread::sleep(Duration::from_secs(10));

        for member in &members {
            common::assert_eq_unordered(&addresses, &member.get_members()?);
        }

        common::stop_members(&mut members)?;

        Ok(())
    })
}

#[test]
fn dead_node_discovered() -> TestResult {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    common::in_namespace(|| -> TestResult {
        let (mut addresses, mut members) = common::create_members(3);
        common::join_neighbours(&mut members)?;

        common::advance_epochs(2);

        for member in &members {
            common::assert_eq_unordered(&addresses, &member.get_members()?);
        }

        // remove one member
        let (_, member) = (addresses.pop().unwrap(), members.pop().unwrap());
        common::stop_members(&mut [member])?;

        common::advance_epochs(2);

        for member in &members {
            common::assert_eq_unordered(&addresses, &member.get_members()?);
        }

        common::stop_members(&mut members)?;

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

        common::stop_members(&mut [ms1, ms2])
    })
}
