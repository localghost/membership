use failure;
use membership;
use membership::Node;
use std::net::SocketAddr;
use std::str::FromStr;

mod common;
use crate::common::*;

type TestResult = std::result::Result<(), failure::Error>;

#[test]
fn all_members_alive() -> TestResult {
    //    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    in_namespace(|| -> TestResult {
        let mut members = create_members(3);
        join_neighbours(&mut members)?;

        advance_epochs(2);

        for member in &members {
            assert_eq_unordered(&get_members_addresses(&members), &member.get_members()?);
        }

        stop_members(&mut members)?;

        Ok(())
    })
}

#[test]
fn dead_node_discovered() -> TestResult {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    in_namespace(|| -> TestResult {
        let mut members = create_members(3);
        join_neighbours(&mut members)?;

        advance_epochs(2);

        for member in &members {
            assert_eq_unordered(&get_members_addresses(&members), &member.get_members()?);
        }

        // remove one member
        let member = members.pop().unwrap();
        stop_members(&mut [member])?;

        advance_epochs(2);

        for member in &members {
            assert_eq_unordered(&get_members_addresses(&members), &member.get_members()?);
        }

        stop_members(&mut members)?;

        Ok(())
    })
}

#[test]
fn different_ports() -> TestResult {
    in_namespace(|| -> TestResult {
        let address1 = SocketAddr::from_str("127.0.0.1:2345")?;
        let address2 = SocketAddr::from_str("127.0.0.1:3456")?;
        let mut ms1 = Node::new(address1, Default::default());
        let mut ms2 = Node::new(address2, Default::default());

        ms1.join(address2)?;
        ms2.join(address1)?;
        advance_epochs(2);

        assert_eq_unordered(&[address1, address2], &ms1.get_members()?);

        stop_members(&mut [ms1, ms2])
    })
}
