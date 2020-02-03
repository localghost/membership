use failure;
use membership;
use membership::Node;
use std::net::SocketAddr;
use std::str::FromStr;

mod common;
use crate::common::*;
use sloggers::Build;

type TestResult = std::result::Result<(), failure::Error>;

#[test]
fn all_members_alive() -> TestResult {
    in_namespace(|| -> TestResult {
        let mut members = create_members(3);
        create_group(&mut members)?;

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
    in_namespace(|| -> TestResult {
        let mut members = create_members(3);
        create_group(&mut members)?;

        advance_epochs(2);

        for member in &members {
            assert_eq_unordered(&get_members_addresses(&members), &member.get_members()?);
        }

        // remove one member
        let member = members.pop().unwrap();
        stop_members(&mut [member])?;

        // This includes suspicion that by default is twice the epoch.
        advance_epochs(5);

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
        let mut node1 = Node::new(address1, Default::default());
        let mut node2 = Node::new(address2, Default::default());

        node1.start()?;
        assert_eq_unordered(&[address1], &node1.get_members()?);

        node2.join(address1)?;
        advance_epochs(2);

        assert_eq_unordered(&[address1, address2], &node1.get_members()?);

        stop_members(&mut [node1, node2])
    })
}

#[test]
fn many_notifications() -> TestResult {
    in_namespace(|| -> TestResult {
        let logger = sloggers::terminal::TerminalLoggerBuilder::new().build().unwrap();

        let mut init_members = create_members(5);
        let mut members = create_members(5);
        create_group(&mut init_members)?;

        slog::info!(logger, "TEST: Created group");

        advance_epochs(5);

        slog::info!(logger, "TEST: Group after 5 epochs");

        for member in &init_members {
            assert_eq_unordered(&get_members_addresses(&init_members), &member.get_members()?);
        }

        slog::info!(logger, "TEST: All members compared");

        slog::info!(
            logger,
            "TEST: Remove member {:?}",
            init_members.last().unwrap().bind_address()
        );
        init_members.pop().unwrap().stop()?;

        slog::info!(logger, "TEST: One member stopped");

        advance_epochs(1);

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        slog::info!(
            logger,
            "TEST: New member joined via {:?}",
            init_members[1].bind_address()
        );

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        slog::info!(
            logger,
            "TEST: New member joined via {:?}",
            init_members[1].bind_address()
        );

        advance_epochs(1);

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        slog::info!(
            logger,
            "TEST: New member joined via {:?}",
            init_members[1].bind_address()
        );

        slog::info!(
            logger,
            "TEST: Remove member {:?}",
            init_members.last().unwrap().bind_address()
        );
        init_members.pop().unwrap().stop()?;

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        slog::info!(
            logger,
            "TEST: New member joined via {:?}",
            init_members[1].bind_address()
        );

        advance_epochs(1);

        slog::info!(
            logger,
            "TEST: Remove member {:?}",
            init_members.last().unwrap().bind_address()
        );
        init_members.pop().unwrap().stop()?;

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        slog::info!(
            logger,
            "TEST: New member joined via {:?}",
            init_members[1].bind_address()
        );

        advance_epochs(5);

        slog::info!(logger, "After advancing epoch for the last time");

        for member in &init_members {
            assert_eq_unordered(&get_members_addresses(&init_members), &member.get_members()?);
        }

        slog::info!(logger, "Stopping all members");

        stop_members(&mut init_members)?;

        Ok(())
    })
}
