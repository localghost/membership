use anyhow;
use membership;
use membership::Node;
use std::net::SocketAddr;
use std::str::FromStr;

mod common;
use crate::common::*;

type TestResult = anyhow::Result<()>;

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

        // This includes suspicion that by default is triple the epoch.
        // 1. after 5 secs other members ping each other and they don't notice the 3rd member dead
        // 2. after 10 secs both members ping the 3rd member
        // 3. after 11 secs no ACK comes back and both members decide to go via proxy
        // 4. after 12 secs no ACK comes back and both members decide to start suspecting the 3rd member
        // 5. after 27 secs suspicion timeout elapases and both members mark the 3rd one as dead
        advance_epochs(6);

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
        let mut init_members = create_members(10);
        let mut members = init_members.split_off(5);
        create_group(&mut init_members)?;

        advance_epochs(5);

        for member in &init_members {
            assert_eq_unordered(&get_members_addresses(&init_members), &member.get_members()?);
        }

        init_members.pop().unwrap().stop()?;

        advance_epochs(1);

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        advance_epochs(2);

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        advance_epochs(1);

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        init_members.pop().unwrap().stop()?;

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        advance_epochs(1);

        init_members.pop().unwrap().stop()?;

        let mut member = members.pop().unwrap();
        member.join(init_members[1].bind_address()).unwrap();
        init_members.insert(0, member);

        advance_epochs(10);

        for member in &init_members {
            assert_eq_unordered(&get_members_addresses(&init_members), &member.get_members()?);
        }

        stop_members(&mut init_members)?;

        Ok(())
    })
}

#[test]
fn member_unsuspected() -> TestResult {
    in_namespace(|| -> TestResult {
        let mut members = create_members(2);
        create_group(&mut members)?;

        advance_epochs(2);

        for member in &members {
            assert_eq_unordered(&get_members_addresses(&members), &member.get_members()?);
        }

        block_member(members.first().unwrap());

        advance_epochs(2);

        unblock_member(members.first().unwrap());

        advance_epochs(5);

        for member in &members {
            assert_eq_unordered(&get_members_addresses(&members), &member.get_members()?);
        }

        stop_members(&mut members)?;

        Ok(())
    })
}
