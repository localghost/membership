use crate::common::get_member_addresses;
use membership;
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
            assert_eq!(addresses, get_member_addresses(ms));
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
            assert_eq!(addresses, get_member_addresses(ms));
        }

        mss.pop().unwrap().stop();

        std::thread::sleep(Duration::from_secs(10));

        let expected_addresses = addresses.iter().rev().skip(1).rev().cloned().collect::<Vec<_>>();
        for ms in &mss {
            assert_eq!(expected_addresses, get_member_addresses(ms));
        }

        mss.iter_mut().for_each(|m| m.stop().unwrap());
    });
}
