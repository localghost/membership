#![deny(missing_docs)]

use std::net::SocketAddr;

pub(crate) struct DisseminatedMembers {
    members: Vec<(SocketAddr, u64)>,
}

impl DisseminatedMembers {
    pub(crate) fn new() -> DisseminatedMembers {
        DisseminatedMembers { members: Vec::new() }
    }

    pub(crate) fn get_members(&self) -> impl Iterator<Item = &SocketAddr> {
        self.members.iter().rev().map(|m| &m.0)
    }

    pub(crate) fn update_members(&mut self, n: usize) {
        self.members.iter_mut().rev().take(n).for_each(|m| m.1 += 1);
        self.update()
    }

    pub(crate) fn add_member(&mut self, member: SocketAddr) {
        self.members.push((member, 0));
    }

    pub(crate) fn remove_member(&mut self, member: SocketAddr) {
        self.members
            .remove(self.members.iter().position(|m| m.0 == member).unwrap());
    }

    fn update(&mut self) {
        self.members.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use failure::_core::str::FromStr;

    fn make_members(addresses: &[&str]) -> Vec<SocketAddr> {
        addresses
            .iter()
            .map(|s| SocketAddr::from_str(&format!("{}:2345", *s)).unwrap())
            .collect()
    }

    #[test]
    fn add_member() {
        let members = make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]);
        let mut disseminated = DisseminatedMembers::new();
        members.iter().for_each(|m| disseminated.add_member(*m));

        assert_eq!(
            make_members(&["10.0.0.1", "192.168.0.1", "127.0.0.1"]),
            disseminated.get_members().cloned().collect::<Vec<_>>()
        );
    }

    #[test]
    fn remove_member() {
        let members = make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]);
        let mut disseminated = DisseminatedMembers::new();
        members.iter().for_each(|m| disseminated.add_member(*m));

        assert_eq!(
            make_members(&["10.0.0.1", "192.168.0.1", "127.0.0.1"]),
            disseminated.get_members().cloned().collect::<Vec<_>>()
        );

        disseminated.remove_member(make_members(&["192.168.0.1"])[0]);
        assert_eq!(
            make_members(&["10.0.0.1", "127.0.0.1"]),
            disseminated.get_members().cloned().collect::<Vec<_>>()
        );
    }

    #[test]
    fn update_members() {
        let members = make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]);
        let mut disseminated = DisseminatedMembers::new();
        members.iter().for_each(|m| disseminated.add_member(*m));

        disseminated.update_members(1);
        assert_eq!(
            make_members(&["192.168.0.1", "127.0.0.1", "10.0.0.1"]),
            disseminated.get_members().cloned().collect::<Vec<_>>()
        );
        disseminated.update_members(1);
        assert_eq!(
            make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]),
            disseminated.get_members().cloned().collect::<Vec<_>>()
        );
        disseminated.update_members(2);
        assert_eq!(
            make_members(&["127.0.0.1", "10.0.0.1", "192.168.0.1"]),
            disseminated.get_members().cloned().collect::<Vec<_>>()
        );
    }
}
