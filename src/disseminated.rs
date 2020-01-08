#![deny(missing_docs)]

#[derive(Debug)]
pub(crate) struct Disseminated<T> {
    items: Vec<(T, u64)>,
    limit: Option<u64>,
}

impl<T> Disseminated<T>
where
    T: std::cmp::PartialEq,
{
    pub(crate) fn new() -> Disseminated<T> {
        Disseminated {
            items: Vec::new(),
            limit: None,
        }
    }

    pub(crate) fn with_limit(limit: u64) -> Disseminated<T> {
        let mut result = Self::new();
        result.set_limit(limit);
        result
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter().rev().map(|m| &m.0)
    }

    pub(crate) fn mark(&mut self, n: usize) {
        self.items.iter_mut().rev().take(n).for_each(|m| m.1 += 1);
        self.update()
    }

    pub(crate) fn add(&mut self, item: T) {
        self.items.push((item, 0));
    }

    pub(crate) fn remove(&mut self, item: &T) {
        if let Some(position) = self.items.iter().position(|m| m.0 == *item) {
            self.items.remove(position);
        }
    }

    pub(crate) fn set_limit(&mut self, limit: u64) {
        self.limit = Some(limit);
    }

    fn update(&mut self) {
        self.items.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        if let Some(limit) = self.limit {
            self.items = self.items.drain(..).filter(|item| item.1 <= limit).collect::<Vec<_>>();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use failure::_core::str::FromStr;
    use std::net::SocketAddr;

    fn make_members(addresses: &[&str]) -> Vec<SocketAddr> {
        addresses
            .iter()
            .map(|s| SocketAddr::from_str(&format!("{}:2345", *s)).unwrap())
            .collect()
    }

    #[test]
    fn add_member() {
        let members = make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]);
        let mut disseminated = Disseminated::new();
        members.iter().for_each(|m| disseminated.add(*m));

        assert_eq!(
            make_members(&["10.0.0.1", "192.168.0.1", "127.0.0.1"]),
            disseminated.iter().cloned().collect::<Vec<_>>()
        );
    }

    #[test]
    fn remove_member() {
        let members = make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]);
        let mut disseminated = Disseminated::new();
        members.iter().for_each(|m| disseminated.add(*m));

        assert_eq!(
            make_members(&["10.0.0.1", "192.168.0.1", "127.0.0.1"]),
            disseminated.iter().cloned().collect::<Vec<_>>()
        );

        //        disseminated.remove(make_members(&["192.168.0.1"])[0]);
        assert_eq!(
            make_members(&["10.0.0.1", "127.0.0.1"]),
            disseminated.iter().cloned().collect::<Vec<_>>()
        );
    }

    #[test]
    fn update_members() {
        let members = make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]);
        let mut disseminated = Disseminated::new();
        members.iter().for_each(|m| disseminated.add(*m));

        disseminated.mark(1);
        assert_eq!(
            make_members(&["192.168.0.1", "127.0.0.1", "10.0.0.1"]),
            disseminated.iter().cloned().collect::<Vec<_>>()
        );
        disseminated.mark(1);
        assert_eq!(
            make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]),
            disseminated.iter().cloned().collect::<Vec<_>>()
        );
        disseminated.mark(2);
        assert_eq!(
            make_members(&["127.0.0.1", "10.0.0.1", "192.168.0.1"]),
            disseminated.iter().cloned().collect::<Vec<_>>()
        );
    }
}
