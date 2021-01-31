#![deny(missing_docs)]

#[derive(Debug)]
struct Item<T> {
    item: T,
    count: u64,
    limit: Option<u64>,
}

impl<T> Item<T> {
    fn new(item: T) -> Self {
        Item {
            item,
            count: 0,
            limit: None,
        }
    }

    fn new_with_limit(item: T, limit: u64) -> Self {
        Item {
            item,
            count: 0,
            limit: Some(limit),
        }
    }
}

struct DisseminatedIter<'a, T: 'a, I: Iterator<Item = &'a mut Item<T>>> {
    items: I,
}

impl<'a, T: 'a, I: Iterator<Item = &'a mut Item<T>>> DisseminatedIter<'a, T, I> {
    fn new(items: I) -> Self {
        Self { items }
    }
}

impl<'a, T, I: Iterator<Item = &'a mut Item<T>>> Iterator for DisseminatedIter<'a, T, I> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next().map(|i| {
            i.count += 1;
            &i.item
        })
    }
}

#[derive(Debug)]
pub(crate) struct Disseminated<T> {
    items: Vec<Item<T>>,
}

impl<T> Disseminated<T>
where
    T: std::cmp::PartialEq,
{
    pub(crate) fn new() -> Self {
        Disseminated { items: Vec::new() }
    }

    pub(crate) fn for_dissemination(&mut self) -> impl Iterator<Item = &T> {
        self.update();
        DisseminatedIter::new(self.items.iter_mut().rev())
    }

    pub(crate) fn iter(&mut self) -> impl Iterator<Item = &T> {
        self.items.iter().rev().map(|i| &i.item)
    }

    pub(crate) fn add(&mut self, item: T) {
        self.items.push(Item::new(item));
    }

    pub(crate) fn add_with_limit(&mut self, item: T, limit: u64) {
        self.items.push(Item::new_with_limit(item, limit));
    }

    pub(crate) fn remove_item(&mut self, item: &T) -> Option<T> {
        if let Some(position) = self.items.iter().position(|i| i.item == *item) {
            return Some(self.remove(position));
        }
        None
    }

    pub(crate) fn remove(&mut self, index: usize) -> T {
        self.items.remove(index).item
    }

    fn update(&mut self) {
        self.items.sort_by(|a, b| b.count.partial_cmp(&a.count).unwrap());
        self.items = self
            .items
            .drain(..)
            .filter(|item| item.limit.is_none() || item.count < item.limit.unwrap())
            .collect::<Vec<_>>();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::SocketAddr;
    use std::str::FromStr;

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
            disseminated.for_dissemination().cloned().collect::<Vec<_>>()
        );
    }

    #[test]
    fn remove_member() {
        let members = make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]);
        let mut disseminated = Disseminated::new();
        members.iter().for_each(|m| disseminated.add(*m));

        assert_eq!(
            make_members(&["10.0.0.1", "192.168.0.1", "127.0.0.1"]),
            disseminated.for_dissemination().cloned().collect::<Vec<_>>()
        );

        disseminated.remove_item(&make_members(&["192.168.0.1"])[0]);
        assert_eq!(
            make_members(&["10.0.0.1", "127.0.0.1"]),
            disseminated.for_dissemination().cloned().collect::<Vec<_>>()
        );
    }

    #[test]
    fn update_members() {
        let members = make_members(&["127.0.0.1", "192.168.0.1", "10.0.0.1"]);
        let mut disseminated = Disseminated::new();
        members.iter().for_each(|m| disseminated.add(*m));

        assert_eq!(
            make_members(&["10.0.0.1"]),
            disseminated.for_dissemination().take(1).cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            make_members(&["192.168.0.1"]),
            disseminated.for_dissemination().take(1).cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            make_members(&["127.0.0.1"]),
            disseminated.for_dissemination().take(1).cloned().collect::<Vec<_>>()
        );
        // All has been disseminated once so internal sorting does not re-order them as the order
        // is determined by the number of times an item has been hand out from the container and
        // not by when it was handed out.
        assert_eq!(
            make_members(&["127.0.0.1"]),
            disseminated.for_dissemination().take(1).cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            make_members(&["192.168.0.1", "10.0.0.1"]),
            disseminated.for_dissemination().take(2).cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            make_members(&["192.168.0.1", "10.0.0.1"]),
            disseminated.for_dissemination().take(2).cloned().collect::<Vec<_>>()
        );
    }

    #[test]
    fn dissemination_limit() {
        let mut disseminated = Disseminated::new();
        disseminated.add_with_limit("foo", 3);

        assert_eq!(
            vec!["foo"],
            disseminated.for_dissemination().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            vec!["foo"],
            disseminated.for_dissemination().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            vec!["foo"],
            disseminated.for_dissemination().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            Vec::<String>::new(),
            disseminated.for_dissemination().cloned().collect::<Vec<_>>()
        );
    }
}
