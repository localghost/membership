use crate::disseminated::Disseminated;
use crate::member::{Member, MemberId};
// use crate::notification::Notification;
use crate::result::Result;
use anyhow::bail;
use rand::prelude::SliceRandom;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::collections::{HashMap, HashSet};

struct BroadcastIterator<'a, I: Iterator<Item = &'a MemberId>> {
    disseminated: I,
    members: &'a HashMap<MemberId, Member>,
}

impl<'a, I: Iterator<Item = &'a MemberId>> BroadcastIterator<'a, I> {
    fn new(disseminated: I, members: &'a HashMap<MemberId, Member>) -> Self {
        Self {
            disseminated: disseminated,
            members,
        }
    }
}

impl<'a, I: Iterator<Item = &'a MemberId>> Iterator for BroadcastIterator<'a, I> {
    type Item = &'a Member;

    fn next(&mut self) -> Option<Self::Item> {
        self.disseminated.next().map(|id| &self.members[id])
    }
}

pub(crate) struct Members {
    ordered: Vec<MemberId>,
    broadcast: Disseminated<MemberId>,
    // notifications: Disseminated<Notification>,
    members: HashMap<MemberId, Member>,
    dead: HashSet<MemberId>,
    next_index: usize,
    rng: SmallRng,
}

impl Members {
    pub(crate) fn new() -> Self {
        Self {
            ordered: vec![],
            broadcast: Disseminated::new(),
            // notifications: Disseminated::new(),
            members: HashMap::new(),
            dead: HashSet::new(),
            next_index: 0,
            rng: SmallRng::from_entropy(),
        }
    }
    pub(crate) fn next(&mut self) -> Option<&Member> {
        if self.ordered.is_empty() {
            return None;
        }
        // Following SWIM paper, section 4.3, next member to probe is picked in round-robin fashion, with all
        // the members randomly shuffled after each one has been probed.
        // FIXME: one thing that is missing is that new members are always added at the end instead of at uniformly
        // random position.
        if self.next_index == 0 {
            self.ordered.shuffle(&mut self.rng);
        }
        let target = self.ordered[self.next_index];
        self.next_index = (self.next_index + 1) % self.ordered.len();
        self.get(&target)
    }

    pub(crate) fn has(&self, id: &MemberId) -> bool {
        self.members.contains_key(id)
    }
    pub(crate) fn get(&self, id: &MemberId) -> Option<&Member> {
        self.members.get(id)
    }

    pub(crate) fn add_or_update(&mut self, member: Member) -> Result<()> {
        if self.dead.contains(&member.id) {
            bail!("Member {} is already marked as dead")
        }
        // If the node is already registered then we only update its incarnation if a higher one is spotted.
        if let Some(m) = self.members.get_mut(&member.id) {
            if m.incarnation < member.incarnation {
                m.incarnation = member.incarnation;
            }
        } else {
            self.members.insert(member.id, member.clone());
            self.ordered.push(member.id);
            self.broadcast.add(member.id);
        }
        Ok(())
    }

    pub(crate) fn remove(&mut self, id: &MemberId) -> Result<()> {
        match self.members.remove(id) {
            Some(_) => {
                self.dead.insert(*id);

                let idx = self.ordered.iter().position(|e| e == id).unwrap();
                self.ordered.remove(idx);
                self.broadcast.remove_item(id);
                if idx <= self.next_index && self.next_index > 0 {
                    self.next_index -= 1;
                }
                Ok(())
            }
            None => bail!("Trying to remove unknown member {:?}", id),
        }
    }

    pub(crate) fn for_broadcast(&mut self) -> impl Iterator<Item = &Member> {
        BroadcastIterator::new(self.broadcast.for_dissemination(), &self.members)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Member> {
        self.members.values()
    }
}
