use std::slice::Iter;

pub(super) struct CircularBuffer<T>{
    buffer: Vec<T>,
    next_insert_position: usize,
}

impl<T> CircularBuffer<T> {
    pub fn new(size: usize) -> Self {
        CircularBuffer{
            buffer: Vec::<T>::with_capacity(size),
            next_insert_position: 0,
        }
    }

    pub fn push(&mut self, el: T) {
        if self.len() == self.capacity() {
            self.buffer[self.next_insert_position] = el;
        } else {
            self.buffer.push(el);
        }
        self.next_insert_position = (self.next_insert_position + 1) % self.buffer.capacity();
    }

    pub fn remove(&mut self, el: &T) where T: PartialEq {
        let indices = self.buffer.iter().enumerate().filter(|&(idx, e)| {*e == *el}).map(|(idx, _)|{idx}).collect::<Vec<_>>();
        let mut round = 0;
        for idx in indices {
            self.buffer.remove(idx - round);
            if idx - round <= self.next_insert_position {
                self.next_insert_position = if self.next_insert_position == 0 {
                    self.buffer.len() - 1
                } else {
                    self.next_insert_position - 1
                };
            }
            round += 1;
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    pub fn iter(&self) -> impl Iterator<Item=&T> {
        self.buffer[0..self.next_insert_position].iter().rev().chain(
            self.buffer[self.next_insert_position..self.len()].iter().rev()
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new() {
        assert_eq!(CircularBuffer::<i32>::new(4).capacity(), 4);
    }

    #[test]
    fn push() {
        let mut buffer = CircularBuffer::new(3);
        assert_eq!(as_vec(&buffer), []);

        buffer.push(42);
        assert_eq!(buffer.len(), 1);
        assert_eq!(as_vec(&buffer), [42]);

        buffer.push(1);
        buffer.push(2);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.capacity(), 3);
        assert_eq!(as_vec(&buffer), [2,1,42]);

        buffer.push(3);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.capacity(), 3);
        assert_eq!(as_vec(&buffer), [3,2,1]);
    }

    #[test]
    fn remove() {
        let mut buffer = CircularBuffer::new(3);

        buffer.remove(&5);

        buffer.push(42);
        assert_eq!(as_vec(&buffer), [42]);

        buffer.remove(&42);
        assert_eq!(as_vec(&buffer), []);

        buffer.push(1);
        buffer.push(1);
        assert_eq!(as_vec(&buffer), [1,1]);

        buffer.remove(&1);
        assert_eq!(as_vec(&buffer), []);

        buffer.push(1);
        buffer.push(2);
        buffer.push(3);
        buffer.push(4);
        assert_eq!(as_vec(&buffer), [4,3,2]);

        buffer.remove(&3);
        assert_eq!(as_vec(&buffer), [4,2]);

        buffer.push(5);
        assert_eq!(as_vec(&buffer), [5,4,2]);
    }

    fn as_vec<T>(buffer: &CircularBuffer<T>) -> Vec<T> where T: Clone {
        buffer.iter().cloned().collect()
    }
}