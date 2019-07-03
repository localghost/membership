pub struct UniqueCircularBuffer<T> {
    buffer: Vec<T>,
    size: usize,
}

// FIXME not the most effective implementation, consider using a list or research for a better data structure
impl<T> UniqueCircularBuffer<T> {
    pub fn new(size: usize) -> Self {
        UniqueCircularBuffer {
            buffer: Vec::<T>::with_capacity(size+1),
            size,
        }
    }

    pub fn push(&mut self, el: T) where T: PartialEq {
        self.remove(&el);
        self.buffer.insert(0, el);
        self.buffer.truncate(self.size);
    }

    pub fn remove(&mut self, el: &T) -> usize where T: PartialEq {
        let indices = self.buffer.iter().enumerate().filter(|&(_, e)| {*e == *el}).map(|(idx, _)|{idx}).collect::<Vec<_>>();
        let mut round: usize = 0;
        for idx in &indices {
            self.buffer.remove(idx - round);
            round += 1;
        }
        indices.len()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn iter(&self) -> impl Iterator<Item=&T> {
        self.buffer.iter()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new() {
        assert_eq!(UniqueCircularBuffer::<i32>::new(4).len(), 0);
    }

    #[test]
    fn push() {
        let mut buffer = UniqueCircularBuffer::new(3);
        assert_eq!(as_vec(&buffer), []);

        buffer.push(42);
        assert_eq!(buffer.len(), 1);
        assert_eq!(as_vec(&buffer), [42]);

        buffer.push(1);
        buffer.push(2);
        assert_eq!(buffer.len(), 3);
        assert_eq!(as_vec(&buffer), [2,1,42]);

        buffer.push(3);
        assert_eq!(buffer.len(), 3);
        assert_eq!(as_vec(&buffer), [3,2,1]);

        buffer.push(2);
        assert_eq!(as_vec(&buffer), [2,3,1]);
    }

    #[test]
    fn remove() {
        let mut buffer = UniqueCircularBuffer::new(3);

        buffer.remove(&5);

        buffer.push(42);
        assert_eq!(as_vec(&buffer), [42]);

        buffer.remove(&42);
        assert_eq!(as_vec(&buffer), []);

        buffer.push(1);
        buffer.push(1);
        assert_eq!(as_vec(&buffer), [1]);

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

    fn as_vec<T>(buffer: &UniqueCircularBuffer<T>) -> Vec<T> where T: Clone {
        buffer.iter().cloned().collect()
    }
}