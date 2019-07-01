use std::slice::Iter;

struct CircularBuffer<T>{
    buffer: Vec<T>,
    next_insert_position: usize
}

impl<T> CircularBuffer<T> {
    pub fn new(size: usize) -> Self {
        CircularBuffer{
            buffer: Vec::<T>::with_capacity(size),
            next_insert_position: 0
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

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    pub fn iter(&self) -> Iter<T> {
        self.buffer.iter()
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
        buffer.push(42);

        assert_eq!(buffer.len(), 1);

        buffer.push(1);
        buffer.push(2);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.capacity(), 3);
        assert_eq!(buffer.iter().as_slice(), [42,1,2]);

        buffer.push(3);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.capacity(), 3);
        assert_eq!(buffer.iter().as_slice(), [3,1,2]);

        buffer.push(4);
        assert_eq!(buffer.iter().as_slice(), [3,4,2]);
    }
}