struct CircularBuffer<T>{
    buffer: Vec<T>,
    last_insert_position: usize
}

impl<T> CircularBuffer<T> {
    pub fn new(size: usize) -> Self {
        CircularBuffer{
            buffer: Vec::<T>::with_capacity(size),
            last_insert_position: 0
        }
    }

    pub fn push(&mut self, el: T) {
        self.buffer.push(el);
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
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
    }
}