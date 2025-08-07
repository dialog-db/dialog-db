use allocator_api2::alloc::Allocator as Alloc;
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc};

// #[derive(Clone)]
pub struct Allocator<T>(Arc<T>)
where
    T: Alloc;

impl<T> Allocator<T>
where
    T: Alloc,
{
    pub fn new(inner: T) -> Self {
        Self(Arc::new(inner))
    }
}

impl<T> Clone for Allocator<T>
where
    T: Alloc,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

unsafe impl<T> Alloc for Allocator<T>
where
    T: Alloc,
{
    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        unsafe { self.0.deallocate(ptr, layout) }
    }

    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, allocator_api2::alloc::AllocError> {
        self.0.allocate(layout)
    }
}

#[derive(Clone)]
pub struct BufferPool<const MIN_CAPACITY: usize> {
    free: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl<const MIN_CAPACITY: usize> BufferPool<MIN_CAPACITY> {
    pub fn take(&self) -> Vec<u8> {
        if let Some(buffer) = self.free.lock().pop_front() {
            buffer
        } else {
            Vec::with_capacity(MIN_CAPACITY)
        }
    }

    pub fn give(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        self.free.lock().push_back(buffer);
    }
}

// #[cfg(test)]
// mod tests {
//     use blink_alloc::SyncBlinkAlloc;

//     use super::Allocator;

//     #[test]
//     fn it_can_clone_the_allocator() {
//         let allocator = Allocator::new(SyncBlinkAlloc::new());
//     }
// }
