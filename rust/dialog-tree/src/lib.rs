mod node;
pub use node::*;

mod key;
pub use key::*;

mod value;
pub use value::*;

mod error;
pub use error::*;

mod storage;
pub use storage::*;

mod tree;
pub use tree::*;

mod distribution;
pub use distribution::*;

mod operation;
pub use operation::*;

#[cfg(test)]
mod tests {
    struct Value(u8);

    impl<'a> From<&'a Value> for ValueRef<'a> {
        fn from(value: &'a Value) -> Self {
            ValueRef(&value.0)
        }
    }

    struct ValueRef<'a>(&'a u8);

    struct Buf<'a> {
        inner: Vec<ValueRef<'a>>,
    }

    impl<'a> Buf<'a> {
        pub fn new() -> Self {
            Self { inner: Vec::new() }
        }

        pub fn from_refs<I>(inner: I) -> Self
        where
            I: Iterator<Item = &'a u8>,
        {
            Self {
                inner: inner.map(|b| ValueRef(b)).collect(),
            }
        }

        pub fn push_ref(&mut self, value: &'a u8) {
            self.inner.push(ValueRef(value));
        }

        pub fn push(&'a mut self, value: &'a Value) {
            self.inner.push(value.into());
        }
    }

    #[test]
    fn it_does_the_thing() {
        let bytes = vec![0, 1, 2u8];
        let value = Value(123);
        let mut buf = Buf::from_refs(bytes.iter());
        buf.push(&value);
    }
}
