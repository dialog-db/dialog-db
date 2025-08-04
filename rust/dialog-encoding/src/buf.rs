use bytes::Bytes;

/// A trait implemented by things that own some bytes; a [Buf] also has an
/// associated [Ref]. Converting from [Buf] to [Ref] is cheap (a [Ref] is just a
/// pointer; no allocations required).
pub trait Buf<'a>: Clone
where
    Self: Sized + 'a,
{
    type Ref: Ref<'a, Self> + 'a;

    fn to_ref(&'a self) -> Self::Ref;
}

/// A trait implemented by things that are references to some bytes owned by
/// something else; a [Ref] can be converted back to its originating [Buf] at
/// the cost of copying its contents into a newly allocated container.
pub trait Ref<'a, Buf>: Clone
where
    Buf: self::Buf<'a, Ref = Self>,
{
    fn to_buf(&self) -> Buf;
}

/// A union of a [Buf] or its counterpart [Ref], to enable expressing
/// collections that may contain either owned or referenced bytes.
#[derive(Clone)]
pub enum BufOrRef<'a, Buf>
where
    Buf: self::Buf<'a>,
{
    Buf(Buf),
    Ref(Buf::Ref),
}

impl<'a, Buf> std::fmt::Debug for BufOrRef<'a, Buf>
where
    Buf: self::Buf<'a> + std::fmt::Debug,
    Buf::Ref: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Buf(arg0) => f.debug_tuple("Buf").field(arg0).finish(),
            Self::Ref(arg0) => f.debug_tuple("Ref").field(arg0).finish(),
        }
    }
}

impl<'a, Buf> BufOrRef<'a, Buf>
where
    Buf: self::Buf<'a>,
{
    pub fn to_ref(&'a self) -> Buf::Ref {
        match self {
            BufOrRef::Buf(buffer) => buffer.to_ref(),
            BufOrRef::Ref(reference) => reference.clone(),
        }
    }

    pub fn to_buf(&'a self) -> Buf {
        match self {
            BufOrRef::Buf(buffer) => buffer.clone(),
            BufOrRef::Ref(reference) => reference.to_buf(),
        }
    }
}

impl<'a> Buf<'a> for Bytes {
    type Ref = &'a [u8];

    fn to_ref(&'a self) -> Self::Ref {
        self.as_ref()
    }
}

impl<'a> Ref<'a, Bytes> for &'a [u8] {
    fn to_buf(&self) -> Bytes {
        Bytes::copy_from_slice(*self)
    }
}

impl<'a> Buf<'a> for Vec<u8> {
    type Ref = &'a [u8];

    fn to_ref(&'a self) -> Self::Ref {
        self.as_ref()
    }
}

impl<'a> Ref<'a, Vec<u8>> for &'a [u8] {
    fn to_buf(&self) -> Vec<u8> {
        self.to_vec()
    }
}

impl<'a, const N: usize> Buf<'a> for [u8; N] {
    type Ref = &'a [u8; N];

    fn to_ref(&'a self) -> Self::Ref {
        self
    }
}

impl<'a, const N: usize> Ref<'a, [u8; N]> for &'a [u8; N] {
    fn to_buf(&self) -> [u8; N] {
        **self
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::BufOrRef;

    struct Container<'a, const N: usize> {
        pub elements: Vec<BufOrRef<'a, [u8; N]>>,
    }

    #[test]
    fn it_can_collect_both_buffers_and_references() -> Result<()> {
        let external = vec![[0u8; 32], [0u8; 32]];
        let external_one = BufOrRef::Ref(external.get(0).unwrap());
        let external_two = BufOrRef::Ref(external.get(1).unwrap());

        let mut container = Container {
            elements: vec![external_one, external_two],
        };

        container.elements.push(BufOrRef::Buf([0u8; 32]));

        let _refs: Vec<&[u8; 32]> = container
            .elements
            .iter()
            .map(|element| element.to_ref())
            .collect();

        Ok(())
    }
}
