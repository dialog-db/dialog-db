use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

#[derive(IntoBytes, Immutable, TryFromBytes, KnownLayout, Debug)]
#[repr(u8)]
pub enum NodeType {
    Branch = 0,
    Segment = 1,
}

impl NodeType {
    pub fn is_branch(&self) -> bool {
        match self {
            NodeType::Branch => true,
            _ => false,
        }
    }

    pub fn is_segment(&self) -> bool {
        match self {
            NodeType::Segment => true,
            _ => false,
        }
    }
}

impl From<NodeType> for u8 {
    fn from(value: NodeType) -> Self {
        match value {
            NodeType::Branch => 0,
            NodeType::Segment => 1,
        }
    }
}
