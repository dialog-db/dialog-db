#[repr(C)]
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataType {
    Null = 0,
    #[default]
    Bytes = 1,
    Entity = 2,
    Boolean = 3,
    String = 4,
    UnsignedInt = 5,
    SignedInt = 6,
    Float = 7,
    Structured = 8,
    Symbol = 9,
}

impl From<u8> for DataType {
    fn from(value: u8) -> Self {
        Self::from(&value)
    }
}

impl From<&u8> for DataType {
    fn from(value: &u8) -> Self {
        match value {
            1 => DataType::Bytes,
            2 => DataType::Entity,
            3 => DataType::Boolean,
            4 => DataType::String,
            5 => DataType::UnsignedInt,
            6 => DataType::SignedInt,
            7 => DataType::Float,
            8 => DataType::Structured,
            _ => DataType::Null,
        }
    }
}

impl From<DataType> for u8 {
    fn from(value: DataType) -> Self {
        u8::try_from(value as isize).unwrap_or_default()
    }
}
