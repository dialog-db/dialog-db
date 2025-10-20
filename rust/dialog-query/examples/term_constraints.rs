use dialog_query::artifact::Value;
use serde::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Kind {
    Bytes = 0,
    Entity = 1,
    Boolean = 2,
    String = 3,
    UnsignedInt = 4,
    SignedInt = 5,
    Float = 6,
    /// TBD structured data (flatbuffers?)
    Record = 7,
    /// A symbol type, used to distinguish attributes from other strings
    Symbol = 8,

    I8 = 9,
    I16 = 10,
    I32 = 11,
    I64 = 12,
    U8 = 13,
    U16 = 14,
    U32 = 15,
    U64 = 16,
    F32 = 17,
    F64 = 18,
}

trait TypeInfo: Serialize + Into<Kind> {
    fn kind(&self) -> Kind;
}

trait Type: TypeInfo {
    fn kind() -> Kind;
    fn make() -> Self;
}

#[derive(Serialize, Clone)]
#[serde(into = "Kind", try_from = "Kind")]
struct Buffer;

impl Into<Kind> for Buffer {
    fn into(self) -> Kind {
        self.kind()
    }
}

impl TryFrom<Kind> for Buffer {
    type Error = std::string::String;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if value == <Self as Type>::kind() {
            Ok(Self)
        } else {
            Err("Wrong tag".into())
        }
    }
}

impl TypeInfo for Buffer {
    fn kind(&self) -> Kind {
        <Self as Type>::kind()
    }
}

impl Type for Buffer {
    fn kind() -> Kind {
        Kind::Bytes
    }

    fn make() -> Self {
        Self
    }
}

#[derive(Serialize, Clone)]
#[serde(into = "Kind", try_from = "Kind")]
struct String;

impl Into<Kind> for String {
    fn into(self) -> Kind {
        self.kind()
    }
}

impl TryFrom<Kind> for String {
    type Error = std::string::String;

    fn try_from(value: Kind) -> Result<Self, Self::Error> {
        if value == <Self as Type>::kind() {
            Ok(Self)
        } else {
            Err("Wrong tag".into())
        }
    }
}

impl TypeInfo for String {
    fn kind(&self) -> Kind {
        <Self as Type>::kind()
    }
}

impl Type for String {
    fn kind() -> Kind {
        Kind::String
    }

    fn make() -> Self {
        Self
    }
}

#[derive(Serialize, Clone)]
#[serde(into = "Kind", try_from = "Kind")]
struct U8;

impl Into<Kind> for U8 {
    fn into(self) -> Kind {
        self.kind()
    }
}

impl TypeInfo for U8 {
    fn kind(&self) -> Kind {
        <Self as Type>::kind()
    }
}

impl Type for U8 {
    fn kind() -> Kind {
        Kind::U8
    }

    fn make() -> Self {
        Self
    }
}

#[derive(Serialize, Clone)]
#[serde(into = "Kind", try_from = "Kind")]
struct U16;

impl Into<Kind> for U16 {
    fn into(self) -> Kind {
        self.kind()
    }
}

impl TypeInfo for U16 {
    fn kind(&self) -> Kind {
        <Self as Type>::kind()
    }
}

impl Type for U16 {
    fn kind() -> Kind {
        Kind::U16
    }

    fn make() -> Self {
        Self
    }
}

impl From<U8> for U16 {
    fn from(_value: U8) -> Self {
        Self
    }
}

impl From<Term<U8>> for Term<U16> {
    fn from(term: Term<U8>) -> Self {
        match term {
            Term::Variable { name, .. } => Term::Variable {
                name,
                ty: <U16 as Type>::make(),
            },
            Term::Constant { value, .. } => Term::Constant {
                value,
                ty: <U16 as Type>::make(),
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
#[repr(transparent)]
pub struct Any(Kind);

impl TypeInfo for Any {
    fn kind(&self) -> Kind {
        self.0.to_owned()
    }
}

impl From<Kind> for Any {
    fn from(value: Kind) -> Self {
        Any(value)
    }
}

impl From<Any> for Kind {
    fn from(value: Any) -> Self {
        value.0
    }
}

impl<T: Type> From<Term<T>> for Term<Any> {
    fn from(term: Term<T>) -> Self {
        match term {
            Term::Variable { name, .. } => Term::Variable {
                name,
                ty: Any::from(<T as Type>::kind()),
            },
            Term::Constant { value, .. } => Term::Constant {
                value,
                ty: Any::from(<T as Type>::kind()),
            },
        }
    }
}

impl<T> TryFrom<Term<Any>> for Term<T>
where
    T: Type,
{
    type Error = std::string::String;

    fn try_from(term: Term<Any>) -> Result<Self, Self::Error> {
        match term {
            Term::Variable { name, ty } => {
                if ty.0 == <T as Type>::kind() {
                    Ok(Term::Variable {
                        name,
                        ty: <T as Type>::make(),
                    })
                } else {
                    Err("Cannot convert to type".into())
                }
            }
            Term::Constant { value, .. } => Ok(Term::Constant {
                value,
                ty: <T as Type>::make(),
            }),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Term<T>
where
    T: TypeInfo,
{
    Variable { name: std::string::String, ty: T },
    Constant { value: Value, ty: T },
}

pub enum Constraint {
    Numeric,
    Textual,
}

pub enum Textual {
    Contains(Value),
    StartsWith(Value),
    EndsWith(Value),
}

pub enum Numeric {
    GreaterThan(Value),
    GreaterThanOrEqual(Value),
    LessThan(Value),
    LessThanOrEqual(Value),
}

impl<T> Term<T>
where
    T: Type,
{
    pub fn var(name: &str) -> Self {
        Self::Variable {
            name: name.into(),
            ty: T::make(),
        }
    }
}

pub fn test_infer() {
    let unknown = Term::var("foo");
    str(&unknown);

    let any: Term<Any> = unknown.into();

    let _try_string = Term::<String>::try_from(any);

    let uint8 = Term::<U8>::var("bar");
    let _uint16: Term<U16> = uint8.into();
    // let u8back: Term<U8> = uint16.into();
}

fn str(_term: &Term<String>) {}

// fn serialization() {
//     let term = Term::<String>::var("foo");
//     let json = serde_json::to_string(&term).unwrap();
//     let result_ any: Term<Any> = serde_json::from_str(&json).unwrap();
//     let _result_typed = Term::<String>::try_from(result_any).unwrap();
// }

fn main() {
    println!("Hello, world!");
}
