use serde::Serialize;

/// Trait for collecting parameters from capability constraints.
///
/// This trait follows the visitor pattern (similar to serde's `Serializer`),
/// allowing consumers to decide the output format. The core capability system
/// doesn't depend on any specific format (like IPLD).
///
/// Implementors decide how to handle each parameter value.
pub trait Parameters {
    /// Set a parameter value. The value must be serializable.
    fn set<V: Serialize + ?Sized>(&mut self, key: &str, value: &V);
}

/// Trait for types that can contribute parameters to capability invocations.
///
/// This trait is auto-implemented for all `Serialize` types via a blanket impl.
/// The `parametrize` method extracts struct fields and calls `params.set()` for each one.
///
/// **Note**: For capability chains (types implementing `Ability`), use
/// `Ability::parametrize()` instead, which properly walks the chain and collects
/// parameters from each constraint.
pub trait Settings {
    /// Contribute this type's fields to the given parameters collector.
    fn parametrize<P: Parameters>(&self, params: &mut P);
}

/// Blanket impl for all Serialize types - extracts struct fields as parameters.
impl<T: Serialize> Settings for T {
    fn parametrize<P: Parameters>(&self, params: &mut P) {
        // Use a mini serde serializer that only handles struct fields
        let _ = self.serialize(FieldExtractor { params });
    }
}

/// Simple serde serializer that only extracts struct fields.
struct FieldExtractor<'a, P: Parameters> {
    params: &'a mut P,
}

impl<'a, P: Parameters> serde::Serializer for FieldExtractor<'a, P> {
    type Ok = ();
    type Error = std::fmt::Error;
    type SerializeSeq = serde::ser::Impossible<(), Self::Error>;
    type SerializeTuple = serde::ser::Impossible<(), Self::Error>;
    type SerializeTupleStruct = serde::ser::Impossible<(), Self::Error>;
    type SerializeTupleVariant = serde::ser::Impossible<(), Self::Error>;
    type SerializeMap = serde::ser::Impossible<(), Self::Error>;
    type SerializeStruct = StructFields<'a, P>;
    type SerializeStructVariant = serde::ser::Impossible<(), Self::Error>;

    // We only care about structs - everything else is a no-op
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(StructFields { params: self.params })
    }

    // All other types are ignored (unit structs, primitives, etc.)
    fn serialize_bool(self, _: bool) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_i8(self, _: i8) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_i16(self, _: i16) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_i32(self, _: i32) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_i64(self, _: i64) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_u8(self, _: u8) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_u16(self, _: u16) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_u32(self, _: u32) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_u64(self, _: u64) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_f32(self, _: f32) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_f64(self, _: f64) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_char(self, _: char) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_str(self, _: &str) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_bytes(self, _: &[u8]) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_some<T: ?Sized + Serialize>(self, _: &T) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(self, _: &'static str, _: &T) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(self, _: &'static str, _: u32, _: &'static str, _: &T) -> Result<Self::Ok, Self::Error> { Ok(()) }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> { Err(std::fmt::Error) }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> { Err(std::fmt::Error) }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct, Self::Error> { Err(std::fmt::Error) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeTupleVariant, Self::Error> { Err(std::fmt::Error) }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> { Err(std::fmt::Error) }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeStructVariant, Self::Error> { Err(std::fmt::Error) }
}

/// Helper for extracting struct fields.
struct StructFields<'a, P: Parameters> {
    params: &'a mut P,
}

impl<P: Parameters> serde::ser::SerializeStruct for StructFields<'_, P> {
    type Ok = ();
    type Error = std::fmt::Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.params.set(key, value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
