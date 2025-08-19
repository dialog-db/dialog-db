//! This example shows what the macro expansion would look like for:
//! ```
//! #[relation]
//! enum MicroshaftEmployee {
//!     Name(String),
//!     Job(String),
//!     Salary(u32),
//!     #[many]
//!     Address(String),
//! }
//! ```
//!
//! The macro would generate individual structs that implement Attribute trait
//! plus a module for accessing them as MicroshaftEmployee::Name, etc.

use dialog_query::{Attribute, Cardinality, ValueDataType};

// === MACRO EXPANSION ===
// This is what the `#[relation]` macro would generate from:
//
// #[relation]
// enum MicroshaftEmployee {
//     Name(String),
//     Job(String),
//     Salary(u32),
//     #[many]
//     Address(String),
// }

// Generated individual attribute structs
pub struct MicroshaftEmployeeName(String);
impl Attribute for MicroshaftEmployeeName {
    fn name() -> &'static str {
        "microshaft.employee/name"
    }
    fn cardinality() -> Cardinality {
        Cardinality::One
    }
    fn value_type() -> ValueDataType {
        ValueDataType::String
    }
}
impl MicroshaftEmployeeName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

pub struct MicroshaftEmployeeJob(String);
impl Attribute for MicroshaftEmployeeJob {
    fn name() -> &'static str {
        "microshaft.employee/job"
    }
    fn cardinality() -> Cardinality {
        Cardinality::One
    }
    fn value_type() -> ValueDataType {
        ValueDataType::String
    }
}
impl MicroshaftEmployeeJob {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

pub struct MicroshaftEmployeeSalary(u32);
impl Attribute for MicroshaftEmployeeSalary {
    fn name() -> &'static str {
        "microshaft.employee/salary"
    }
    fn cardinality() -> Cardinality {
        Cardinality::One
    }
    fn value_type() -> ValueDataType {
        ValueDataType::UnsignedInt
    }
}
impl MicroshaftEmployeeSalary {
    pub fn new(value: u32) -> Self {
        Self(value)
    }
}

pub struct MicroshaftEmployeeAddress(String);
impl Attribute for MicroshaftEmployeeAddress {
    fn name() -> &'static str {
        "microshaft.employee/address"
    }
    fn cardinality() -> Cardinality {
        Cardinality::Many  // Because of #[many] attribute
    }
    fn value_type() -> ValueDataType {
        ValueDataType::String
    }
}
impl MicroshaftEmployeeAddress {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

// Generated module for accessing types as MicroshaftEmployee::Name
#[allow(non_snake_case)]
pub mod MicroshaftEmployee {
    pub type Name = super::MicroshaftEmployeeName;
    pub type Job = super::MicroshaftEmployeeJob;
    pub type Salary = super::MicroshaftEmployeeSalary;
    pub type Address = super::MicroshaftEmployeeAddress;
}

fn main() {
    println!("=== Macro Expansion Example ===\n");
    
    // Now you can use the MicroshaftEmployee::Name syntax!
    let _name = MicroshaftEmployee::Name::new("John Doe");
    let _job = MicroshaftEmployee::Job::new("Software Engineer");
    let _salary = MicroshaftEmployee::Salary::new(150000);
    let _address = MicroshaftEmployee::Address::new("123 Main St");
    
    println!("✅ MicroshaftEmployee::Name::new() works!");
    println!("✅ MicroshaftEmployee::Job::new() works!");
    println!("✅ MicroshaftEmployee::Salary::new() works!");
    println!("✅ MicroshaftEmployee::Address::new() works!");
    
    // The structs implement the Attribute trait
    println!("\nAttribute info:");
    println!("Name attribute: {}", MicroshaftEmployeeName::name());
    println!("Job cardinality: {:?}", MicroshaftEmployeeJob::cardinality());
    println!("Salary value type: {:?}", MicroshaftEmployeeSalary::value_type());
    
    // Address has Many cardinality because of #[many]
    println!("Address cardinality: {:?}", MicroshaftEmployeeAddress::cardinality());
}
