Always import types and refer to them by name. Never use qualified paths like `crate::Foo`, `dialog_capability::Bar`, or `super::Baz` in code bodies.

Instead, add `use` imports at the top of the file/module and refer by short name.

Never use inner `use` statements inside functions or blocks. All imports belong at the top of the file or module.

Exception: capability effect types can be referred to by module path for clarity, e.g. `memory::Resolve`, `archive::Get`, `storage_fx::Load`.
