use crate::{DialogEncodingError, Width};

/// Trait for data structures that can be decomposed into and reconstructed from byte cells.
///
/// This trait enables columnar encoding and decoding by defining how a data structure
/// can be broken down into individual byte sequences (cells) and later reconstructed
/// from those same cells. It is the core abstraction that makes zero-copy encoding
/// and decoding possible.
///
/// # Columnar Representation
///
/// Types implementing `Cellular` represent their data as a sequence of byte cells,
/// where each cell corresponds to a field, element, or component of the structure.
/// This columnar approach enables efficient encoding with deduplication and zero-copy
/// reading.
///
/// # Example
///
/// ```rust
/// use dialog_encoding::{Cellular, Width, DialogEncodingError};
///
/// struct Person<'a> {
///     name: &'a [u8],
///     age: &'a [u8],
///     email: &'a [u8],
/// }
///
/// impl<'a> Cellular<'a> for Person<'a> {
///     fn cell_width() -> Width {
///         Width::Bounded(3)
///     }
///
///     fn cells(&'a self) -> impl Iterator<Item = &'a [u8]> {
///         [self.name, self.age, self.email].into_iter()
///     }
///
///     fn try_from_cells<I>(cells: &mut I) -> Result<Self, DialogEncodingError>
///     where
///         I: Iterator<Item = &'a [u8]>,
///     {
///         let name = cells.next().unwrap();
///         let age = cells.next().unwrap();
///         let email = cells.next().unwrap();
///         Ok(Person { name, age, email })
///     }
/// }
/// ```
pub trait Cellular<'a>: Sized {
    fn cell_width() -> Width;

    /// Decomposes this data structure into an iterator of byte cells.
    ///
    /// Each cell represents a component of the data structure as a byte slice.
    /// The order of cells returned by this method must match the order expected
    /// by [`try_from_cells`](Self::try_from_cells).
    ///
    /// # Returns
    ///
    /// An iterator that yields byte slices representing the individual cells
    /// that make up this data structure.
    fn cells(&self) -> impl Iterator<Item = &[u8]>;

    /// Reconstructs a data structure from an iterator of byte cells.
    ///
    /// This method consumes cells in the same order they were produced by
    /// [`cells`](Self::cells) and reconstructs the original data structure.
    /// Since this is used for zero-copy decoding, the cells are slices directly
    /// into the encoded buffer.
    ///
    /// # Arguments
    ///
    /// * `cells` - An iterator of byte slices representing the data structure's cells
    ///
    /// # Returns
    ///
    /// Returns the reconstructed data structure on success, or a [`DialogEncodingError`]
    /// if the cells cannot be properly interpreted (e.g., insufficient cells, invalid data).
    ///
    /// # Contract
    ///
    /// Implementations must ensure that for any value `x` of type `Self`:
    /// ```text
    /// let cells: Vec<_> = x.cells().collect();
    /// let reconstructed = Self::try_from_cells(cells.into_iter())?;
    /// assert_eq!(x, reconstructed); // If Self implements PartialEq
    /// ```
    fn try_from_cells<I>(cells: &mut I) -> Result<Self, DialogEncodingError>
    where
        I: Iterator<Item = &'a [u8]>;
}

pub trait CellularToOwned<'a>: Cellular<'a> {
    type Owned: From<Self> + 'static;
    fn to_owned(&self) -> Self::Owned;
}
