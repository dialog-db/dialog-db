use thiserror::Error;

/// Errors that can occur during encoding and decoding operations.
///
/// This enum represents the various failure modes that can occur when working
/// with the dialog-encoding library. All errors are recoverable and provide
/// detailed information about what went wrong.
#[derive(Error, Debug)]
pub enum DialogEncodingError {
    /// Failed to decode a LEB128-encoded integer from the buffer.
    ///
    /// This error occurs when the buffer contains malformed LEB128 data,
    /// such as incomplete sequences or invalid byte patterns. LEB128 integers
    /// are used throughout the encoding format for lengths, offsets, and indices.
    #[error("Failed to decode an integer: {0}")]
    IntegerDecode(leb128::read::Error),

    /// Failed to perform a buffer I/O operation during decoding.
    ///
    /// This error occurs when there are issues reading from or seeking within
    /// the encoded buffer, such as attempting to read beyond the buffer's bounds
    /// or encountering other I/O-related failures.
    #[error("Failed to decode a buffer: {0}")]
    BufferDecode(std::io::Error),
}

impl From<leb128::read::Error> for DialogEncodingError {
    fn from(value: leb128::read::Error) -> Self {
        DialogEncodingError::IntegerDecode(value)
    }
}

impl From<std::io::Error> for DialogEncodingError {
    fn from(value: std::io::Error) -> Self {
        DialogEncodingError::BufferDecode(value)
    }
}
