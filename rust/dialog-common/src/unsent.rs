use tokio::io::AsyncRead;

#[repr(transparent)]
pub struct Unsent<T>(T);

impl<T> Unsent<T> {
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
unsafe impl<T> Send for Unsent<T> {
    // SAFETY: In wasm32-unknown-unknown targets (browsers), there is no actual
    // thread spawning or cross-thread data movement possible. This
    // implementation assumes single-threaded execution model.
}

impl<T> AsyncRead for Unsent<T>
where
    T: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let pinned = std::pin::pin!(&mut self.0);
        pinned.poll_read(cx, buf)
    }
}
