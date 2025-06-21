#[derive(Debug)]
pub enum Promise<T> {
    Resolved(T),
    Pending,
}

impl<T> Clone for Promise<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Resolved(arg0) => Self::Resolved(arg0.clone()),
            Self::Pending => Self::Pending,
        }
    }
}
