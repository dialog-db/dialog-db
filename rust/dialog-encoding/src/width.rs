use std::ops::Add;

pub enum Width {
    Bounded(usize),
    Unbounded,
}

impl Add for Width {
    type Output = Width;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Width::Bounded(l), Width::Bounded(r)) => Width::Bounded(l + r),
            _ => Width::Unbounded,
        }
    }
}
