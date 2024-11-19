//! Builder utility.
//!

use std::iter::{Chain, Flatten};

use adbc_core::options::OptionValue;

/// An iterator over the builder options.
pub struct BuilderIter<T, const COUNT: usize>(
    #[allow(clippy::type_complexity)]
    Chain<
        Flatten<<[Option<(T, OptionValue)>; COUNT] as IntoIterator>::IntoIter>,
        <Vec<(T, OptionValue)> as IntoIterator>::IntoIter,
    >,
);

impl<T, const COUNT: usize> BuilderIter<T, COUNT> {
    pub(crate) fn new(
        fixed: [Option<(T, OptionValue)>; COUNT],
        other: Vec<(T, OptionValue)>,
    ) -> Self {
        Self(fixed.into_iter().flatten().chain(other))
    }
}

impl<T, const COUNT: usize> Iterator for BuilderIter<T, COUNT> {
    type Item = (T, OptionValue);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
