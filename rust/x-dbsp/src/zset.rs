use std::collections::BTreeMap;

use x_common::ConditionalSend;

pub trait ZSetElement: PartialOrd + Ord + PartialEq + Eq + Clone + ConditionalSend {}

impl<T> ZSetElement for T where T: PartialOrd + Ord + PartialEq + Eq + Clone + ConditionalSend {}

#[derive(Clone, Default)]
pub struct ZSet<T>
where
    T: ZSetElement,
{
    elements: BTreeMap<T, isize>,
}

impl<T> ZSet<T>
where
    T: ZSetElement,
{
    /// Directly assigns a multiplicity to the given entry
    pub fn set(&mut self, entry: &T, multiplicity: isize) {
        if let Some(current_multiplicity) = self.elements.get_mut(entry) {
            *current_multiplicity = multiplicity;
        } else {
            self.elements.insert(entry.clone(), multiplicity);
        }
    }

    /// Look up the multiplicity of an element, defaulting to 0 in cases for as-yet
    /// untracked elements.
    pub fn get(&self, entry: &T) -> isize {
        self.elements.get(entry).copied().unwrap_or_default()
    }

    /// Adds the given multiplicity to the current multiplicity (or zero, if the entry is not
    /// yet recorded), records the new value and returns it.
    pub fn add(&mut self, entry: &T, multiplicity: isize) -> isize {
        if let Some(current_multiplicity) = self.elements.get_mut(entry) {
            *current_multiplicity += multiplicity;
            *current_multiplicity
        } else {
            self.elements.insert(entry.clone(), multiplicity);
            multiplicity
        }
    }

    /// Subtracts the given multiplicity from the current multiplicity (or zero, if the entry is not
    /// yet recorded), records the new value and returns it.
    pub fn subtract(&mut self, entry: &T, multiplicity: isize) -> isize {
        if let Some(current_multiplicity) = self.elements.get_mut(entry) {
            *current_multiplicity -= multiplicity;
            *current_multiplicity
        } else {
            self.elements.insert(entry.clone(), -multiplicity);
            -multiplicity
        }
    }

    /// Multiplies the given multiplicity by the current multiplicity (or zero, if the entry is not
    /// yet recorded), records the new value and returns it.
    pub fn multiply(&mut self, entry: &T, multiplicity: isize) -> isize {
        if let Some(current_multiplicity) = self.elements.get_mut(entry) {
            *current_multiplicity *= multiplicity;
            *current_multiplicity
        } else {
            0
        }
    }

    /// Increment the multiplicity of an element by one, setting it to 1 if it does not
    /// already exist in the [ZSet]
    pub fn increment(&mut self, entry: &T) {
        if let Some(multiplicity) = self.elements.get_mut(entry) {
            *multiplicity += 1;
        } else {
            self.elements.insert(entry.clone(), 1);
        }
    }

    /// Decrement the multiplicity of an element, setting it to -1 if it does not
    /// already exist in the [ZSet]
    pub fn decrement(&mut self, entry: &T) {
        if let Some(multiplicity) = self.elements.get_mut(entry) {
            *multiplicity -= 1;
        } else {
            self.elements.insert(entry.clone(), -1);
        }
    }

    /// Compute and return the sum (by adding multiplicities) of this [ZSet]
    /// and another one
    pub fn sum(&self, other: &ZSet<T>) -> ZSet<T> {
        let mut base = self.clone();

        for (entry, multiplicity) in &other.elements {
            if let Some(base_multiplicity) = base.elements.get_mut(entry) {
                *base_multiplicity += *multiplicity;
            } else {
                base.elements.insert(entry.clone(), *multiplicity);
            }
        }

        base
    }

    /// Compute and return the difference (by substracting multiplicities) of this [ZSet]
    /// and another one.
    pub fn difference(&self, other: &ZSet<T>) -> ZSet<T> {
        let mut base = self.clone();

        for (entry, multiplicity) in &other.elements {
            if let Some(base_multiplicity) = base.elements.get_mut(entry) {
                *base_multiplicity -= *multiplicity;
            } else {
                base.elements.insert(entry.clone(), -*multiplicity);
            }
        }

        base
    }

    /// Bulk insert another [ZSet] into this one (mutating it in-place). Multiplicities
    /// will be the sum of the values in both sets
    pub fn merge(&mut self, other: ZSet<T>) {
        for (entry, multiplicity) in other.elements {
            if let Some(base_multiplicity) = self.elements.get_mut(&entry) {
                *base_multiplicity += multiplicity;
            } else {
                self.elements.insert(entry, multiplicity);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ZSet;

    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
    struct Element(pub usize);

    #[test]
    fn it_can_increment_and_decrement_the_multiplicity_of_elements() {
        let mut set = ZSet::default();

        let element_zero = Element(0);
        let element_one = Element(1);

        set.increment(&element_zero);

        assert_eq!(set.get(&element_zero), 1);
        assert_eq!(set.get(&element_one), 0);

        set.increment(&element_zero);
        set.increment(&element_one);

        assert_eq!(set.get(&element_zero), 2);
        assert_eq!(set.get(&element_one), 1);

        set.decrement(&element_zero);

        assert_eq!(set.get(&element_zero), 1);
        assert_eq!(set.get(&element_one), 1);
    }

    #[test]
    fn it_can_produce_the_sum_of_two_sets() {
        let mut set_one = ZSet::default();
        let mut set_two = ZSet::default();

        let element_zero = Element(0);
        let element_one = Element(1);
        let element_two = Element(2);

        set_one.set(&element_zero, 1);
        set_one.set(&element_one, 2);

        set_two.set(&element_one, 1);
        set_two.set(&element_two, 2);

        let set_three = set_one.sum(&set_two);

        assert_eq!(set_three.get(&element_zero), 1);
        assert_eq!(set_three.get(&element_one), 3);
        assert_eq!(set_three.get(&element_two), 2);
    }

    #[test]
    fn it_can_produce_the_difference_of_two_sets() {
        let mut set_one = ZSet::default();
        let mut set_two = ZSet::default();

        let element_zero = Element(0);
        let element_one = Element(1);
        let element_two = Element(2);

        set_one.set(&element_zero, 1);
        set_one.set(&element_one, 2);

        set_two.set(&element_one, 1);
        set_two.set(&element_two, 2);

        let set_three = set_one.difference(&set_two);

        assert_eq!(set_three.get(&element_zero), 1);
        assert_eq!(set_three.get(&element_one), 1);
        assert_eq!(set_three.get(&element_two), -2);
    }

    #[test]
    fn it_can_merge_one_set_into_another() {
        let mut set_one = ZSet::default();
        let mut set_two = ZSet::default();

        let element_zero = Element(0);
        let element_one = Element(1);
        let element_two = Element(2);

        set_one.set(&element_zero, 1);
        set_one.set(&element_one, 2);

        set_two.set(&element_one, 1);
        set_two.set(&element_two, 2);

        set_one.merge(set_two);

        assert_eq!(set_one.get(&element_zero), 1);
        assert_eq!(set_one.get(&element_one), 3);
        assert_eq!(set_one.get(&element_two), 2);
    }
}
