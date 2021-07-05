use defaults::{bitmask, bitshift};
use std::{ops::RangeBounds, mem::MaybeUninit};

pub mod defaults;

pub type SortedTable<T> = Vec<(u128, T)>;

pub struct OidMap<T, const N: usize> {
    pub root: [SortedTable<T>; N],
}

macro_rules! shiftedkey {
    ($val:expr) => {
        (($val & Self::MASK) >> Self::SHIFT) as usize
    };
}

pub struct OidMapIterator<'a, T, const N: usize> {
    pub start_key_index: usize, // inclusive
    pub end_key_index: usize, // not inclusive
    pub map: &'a OidMap<T, N>,
    pub within_table_index: usize,
}

impl<'a, T, const N: usize> Iterator for OidMapIterator<'a, T, N> {
    type Item = (&'a u128, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.start_key_index >= self.end_key_index {
                return None;
            }
            let entry = &self.map.root[self.start_key_index];
            match entry.get(self.within_table_index) {
                Some((k, ret)) => {
                    self.within_table_index += 1;
                    return Some((k, ret));
                }
                None => {
                    // reached end of this table. advance:
                    self.within_table_index = 0;
                    self.start_key_index += 1;
                }
            }
        }
    }
}

impl<T, const N: usize> OidMap<T, N> {
    const MASK: u128 = bitmask(N);
    const SHIFT: usize = bitshift(N);

    /// each table gets a pre-allocated size.
    /// use this if you know that at the end of
    /// all of your insertions, your average table size
    /// will be roughly `m`. ie: if you know
    /// you have 10000 entries to insert, and you are using B10,
    /// that means we have 1024 tables. so we know that if the 10000
    /// entries are sparse, then each table should have about 10
    /// entries, and thus we can pre-allocate the table sizes by setting
    /// m = 10.
    pub fn new_with_prealloc(m: usize) -> OidMap<T, N> {
        let mut arr: MaybeUninit<[Vec<(u128, T)>; N]> = MaybeUninit::uninit();
        let mut ptr_i = arr.as_mut_ptr() as *mut Vec<(u128, T)>;
        let root = unsafe {
            for _ in 0..N {
                let val = Vec::with_capacity(m);
                ptr_i.write(val);
                ptr_i = ptr_i.add(1);
            }
            arr.assume_init()
        };
        Self { root }
    }

    /// `m` is the number of objects you expect to have inserted.
    /// we just calculate a reasonable preallocation amount, which is:
    /// (`m` / `number of tables`) * 1.15, ie: find the average
    /// number of objects in a table, and multiply that number by 10%,
    /// and use that as the default allocation amount. eg:
    /// if you have 10000 entries to insert, and you are using B10,
    /// then the average table size will be 10000 / 1024 = 9.765.
    /// we round up to 10, and then multiply by 10% to get 11.
    pub fn new_with_prealloc_m_objects(m: usize) -> OidMap<T, N> {
        Self::new_with_prealloc_m_objects_and_percent(m, 1.15)
    }

    /// like `new_with_prealloc_m_objects` but you specify
    /// a percentage to use. otherwise, call `new_with_prealloc_m_objects`
    /// which uses a default of 15%.
    /// NOTE: `pct` should be 1 + percent. ie: if you want an additional 10%,
    /// you should pass 1.1
    pub fn new_with_prealloc_m_objects_and_percent(m: usize, pct: f64) -> OidMap<T, N> {
        let avg = (m as f64) / (N as f64);
        let avg = avg + 1.0;
        let avg = avg * pct;
        Self::new_with_prealloc(avg as usize)
    }

    #[inline(always)]
    pub fn get_table_from_key(&self, key: &u128) -> &SortedTable<T> {
        let index = shiftedkey!(*key);
        let table = &self.root[index];
        table
    }

    #[inline(always)]
    pub fn get_table_from_key_mut(&mut self, key: &u128) -> &mut SortedTable<T> {
        let index = shiftedkey!(*key);
        let table = &mut self.root[index];
        table
    }

    #[inline(always)]
    pub fn binary_search_table_for_key(table: &SortedTable<T>, key: &u128) -> Result<usize, usize> {
        table.binary_search_by(|(k, _)| k.cmp(key))
    }

    /// Only use this for debugging...
    /// TODO: I should add a debug only cfg attribute or something...
    pub fn table_report(&self) {
        let mut unused_space = 0;
        let mut avg_len = 0;
        for (i, table) in self.root.iter().enumerate() {
            let len = table.len();
            let cap = table.capacity();
            println!("T_{} size: {}, alloc: {}", i, len, cap);
            unused_space += cap - len;
            avg_len += len;
        }
        let entry_size = std::mem::size_of::<(u128, T)>();
        let avg_len = (avg_len as f64) / (self.root.len() as f64);
        println!("Avg len: {}", avg_len);
        println!("Size of entries: {}", entry_size);
        println!("Wasted space: {}", entry_size * unused_space);
    }

    pub fn len(&self) -> usize {
        self.root.iter().map(|e| e.len()).sum()
    }

    pub fn capacity(&self) -> usize {
        self.root.iter().map(|e| e.capacity()).sum()
    }

    pub fn contains_key(&self, key: &u128) -> bool {
        let table = self.get_table_from_key(key);
        let found = Self::binary_search_table_for_key(table, key);
        found.is_ok()
    }

    pub fn get(&self, key: &u128) -> Option<&T> {
        let table = self.get_table_from_key(key);
        let found = Self::binary_search_table_for_key(table, key);
        let entry_at = match found {
            Ok(i) => i,
            Err(_) => { return None;}
        };
        Some(&table[entry_at].1)
    }

    pub fn get_mut(&mut self, key: &u128) -> Option<&mut T> {
        let table = self.get_table_from_key_mut(key);
        let found = Self::binary_search_table_for_key(table, key);
        let entry_at = match found {
            Ok(i) => i,
            Err(_) => { return None;}
        };
        Some(&mut table[entry_at].1)
    }

    pub fn insert(&mut self, key: u128, t: T) {
        let table = self.get_table_from_key_mut(&key);
        let found = Self::binary_search_table_for_key(table, &key);
        let insert_at = match found {
            Ok(i) |
            Err(i) => i
        };
        let mut i = table.len();
        // arbitrary: if table is relatively large, we can try
        // to optimize by checking if the insertion point
        // is towards the beginning of the table. if so, we use
        // insert, otherwise if the entry is towards the end of
        // the table, then we push and swap
        if i >= 100 {
            if insert_at < (i / 2) {
                table.insert(insert_at, (key, t));
                return;
            }
        }

        table.push((key, t));
        // believe it or not, on average this is faster than
        // doing table.insert(insert_at, t);
        // I think its because of alignment issues.
        // also obviously this is only faster when the size of this table
        // is relatively small, and/or we are inserting close to the end.
        // obviously if the table is large, and we are inserting at the beginning,
        // then this would be slower than doing table.insert...
        // TODO: maybe calculate if insert_at is close to beginning
        // of table, and if so, use table.insert?
        while i > insert_at {
            table.swap(i, i - 1);
            i -= 1;
        }
    }

    pub fn range<'a, R: RangeBounds<u128>>(&'a self, range: R) -> OidMapIterator<'a, T, N> {
        let range_start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i + 1,
            std::ops::Bound::Unbounded => {
                0
            }
        };
        let start_index = shiftedkey!(range_start);
        OidMapIterator {
            start_key_index: start_index,
            // TODO: this is inaccurate. it might work on most cases,
            // but i think its possible for a range to cross table boundaries.
            // currently, we assume table iteration only occurs on one table...
            end_key_index: start_index + 1,
            map: self,
            within_table_index: 0,
        }
    }

    pub fn iter<'a>(&'a self) -> OidMapIterator<'a, T, N> {
        OidMapIterator {
            start_key_index: 0,
            end_key_index: N,
            map: self,
            within_table_index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use defaults::B13;

    #[test]
    fn it_works() {
        let mut map = OidMap::<u128, B13>::default();
        assert_eq!(map.len(), 0);
        assert!(map.get(&2).is_none());
        map.insert(2, 2);
        assert_eq!(map.get(&2).unwrap(), &2);
        assert_eq!(map.len(), 1);

        // what about BEEG keys
        map.insert(u128::MAX, 2);
        assert_eq!(map.get(&u128::MAX).unwrap(), &2);
        assert_eq!(map.len(), 2);
    }
}
