use std::hash::Hash;
use std::sync::atomic::Ordering;
use core::ptr;

use arr_macro::arr;
use crossbeam_epoch::{Atomic, Guard, Owned, Shared};

use crate::sponge::Sponge;

pub struct Entry<K: Hash + Eq, V> {
    pub key: K,
    pub value: Option<V>,
}

pub enum Bucket<K: Hash + Eq, V> {
    Leaf(Entry<K, V>),
    Branch(Table<K, V>),
}

/// A table which constructs the NestedHashMap.
pub struct Table<K: Hash + Eq, V> {
    /// The buckets in the table.
    buckets: [Atomic<Bucket<K, V>>; 256],
}

impl<K: Hash + Eq, V> Bucket<K, V> {
    fn get_key(&self) -> Option<&K> {
        match self {
            Bucket::Leaf(entry) => { Some(&entry.key) },
            _ => { None }
        }
    }
    fn into_value(self) -> Result<V, ()> {
        match self {
            Bucket::Leaf(entry) => {
                match entry.value {
                    None => { Err(()) },
                    Some(v) => { Ok(v) }
                }
            },
            _ => { Err(()) }
        }
    }
}


impl<'a, K:'a + Hash + Eq, V: 'a> Table<K, V> {
    /// Create a table containing two particular entries.
    fn with_two_entries(
        entry1: Shared<'a, Bucket<K, V>>, sponge1: &mut Sponge,
        entry2: Shared<'a, Bucket<K, V>>, sponge2: &mut Sponge,
    ) -> Self {
        let mut table = Table::default();

        // Squeeze the two sponges.
        let idx1 = sponge1.squeeze() as usize;
        let idx2 = sponge2.squeeze() as usize;

        if idx1 != idx2 {
            // If it doesn't collide, insert the two entries
            table.buckets[idx1].store(entry1, Ordering::Relaxed);
            table.buckets[idx2].store(entry2, Ordering::Relaxed);
        } else {
            // The two positions from the sponge matched, so we must place another branch.
            table.buckets[idx1 as usize] = Atomic::new(Bucket::Branch(
                Table::with_two_entries(entry1, sponge1, entry2, sponge2)
            ));
        }

        table
    }

    /// Get the value associated with some key, given its sponge.
    pub fn lookup(&'a self, key: &K, mut sponge: Sponge, guard: &'a Guard) -> Option<&'a V>
    {
        let bucket = self.buckets[sponge.squeeze() as usize].load(Ordering::Relaxed, guard);

        match unsafe{ bucket.as_ref() }  {
            None => { None },
            Some(Bucket::Leaf(Entry{key: k, value: val})) => {
                if key == k { 
                    match val {
                        None => { return None; },
                        Some(v) => {return Some(v); }
                    }
                }
                else { None }
            },
            Some(Bucket::Branch(table)) => {
                // The bucket is a branch with another table, so we recurse and look up in said
                // sub-table.
                table.lookup(key, sponge, guard)
            }
        }
    }

    /// Insert a key-value pair into the table, given its sponge.
    ///
    /// - Returns `Some(value)` for the given `value` if `key` is already occupied.
    /// - Returns `None` if key was unoccupied.
    pub fn insert(&'a self, entry: Shared<Bucket<K, V>>, sponge: &mut Sponge, guard: &Guard) -> Option<V> {
        let index = sponge.squeeze() as usize;
        loop {
            // We squeeze the sponge to get the right bucket of our table
            let bucket = self.buckets[index].load(Ordering::Relaxed, guard);

            match unsafe{ bucket.as_ref() } {
                None => {
                    // Try to CAS if bucket is empty
                    match self.buckets[index].compare_and_set(
                        bucket,
                        entry,
                        Ordering::Relaxed,
                        guard
                    ) {
                        Ok(_) => { return None; },
                        Err(_) => { continue; }
                    };
                }
                Some(bucket_) => {
                    match bucket_ {
                        Bucket::Branch(table) => {
                            return table.insert(entry, sponge, guard);
                        },
                        Bucket::Leaf(entry2) =>  {
                            if unsafe{ entry.deref() }.get_key().unwrap() == &entry2.key {
                                match self.buckets[index].compare_and_set(
                                    bucket,
                                    entry,
                                    Ordering::Relaxed,
                                    guard
                                ){
                                    Ok(_) => { 
                                        let old_entry = unsafe{ ptr::read(&*bucket.as_raw()) };
                                        match old_entry.into_value() {
                                            Ok(v) => { return Some(v); },
                                            Err(_) => { return None; }
                                        }
                                    },
                                    Err(_) => { continue; }
                                }
                            }else{
                                let mut sponge2 = Sponge::new(&entry2.key);
                                sponge2.matching(&sponge);
                                match self.buckets[index].compare_and_set(
                                    bucket,
                                    Owned::new(
                                        Bucket::Branch(
                                            Table::with_two_entries(
                                                entry, sponge,
                                                bucket, &mut sponge2
                                                )
                                            )
                                        ),
                                    Ordering::Relaxed,
                                    guard
                                ){
                                    Ok(_) => { return None; },
                                    Err(_) => { continue; }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn delete(&self, key: &K, sponge: &mut Sponge, guard: &Guard) -> Result<V, ()> {
        let index = sponge.squeeze() as usize;
        loop {
            let bucket = self.buckets[index].load(Ordering::Relaxed, guard);
            match unsafe{ bucket.as_ref() } {
                None => { return Err(()); },
                Some(bucket_) => { 
                    match bucket_ {
                        Bucket::Branch(table) => {
                            return table.delete(key, sponge, guard);
                        },
                        Bucket::Leaf(_) => {
                            match self.buckets[index].compare_and_set(
                                bucket,
                                Shared::null(),
                                Ordering::Relaxed,
                                guard
                                ){
                                Ok(_) => {
                                    let old_entry = unsafe{ ptr::read(&*bucket.as_raw()) };
                                    return old_entry.into_value();
                                },
                                Err(_) => { continue; }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<K: Hash + Eq, V> Default for Table<K, V> {
    fn default() -> Self {
        Self {
            buckets: arr![Atomic::null(); 256],
        }
    }
}

