//! Implementation of a lock-free, atomic hash table.
//!
//! This crate provides a high-performance implementation of a completely
//! lock-free (no mutexes, no spin-locks, or the alike) hash table.
//!
//! The only instruction we use is CAS, which allows us to atomically update
//! the table.
//!
//! # Design
//!
//! It is structured as a 256-radix tree with a pseudorandom permutation
//! applied to the key.  Contrary to open addressing, this approach is entirely
//! lock-free and need not reallocation.
//!
//! The permutation is a simple table+XOR based length-padded function, which
//! is applied to avoid excessive depth (this is what makes it a "hash table").
//!
//! See [this blog post](https://ticki.github.io/blog/an-atomic-hash-table/)
//! for details.

use crossbeam_epoch::{Guard, Owned};
use std::fmt::Display;
use std::hash::Hash;

use crate::sponge::Sponge;
use crate::table::{Bucket, Entry, Table};

/// A lock-free, concurrent hash map.
pub struct NestedMap<K: Hash + Eq + Display, V> {
    /// The root table of the hash map.
    root: Table<K, V>,
}

impl<'a, K: 'a + Hash + Eq + Display, V: 'a> Default for NestedMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, K: 'a + Hash + Eq + Display, V> NestedMap<K, V> {
    pub fn new() -> Self {
        Self {
            root: Table::default(),
        }
    }

    /// Lookups a key.
    pub fn lookup(&'a self, key: &K, guard: &'a Guard) -> Option<&V> {
        self.root.lookup(key, Sponge::new(&key), guard)
    }

    /// Insert a key with a certain value into the map.
    ///
    /// - Returns `Some(value)` for the given `value` if `key` is already occupied.
    /// - Returns `None` if key was unoccupied.
    pub fn insert(&self, key: K, val: V, guard: &Guard) -> Option<V> {
        let mut sponge = Sponge::new(&key);
        self.root.insert(
            Owned::new(Bucket::Leaf(Entry {
                key,
                value: Some(val),
            }))
            .into_shared(guard),
            &mut sponge,
            guard,
        )
    }

    /// Remove a key from the hash map.
    ///
    /// If any, the removed value is returned.
    pub fn delete(&self, key: &K, guard: &Guard) -> Result<V, ()> {
        self.root.delete(key, &mut Sponge::new(&key), guard)
    }
}
