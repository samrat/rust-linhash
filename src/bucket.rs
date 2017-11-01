use page::Page;
use disk;

use std::fmt::Debug;
use serde::de::DeserializeOwned;
use util::deserialize_kv;
use std::ops::{Index,IndexMut};
use std::slice;
use std::ops::Deref;

#[derive(Clone, Debug)]
pub struct Bucket<K, V> {
    num_records: usize,
    records: Vec<(K, V)>,
}

impl<K, V> Bucket<K, V>
    where K: DeserializeOwned,
          V: DeserializeOwned {
    pub fn new() -> Bucket<K, V> {
        Bucket {
            num_records: 0,
            records: Vec::new(),
        }
    }

    pub fn from_page(mut p: Page) -> Bucket<K, V> {
        let num_records = p.num_tuples;
        let mut records = Vec::with_capacity(num_records);

        for i in 0..num_records {
            let (k, v) = p.read_tuple(i);
            let (dk, dv) : (K, V) = deserialize_kv(k, v);
            records.push((dk, dv));
        }

        Bucket {
            num_records: num_records,
            records: records,
        }
    }

    pub fn push(&mut self, (key, val): (K, V)) {
        self.records.push((key, val));
    }

    pub fn remove(&mut self, index: usize) -> (K, V) {
        self.records.remove(index)
    }
}

impl<K,V> Deref for Bucket<K,V> {
    type Target = [(K,V)];

    fn deref(&self) -> &[(K,V)] {
        &self.records
    }
}

impl<K,V> Index<usize> for Bucket<K,V> {
    type Output = (K,V);
    fn index<'a>(&'a self, idx: usize) -> &'a (K, V) {
        return &self.records[idx];
    }
}

impl<K,V> IndexMut<usize> for Bucket<K,V> {
    fn index_mut<'a>(&'a mut self, idx: usize) -> &'a mut (K,V) {
        return self.records.index_mut(idx);
    }
}
