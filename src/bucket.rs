use page::Page;
use disk;

use std::fmt::Debug;
use serde::de::DeserializeOwned;
use util::deserialize_kv;

#[derive(Debug)]
pub struct Bucket<K, V> {
    num_records: usize,
    records: Vec<(K, V)>,
}

impl<K, V> Bucket<K, V>
    where K: DeserializeOwned + Debug,
          V: DeserializeOwned + Debug {
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
}
