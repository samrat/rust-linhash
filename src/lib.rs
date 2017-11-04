use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use std::marker::PhantomData;
use std::fmt::Debug;

// TODO: implement remove

extern crate serde;
extern crate bincode;
mod util;
mod page;
mod disk;

use page::Page;
use disk::{DbFile,SearchResult};
use serde::ser::Serialize;
use serde::de::DeserializeOwned;

/// Linear Hashtable
pub struct LinHash<K, V> {
    buckets: DbFile,
    nbits: usize,               // no of bits used from hash
    nitems: usize,              // number of items in hashtable
    nbuckets: usize,            // number of buckets
    phantom: (PhantomData<K>, PhantomData<V>),
}

impl<K, V> LinHash<K, V>
    where K: PartialEq + Hash + Clone + Serialize + DeserializeOwned + Debug,
          V: Clone + DeserializeOwned + Serialize + Debug {
    /// "load"(utilization of data structure) needed before the
    /// hashmap needs to grow.
    const THRESHOLD: f32 = 0.8;

    /// Creates a new Linear Hashtable.
    pub fn new(filename: &str) -> LinHash<K, V> {
        let nbits = 1;
        let nitems = 0;
        let nbuckets = 2;
        LinHash {
            buckets: DbFile::new::<K,V>(filename),
            nbits: nbits,
            nitems: nitems,
            nbuckets: nbuckets,
            phantom: (PhantomData, PhantomData),
        }
    }

    pub fn open(filename: &str) -> LinHash<K, V> {
        let mut dbfile = DbFile::new::<K,V>(filename);
        let (nbits, nitems, nbuckets) = dbfile.read_ctrlpage();
        LinHash {
            buckets: dbfile,
            nbits: nbits,
            nitems: nitems,
            nbuckets: nbuckets,
            phantom: (PhantomData, PhantomData),
        }
    }

    fn hash(&self, key: &K) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    /// Which bucket to place the key-value pair in. If the target
    /// bucket does not yet exist, it is guaranteed that the MSB is a
    /// `1`. To find the bucket, the pair should be placed in,
    /// subtract this `1`.
    fn bucket(&self, key: &K) -> usize {
        let hash = self.hash(key);
        let bucket = (hash & ((1 << self.nbits) - 1)) as usize;
        let adjusted_bucket_index =
            if bucket < self.nbuckets {
                bucket
            } else {
                bucket - (1 << (self.nbits-1))
            };

        adjusted_bucket_index
    }

    /// Returns true if the `load` exceeds `LinHash::THRESHOLD`
    fn split_needed(&self) -> bool {
        (self.nitems as f32 / (self.buckets.records_per_page * self.nbuckets) as f32) >
            LinHash::<K,V>::THRESHOLD
    }

    /// If necessary, allocates new bucket. If there's no more space
    /// in the buckets vector(ie. n > 2^i), increment number of bits
    /// used(i).

    /// Note that, the bucket split is not necessarily the one just
    /// inserted to.
    fn maybe_split(&mut self) -> bool {
        if self.split_needed() {
            self.nbuckets += 1;

            self.buckets.allocate_new_bucket::<K,V>();
            if self.nbuckets > (1 << self.nbits) {
                self.nbits += 1;
            }

            // Take index of last item added(the `push` above) and
            // subtract the 1 at the MSB position. eg: after bucket 11
            // is added, bucket 01 needs to be split
            let bucket_to_split =
                (self.nbuckets-1) ^ (1 << (self.nbits-1));
            println!("nbits: {} nitems: {} nbuckets: {} splitting {}",
                     self.nbits, self.nitems, self.nbuckets, bucket_to_split);

            let key_size = mem::size_of::<K>();
            let val_size = mem::size_of::<V>();

            // Replace the bucket to split with a fresh, empty
            // page. And get a list of all records stored in the bucket
            let old_bucket_records =
                self.buckets.clear_bucket::<K,V>(bucket_to_split);

            println!("{:?}", old_bucket_records);
            // Re-hash all records in old_bucket. Ideally, about half
            // of the records will go into the new bucket.
            for &(ref k, ref v) in old_bucket_records.iter() {
                self.reinsert(k.clone(), v.clone());
            }

            return true
        }

        false
    }

    /// Does the hashmap contain a record with key `key`?
    pub fn contains(&mut self, key: K) -> bool {
        match self.get(key) {
            Some(_) => true,
            None => false,
        }
    }

    /// Update the mapping of record with key `key`.
    pub fn update(&mut self, key: K, val: V) -> bool {
        let bucket_index = self.bucket(&key);
        match self.buckets.search_bucket::<K,V>(bucket_index, key.clone()) {
            SearchResult { page_id, row_num, val: old_val } => {
                match (page_id, row_num, old_val) {
                    (Some(page_id), Some(row_num), Some(_)) => {
                        println!("update: {:?}", (page_id, row_num, key.clone(), val.clone()));
                        self.buckets.write_record(page_id, row_num, key, val);
                        true
                    }
                    _ => false,
                }
            },
            _ => false,
        }
    }

    /// Insert (key,value) pair into the hashtable.
    pub fn put(&mut self, key: K, val: V) {
        println!("[put] {:?}", (key.clone(), val.clone()));
        let bucket_index = self.bucket(&key);
        match self.buckets.search_bucket::<K,V>(bucket_index, key.clone()) {
            SearchResult { page_id, row_num, val: old_val } => {
                println!("{:?}", (page_id, row_num, old_val.clone()));
                match (page_id, row_num, old_val) {
                    // new insert
                    (Some(page_id), Some(pos), None) => {
                        self.buckets.write_record_incr(page_id, pos, key, val);
                        self.nitems += 1;
                    },
                    // case for update
                    (Some(page_id), Some(pos), Some(_old_val)) =>
                        self.buckets.write_record(page_id, pos, key, val),
                    // new insert, in overflow page
                    (None, None, None) => {
                        println!("allocating new buffer for bucket: {}", bucket_index);
                        // overflow
                        let overflow_index = self.buckets.allocate_overflow::<K,V>(bucket_index);
                        self.buckets.put(overflow_index, key, val);
                        self.nitems += 1;
                    },
                    _ => panic!("impossible case"),
                }
            },
        }
        // self.maybe_split();
        self.buckets.write_ctrlpage((self.nbits, self.nitems, self.nbuckets));
    }

    /// Re-insert (key, value) pair after a split
    fn reinsert(&mut self, key: K, val: V) {
        let bucket_index = self.bucket(&key);
        self.buckets.put(bucket_index, key, val);

        // self.maybe_split();
    }

    /// Lookup `key` in hashtable
    pub fn get(&mut self, key: K) -> Option<V> {
        let bucket_index = self.bucket(&key);
        match self.buckets.search_bucket::<K,V>(bucket_index, key) {
            SearchResult { page_id, row_num, val } => val,
            _ => None,
        }
    }

    // Removes record with `key` in hashtable.
    // pub fn remove(&mut self, key: K) -> Option<V> {
    //     let bucket_index = self.bucket(&key);
    //     let index_to_delete = self.search_bucket(bucket_index, &key);

    //     // Delete item from bucket
    //     match index_to_delete {
    //         Some(x) => Some(self.buckets[bucket_index].remove(x).1),
    //         None => None,
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use LinHash;
    use std::fs;

    #[test]
    fn all_ops() {
        let mut h : LinHash<String, i32> = LinHash::new("/tmp/test_all_ops");
        h.put(String::from("hello"), 12);
        h.put(String::from("there"), 13);
        h.put(String::from("foo"), 42);
        h.put(String::from("bar"), 11);
        h.put(String::from("bar"), 22);
        h.update(String::from("foo"), 84);

        assert_eq!(h.get(String::from("hello")), Some(12));
        assert_eq!(h.get(String::from("there")), Some(13));
        assert_eq!(h.get(String::from("foo")), Some(84));
        assert_eq!(h.get(String::from("bar")), Some(22));

        // assert_eq!(h.update(String::from("doesn't exist"), 99), false);
        assert_eq!(h.contains(String::from("doesn't exist")), false);
        assert_eq!(h.contains(String::from("hello")), true);

        fs::remove_file("/tmp/test_all_ops");
    }

    #[test]
    fn test_persistence() {
        let mut h : LinHash<String, i32> = LinHash::new("/tmp/test_persistence");
        h.put(String::from("hello"), 12);
        h.put(String::from("world"), 13);
        h.put(String::from("linear"), 144);
        h.put(String::from("hashing"), 999);

        // This reloads the file and creates a new hashtable
        let mut h2 : LinHash<String, i32> = LinHash::open("/tmp/test_persistence");
        assert_eq!(h2.get(String::from("hello")), Some(12));

        fs::remove_file("/tmp/test_persistence");
    }
}
