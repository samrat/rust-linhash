use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use std::fmt::Debug;
use std::path::Path;

// TODO: implement remove

pub mod util;
pub mod page;
pub mod disk;

use page::Page;
use disk::{DbFile,SearchResult};

/// Linear Hashtable
pub struct LinHash {
    buckets: DbFile,
    nbits: usize,               // no of bits used from hash
    nitems: usize,              // number of items in hashtable
    nbuckets: usize,            // number of buckets
    keysize: usize,             // size of keys
    valsize: usize,             // size of vals
}

impl LinHash {
    /// "load factor" needed before the hashmap needs to grow.
    const THRESHOLD: f32 = 0.8;

    /// Creates a new Linear Hashtable.
    pub fn open(filename: &str, keysize: usize, valsize: usize) -> LinHash {
        let file_exists = Path::new(filename).exists();
        let mut dbfile = DbFile::new(filename, keysize, valsize);
        let (nbits, nitems, nbuckets) =
            if file_exists {
                dbfile.read_ctrlpage()
            } else {
                (1, 0, 2)
            };
        println!("{:?}", (nbits, nitems, nbuckets));
        LinHash {
            buckets: dbfile,
            nbits: nbits,
            nitems: nitems,
            nbuckets: nbuckets,
            keysize: keysize,
            valsize: valsize,
        }
    }

    fn hash(&self, key: &[u8]) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    /// Which bucket to place the key-value pair in. If the target
    /// bucket does not yet exist, it is guaranteed that the MSB is a
    /// `1`. To find the bucket, the pair should be placed in,
    /// subtract this `1`.
    fn bucket(&self, key: &[u8]) -> usize {
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
            LinHash::THRESHOLD
    }

    /// If necessary, allocates new bucket. If there's no more space
    /// in the buckets vector(ie. n > 2^i), increment number of bits
    /// used(i).

    /// Note that, the bucket split is not necessarily the one just
    /// inserted to.
    fn maybe_split(&mut self) -> bool {
        if self.split_needed() {
            self.nbuckets += 1;

            self.buckets.allocate_new_bucket();
            if self.nbuckets > (1 << self.nbits) {
                self.nbits += 1;
            }

            // Take index of last item added(the `push` above) and
            // subtract the 1 at the MSB position. eg: after bucket 11
            // is added, bucket 01 needs to be split
            let bucket_to_split =
                (self.nbuckets-1) ^ (1 << (self.nbits-1));
            println!("nbits: {} nitems: {} nbuckets: {} splitting {} and {}",
                     self.nbits, self.nitems, self.nbuckets, bucket_to_split, (self.nbuckets-1));
            // Replace the bucket to split with a fresh, empty
            // page. And get a list of all records stored in the bucket
            let old_bucket_records =
                self.buckets.clear_bucket(bucket_to_split);

            // Re-hash all records in old_bucket. Ideally, about half
            // of the records will go into the new bucket.
            for (k, v) in old_bucket_records.into_iter() {
                let bucket_index = self.bucket(&k);
                self.reinsert(&k, &v);
            }
            return true
        }

        false
    }

    /// Does the hashmap contain a record with key `key`?
    pub fn contains(&mut self, key: &[u8]) -> bool {
        match self.get(key) {
            Some(_) => true,
            None => false,
        }
    }

    /// Update the mapping of record with key `key`.
    pub fn update(&mut self, key: &[u8], val: &[u8]) -> bool {
        let bucket_index = self.bucket(&key);
        match self.buckets.search_bucket(bucket_index, key.clone()) {
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
    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        println!("[put] {:?}", (key.clone(), val.clone()));
        let bucket_index = self.bucket(&key);
        match self.buckets.search_bucket(bucket_index, key.clone()) {
            SearchResult { page_id, row_num, val: old_val } => {
                println!("[put] {:?} {:?}", key, (page_id, row_num, old_val.clone()));
                match (page_id, row_num, old_val) {
                    // new insert
                    (Some(page_id), Some(pos), None) => {
                        self.buckets.write_record_incr(page_id, pos, key, val);
                        self.nitems += 1;
                    },
                    // case for update
                    (Some(page_id), Some(pos), Some(_old_val)) => {
                        panic!("can't use put to reinsert old item: {:?}", (key, val));
                    },
                    // new insert, in overflow page
                    (Some(last_page_id), None, None) => {
                        // overflow
                        let (overflow_page_id, pos) =
                            self.buckets.allocate_overflow(bucket_index, last_page_id);
                        self.put(key, val);
                    },
                    _ => panic!("impossible case"),
                }
            },
        }

        self.maybe_split();
        self.buckets.write_ctrlpage((self.nbits, self.nitems, self.nbuckets));
    }

    /// Re-insert (key, value) pair after a split
    fn reinsert(&mut self, key: &[u8], val: &[u8]) {
        self.put(key, val);
        // correct for nitems increment in `put`
        self.nitems -= 1;
    }

    /// Lookup `key` in hashtable
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let bucket_index = self.bucket(&key);
        match self.buckets.search_bucket(bucket_index, key) {
            SearchResult { page_id, row_num, val } => {
                match val {
                    Some(v) => Some(v),
                    _ => None,
                }
            },
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

    pub fn close(&mut self) {
        self.buckets.write_ctrlpage((self.nbits, self.nitems, self.nbuckets));
        self.buckets.close();
    }
}

#[cfg(test)]
mod tests {
    use LinHash;
    use std::fs;
    use util::*;

    #[test]
    fn all_ops() {
        let mut h = LinHash::open("/tmp/test_all_ops", 32, 4);
        h.put("hello".as_bytes(), &[12]);
        h.put("there".as_bytes(), &[13]);
        h.put("foo".as_bytes(), &[42]);
        h.put("bar".as_bytes(), &[11]);
        h.update("bar".as_bytes(), &[22]);
        h.update("foo".as_bytes(), &[84]);

        assert_eq!(h.get("hello".as_bytes()), Some(vec![12, 0, 0, 0]));
        assert_eq!(h.get("there".as_bytes()), Some(vec![13, 0, 0, 0]));
        assert_eq!(h.get("foo".as_bytes()), Some(vec![84, 0, 0, 0]));
        assert_eq!(h.get("bar".as_bytes()), Some(vec![22, 0, 0, 0]));

        // assert_eq!(h.update(String::from("doesn't exist"), 99), false);
        assert_eq!(h.contains("doesn't exist".as_bytes()), false);
        assert_eq!(h.contains("hello".as_bytes()), true);

        h.close();
        fs::remove_file("/tmp/test_all_ops");
    }

    #[test]
    fn test_persistence() {
        let mut h = LinHash::open("/tmp/test_persistence", 32, 4);
        h.put("hello".as_bytes(), &[12]);
        h.put("world".as_bytes(), &[13]);
        h.put("linear".as_bytes(), &[144]);
        h.put("hashing".as_bytes(), &[255]);
        h.close();

        // This reloads the file and creates a new hashtable
        let mut h2 = LinHash::open("/tmp/test_persistence", 32, 4);
        assert_eq!(h2.get("hello".as_bytes()), Some(vec![12, 0, 0, 0]));

        h2.close();
        fs::remove_file("/tmp/test_persistence");
    }

    // TODO: figure out a better testing strategy for this. This test
    // currently inserts 10,000 records and checks that they are all
    // there.
    #[test]
    fn test_overflow_and_splitting() {
        let mut h = LinHash::open("/tmp/test_overflow_and_splitting", 4, 4);
        for k in 0..10000 {
            h.put(&i32_to_bytearray(k),
                   &i32_to_bytearray(k+1));
        }
        h.close();

        let mut h2 = LinHash::open("/tmp/test_overflow_and_splitting", 4, 4);
        for k in 0..10000 {
            assert_eq!(h2.get(&i32_to_bytearray(k)),
                       Some(i32_to_bytearray(k+1).to_vec()));
        }

        fs::remove_file("/tmp/test_overflow_and_splitting");
    }
}
