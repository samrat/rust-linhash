use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt::{Display, Debug};

/// Linear Hashtable
#[derive(Debug)]
pub struct LinHash<K, V> {
    buckets: Vec<Vec<(K,V)>>,
    i: usize,                   // no of bits used from hash
    r: usize,                   // number of items in hashtable
    n: usize,                   // number of buckets
}

impl<K, V> LinHash<K, V>
    where K: PartialEq + Hash + Display + Debug + Clone,
          V: Clone + Debug {

    const THRESHOLD: f32 = 0.8;

    /// Creates a new Linear Hashtable.
    pub fn new() -> LinHash<K, V> {
        let i = 1;
        let r = 0;
        let n = 2;
        LinHash {
            buckets: vec![vec![]; n],
            i: i,
            r: r,
            n: n,
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
        let bucket = (hash & ((1 << self.i) - 1)) as usize;
        let adjusted_bucket_index =
            if bucket < self.n {
                bucket
            } else {
                bucket - (1 << (self.i-1))
            };

        adjusted_bucket_index
    }

    /// Returns true if the average number of records per bucket(r/n)
    /// exceeds `threshold`
    fn split_needed(&self) -> bool {
        (self.r as f32 / self.n as f32) > LinHash::<K,V>::THRESHOLD
    }

    /// If necessary, allocates new bucket. If there's no more space
    /// in the buckets vector(ie. n > 2^i), increment number of bits
    /// used(i).

    /// Note that, the bucket split is not necessarily the one just
    /// inserted to.
    fn maybe_split(&mut self) -> bool {
        if self.split_needed() {
            self.n += 1;
            self.buckets.push(vec![]);
            if self.n > (1 << self.i) {
                self.i += 1;
            }
            
            // Take index of last item added(the `push` above) and
            // subtract the 1 at the MSB position. eg: after bucket 11
            // is added, bucket 01 needs to be split
            let bucket_to_split = (self.n-1) ^ (1 << (self.i-1));

            // Copy the bucket we are about to split
            let old_bucket = self.buckets[bucket_to_split].clone();
            // And allocate a new vector to replace it
            self.buckets[bucket_to_split] = vec![];

            // Re-hash all records in old_bucket. Ideally, about half
            // of the records will go into the new bucket.
            for (k, v) in old_bucket {
                self.put(k, v);
            }

            return true
        }

        false
    }

    pub fn contains(&mut self, key: K) -> bool {
        match self.get(key) {
            Some(_) => true,
            None => false,
        }
    }

    fn search_bucket(&self, bucket_index: usize, key: &K) -> Option<usize> {
        let bucket = &self.buckets[bucket_index];
        for (i, &(ref k, ref _v)) in bucket.iter().enumerate() {
            if k.clone() == key.clone() {
                return Some(i);
            }
        }
        None
    }

    pub fn update(&mut self, key: K, val: V) -> bool {
        let bucket_index = self.bucket(&key);
        let index_to_update = self.search_bucket(bucket_index, &key);

        match index_to_update {
            Some(x) => {
                self.buckets[bucket_index][x] = (key, val);
                true
            },
            None => false,
        }
    }

    /// Insert (key,value) pair into the hashtable.
    pub fn put(&mut self, key: K, val: V) {
        let bucket_index = self.bucket(&key);
        match self.search_bucket(bucket_index, &key) {
            Some(_) => {
                self.update(key, val);
            },
            None => {
                self.buckets[bucket_index].push((key, val));
                self.r += 1;
            },
        }

        self.maybe_split();
    }

    /// Lookup `key` in hashtable
    pub fn get(&self, key: K) -> Option<V> {
        let bucket_index = self.bucket(&key);
        let bucket = &self.buckets[bucket_index];
        for &(ref k, ref v) in bucket {
            if k.clone() == key {
                return Some(v.clone())
            }
        }
        None
    }

    /// Removes record with `key` in hashtable.
    pub fn remove(&mut self, key: K) -> Option<V> {
        let bucket_index = self.bucket(&key);
        let index_to_delete = self.search_bucket(bucket_index, &key);

        // Delete item from bucket
        match index_to_delete {
            Some(x) => Some(self.buckets[bucket_index].remove(x).1),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use LinHash;
    
    #[test]
    fn all_ops() {
        let mut h : LinHash<&str, i32> = LinHash::new();
        h.put("hello", 12);
        h.put("there", 13);
        h.put("foo", 42);
        h.put("bar", 11);
        h.put("bar", 22);
        h.remove("there");
        h.update("foo", 84);

        assert_eq!(h.get("hello"), Some(12));
        assert_eq!(h.get("there"), None);
        assert_eq!(h.get("foo"), Some(84));
        assert_eq!(h.get("bar"), Some(22));
        assert_eq!(h.update("doesn't exist", 99), false);
        assert_eq!(h.contains("doesn't exist"), false);
        assert_eq!(h.contains("hello"), true);
    }
}
