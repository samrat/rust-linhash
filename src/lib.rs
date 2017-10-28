use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt::{Display, Debug};

#[derive(Debug)]
pub struct LinHash<K, V> {
    buckets: Vec<Vec<(K,V)>>,
    i: usize,                   // no of bits used from hash
    r: usize,                   // number of items in hashtable
    n: usize,                   // number of buckets
    threshold: f32,
}

impl<K, V> LinHash<K, V>
    where K: Eq + Hash + Display + Debug + Clone,
          V: Clone + Debug {

    pub fn new() -> LinHash<K, V> {
        let i = 1;
        let r = 0;
        let n = 2;
        LinHash {
            buckets: vec![vec![]; n],
            i: i,
            r: r,
            n: n,
            threshold: 0.8,
        }
    }

    pub fn hash(&self, key: &K) -> u64 {
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
                // println!("i: {} {:b}", self.i, (1 << self.i));
                bucket - (1 << (self.i-1))
            };

        println!("{} hash: {} bucket: {}, adjusted: {}", key, hash, bucket, adjusted_bucket_index);
        adjusted_bucket_index
    }

    fn split_needed(&self) -> bool {
        (self.r as f32 / self.n as f32) > self.threshold
    }

    fn maybe_split(&mut self) -> bool {
        if self.split_needed() {
            self.n += 1;
            self.buckets.push(vec![]);
            if self.n > (1 << self.i) {
                self.i += 1;
            }
            
            // eg: after bucket 11 is added, bucket 01 needs to be
            // split
            let bucket_to_split = (self.n-1) ^ (1 << (self.i-1));
            println!("bucket_to_split: {}(0b{:b}) n: {} i: {}",
                     bucket_to_split, bucket_to_split, self.n, self.i);

            let old_bucket = self.buckets[bucket_to_split].clone();
            self.buckets[bucket_to_split] = vec![];

            for (k, v) in old_bucket {
                self.put(k, v);
            }

            println!("{:?}", self);
            return true
        }

        false
    }

    pub fn put(&mut self, key: K, val: V) {
        let bucket_index = self.bucket(&key);
        self.buckets[bucket_index].push((key, val));
        self.r += 1;

        self.maybe_split();
        // loop {
        //     if !self.maybe_split() {
        //         break;
        //     }
        // }
    }

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
}

#[cfg(test)]
mod tests {
    use LinHash;
    
    #[test]
    fn it_works() {
        let mut h : LinHash<&str, i32> = LinHash::new();
        h.put("hello", 12);
        h.put("there", 13);
        println!("{:?}", h.get("hello"));
    }
}
