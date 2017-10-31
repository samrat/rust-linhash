use std::fmt::Debug;
use bincode;
use bincode::{deserialize as bin_deserialize};
use serde::de::{Deserialize, DeserializeOwned};

pub fn mem_move(dest: &mut [u8], src: &[u8]) {
    for (d, s) in dest.iter_mut().zip(src) {
        *d = *s
    }
}

pub fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
    where T: Deserialize<'a> {
    bin_deserialize(bytes)
}

pub fn deserialize_kv<K, V>(k: &[u8], v: &[u8]) -> (K, V)
        where K: DeserializeOwned + Debug,
              V: DeserializeOwned + Debug {
        (deserialize(k).unwrap(), deserialize(v).unwrap())
    }
