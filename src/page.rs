use std::fmt::Debug;
use std::mem;
use util::mem_move;

use serde::ser::Serialize;
use bincode;
use bincode::{serialize, deserialize as bin_deserialize,
                    Bounded};
use serde::de::{Deserialize, DeserializeOwned};

pub const PAGE_SIZE : usize = 4096;     // bytes
pub const HEADER_SIZE : usize = 16;      // bytes

pub fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
    where T: Deserialize<'a> {
    bin_deserialize(bytes)
}

pub struct Page {
    pub storage: [u8; PAGE_SIZE],
    pub num_tuples: usize,
    // page_id of overflow bucket
    next: Option<usize>,
    cursor: usize,
}

struct RowOffsets {
    header_offset: usize,
    key_offset: usize,
    val_offset: usize,
    row_end: usize,
    key_size: usize,
    val_size: usize,
}

impl Page {
    pub fn new() -> Page {
        Page {
            num_tuples: 0,
            storage: [0; PAGE_SIZE],
            next: None,
            cursor: 0,
        }
    }

    fn compute_offsets<K, V>(row_num: usize) -> RowOffsets {
        let key_size = mem::size_of::<K>();
        let val_size = mem::size_of::<V>();
        let total_size = key_size + val_size;

        let row_offset = row_num * total_size;
        let header_offset = row_offset;
        let key_offset = header_offset + HEADER_SIZE;
        let val_offset = key_offset + key_size;
        let row_end = val_offset + val_size;

        RowOffsets {
            header_offset: header_offset,
            key_offset: key_offset,
            val_offset: val_offset,
            row_end: row_end,
            key_size: key_size,
            val_size: val_size,
        }
    }

    pub fn read_tuple<K: DeserializeOwned + Debug,
                      V: DeserializeOwned + Debug> (&mut self, row_num: usize) -> V {
        let offsets = Page::compute_offsets::<K,V>(row_num);

        let decoded_key : K =
            deserialize(&self.storage[offsets.key_offset..offsets.val_offset]).unwrap();
        let decoded_val : V =
            deserialize(&self.storage[offsets.val_offset..offsets.row_end]).unwrap();

        println!("read: {:?} {:?}", decoded_key, decoded_val);
        decoded_val
    }

    pub fn write_tuple<K, V>(&mut self, row_num: usize, key: K, val: V)
        where K: Serialize,
              V: Serialize {
        // TODO: check if it's not just a overwrite
        self.num_tuples += 1;
        let offsets = Page::compute_offsets::<K,V>(row_num);

        // The maximum sizes of the encoded key and val.
        let key_limit = Bounded(offsets.key_size as u64);
        let val_limit = Bounded(offsets.val_size as u64);

        mem_move(&mut self.storage[offsets.key_offset..offsets.val_offset],
                 &serialize(&key, key_limit).unwrap());
        mem_move(&mut self.storage[offsets.val_offset..offsets.row_end],
                 &serialize(&val, val_limit).unwrap());
    }

    pub fn next() {

    }
}
