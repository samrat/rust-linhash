use std::fmt::Debug;
use std::mem;
use util::mem_move;
use std::str;

pub const PAGE_SIZE : usize = 4096;     // bytes
pub const HEADER_SIZE : usize = 16;      // bytes

pub struct Page {
    pub id: usize,
    pub storage: [u8; PAGE_SIZE],
    pub num_tuples: usize,
    // page_id of overflow bucket
    next: Option<usize>,
    key_size: usize,
    val_size: usize,
}

#[derive(Debug)]
struct RowOffsets {
    header_offset: usize,
    key_offset: usize,
    val_offset: usize,
    row_end: usize,
}

impl Page {
    pub fn new(key_size: usize, val_size: usize) -> Page {
        Page {
            id: 0,
            num_tuples: 0,
            storage: [0; PAGE_SIZE],
            next: None,
            key_size: key_size,
            val_size: val_size,
        }
    }

    fn compute_offsets(&self, row_num: usize) -> RowOffsets {
        let total_size = HEADER_SIZE + self.key_size + self.val_size;

        let row_offset = row_num * total_size;
        let header_offset = row_offset;
        let key_offset = header_offset + HEADER_SIZE;
        let val_offset = key_offset + self.key_size;
        let row_end = val_offset + self.val_size;

        RowOffsets {
            header_offset: header_offset,
            key_offset: key_offset,
            val_offset: val_offset,
            row_end: row_end,
        }
    }

    pub fn read_tuple(&mut self, row_num: usize) -> (&[u8], &[u8]) {
        let offsets = self.compute_offsets(row_num);
        // println!("{} {:?} {:?}", self.id, offsets, self.storage.to_vec());

        let key = &self.storage[offsets.key_offset..offsets.val_offset];
        let val = &self.storage[offsets.val_offset..offsets.row_end];
        (key, val)
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) {
        let offsets = self.compute_offsets(self.num_tuples);
        // println!("[page.put] page_id: {} key: {:?} val: {:?}", self.id, key.to_vec(), val.to_vec());
        mem_move(&mut self.storage[offsets.key_offset..offsets.val_offset],
                 key);
        mem_move(&mut self.storage[offsets.val_offset..offsets.row_end],
                 val);
        // println!("storage: {:?}", self.storage.to_vec());
        // TODO: check if it's not just a overwrite
        self.num_tuples += 1;
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let num_records = self.num_tuples;

        for i in 0..num_records {
            let (k, v) = self.read_tuple(i);
            let v_vec = v.to_vec();
            println!("{:?}", str::from_utf8(&k));
            if k.iter().zip(key).all(|(a,b)| a == b) {
                return Some(v_vec);
            }
        }
        None
    }


}

// impl KVStore for Page {
//     fn put(&mut self, key: &[u8], val: &[u8]) {
//         self.write_tuple(key, val)
//     }


// }
