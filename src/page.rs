use std::fmt::Debug;
use std::mem;
use util::mem_move;

pub const PAGE_SIZE : usize = 4096;     // bytes
pub const HEADER_SIZE : usize = 16;      // bytes

pub struct Page {
    pub storage: [u8; PAGE_SIZE],
    pub num_tuples: usize,
    // page_id of overflow bucket
    next: Option<usize>,
    cursor: usize,
    key_size: usize,
    val_size: usize,
}

struct RowOffsets {
    header_offset: usize,
    key_offset: usize,
    val_offset: usize,
    row_end: usize,
}

impl Page {
    pub fn new(key_size: usize, val_size: usize) -> Page {
        Page {
            num_tuples: 0,
            storage: [0; PAGE_SIZE],
            next: None,
            cursor: 0,
            key_size: key_size,
            val_size: val_size,
        }
    }

    fn compute_offsets(&self, row_num: usize) -> RowOffsets {
        let total_size = self.key_size + self.val_size;

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

        let key = &self.storage[offsets.key_offset..offsets.val_offset];
        let val = &self.storage[offsets.val_offset..offsets.row_end];
        (key, val)
    }

    pub fn write_tuple(&mut self, row_num: usize,
                             key: &[u8], val: &[u8]) {
        // TODO: check if it's not just a overwrite
        self.num_tuples += 1;
        let offsets = self.compute_offsets(row_num);

        mem_move(&mut self.storage[offsets.key_offset..offsets.val_offset],
                 key);
        mem_move(&mut self.storage[offsets.val_offset..offsets.row_end],
                 val);
    }

    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        let cursor;
        {
            cursor = self.cursor;
        }

        self.cursor += 1;
        if self.cursor < self.num_tuples {
            Some(self.read_tuple(cursor))
        } else {
            None
        }
    }
}
