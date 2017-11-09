use util::*;

pub const PAGE_SIZE : usize = 4096; // bytes
pub const HEADER_SIZE : usize = 16; // bytes

pub struct Page {
    pub id: usize,
    pub storage: [u8; PAGE_SIZE],
    pub num_records: usize,
    // page_id of overflow bucket
    pub next: Option<usize>,
    pub dirty: bool,

    keysize: usize,
    valsize: usize,
}

// Row layout:
// | key | val |
#[derive(Debug)]
struct RowOffsets {
    key_offset: usize,
    val_offset: usize,
    row_end: usize,
}

impl Page {
    pub fn new(keysize: usize, valsize: usize) -> Page {
        Page {
            id: 0,
            num_records: 0,
            storage: [0; PAGE_SIZE],
            next: None,
            keysize: keysize,
            valsize: valsize,
            dirty: false,
        }
    }

    /// Compute where in the page the row should be placed. Within the
    /// row, calculate the offsets of the header, key and value.
    fn compute_offsets(&self, row_num: usize) -> RowOffsets {
        let total_size = self.keysize + self.valsize;

        let row_offset = HEADER_SIZE + (row_num * total_size);
        let key_offset = row_offset;
        let val_offset = key_offset + self.keysize;
        let row_end = val_offset + self.valsize;

        RowOffsets {
            key_offset: key_offset,
            val_offset: val_offset,
            row_end: row_end,
        }
    }


    pub fn read_header(&mut self) {
        let num_records : usize = bytearray_to_usize(self.storage[0..8].to_vec());
        let next : usize = bytearray_to_usize(self.storage[8..16].to_vec());
        self.num_records = num_records;
        self.next = if next != 0 {
            Some(next)
        } else {
            None
        };
    }

    pub fn write_header(&mut self) {
        mem_move(&mut self.storage[0..8], &usize_to_bytearray(self.num_records));
        mem_move(&mut self.storage[8..16], &usize_to_bytearray(self.next.unwrap_or(0)));
    }

    pub fn read_record(&mut self, row_num: usize) -> (&[u8], &[u8]) {
        let offsets = self.compute_offsets(row_num);
        let key = &self.storage[offsets.key_offset..offsets.val_offset];
        let val = &self.storage[offsets.val_offset..offsets.row_end];
        (key, val)
    }

    /// Write record to offset specified by `row_num`. The offset is
    /// calculated to accomodate header as well.
    pub fn write_record(&mut self, row_num: usize, key: &[u8], val: &[u8]) {
        let offsets = self.compute_offsets(row_num);
        mem_move(&mut self.storage[offsets.key_offset..offsets.val_offset],
                 key);
        mem_move(&mut self.storage[offsets.val_offset..offsets.row_end],
                 val);
    }

    /// Increment number of records in page
    pub fn incr_num_records(&mut self) {
        self.num_records += 1;
    }
}
