use std::io::prelude::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::str;
use std::mem;
use std::fmt::Debug;

use page;
use page::{Page, PAGE_SIZE, HEADER_SIZE};
use util::*;

const CTRL_HEADER_SIZE : usize = 32; // bytes

pub struct SearchResult {
    pub page_id: Option<usize>,
    pub row_num: Option<usize>,
    pub val: Option<Vec<u8>>
}

fn flatten<T>(v: Vec<(usize, Vec<T>)>) -> Vec<T> {
    let mut result = vec![];
    for (_, mut i) in v {
        result.append(&mut i);
    }
    result
}

pub struct DbFile {
    path: String,
    file: File,
    ctrl_buffer: Page,
    pub buffer: Page,
    // which page is currently in `buffer`
    page_id: Option<usize>,
    pub records_per_page: usize,
    // changes made to `buffer`?
    dirty: bool,
    bucket_to_page: Vec<usize>,
    free_page: usize,
    keysize: usize,
    valsize: usize,
}

impl DbFile {
    pub fn new(filename: &str, keysize: usize, valsize: usize) -> DbFile {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename);
        let file = match file {
            Ok(f) => f,
            Err(e) => panic!(e),
        };

        let total_size = HEADER_SIZE + keysize + valsize;
        let records_per_page = PAGE_SIZE / total_size;
        DbFile {
            path: String::from(filename),
            file: file,
            ctrl_buffer: Page::new(0, 0),
            buffer: Page::new(keysize, valsize),
            page_id: None,
            records_per_page: records_per_page,
            dirty: false,
            free_page: 3,
            bucket_to_page: vec![1, 2],
            keysize: keysize,
            valsize: valsize,
        }
    }

    // Control page layout:
    // | nbits | nitems | nbuckets | bucket_to_page mapping ....
    pub fn read_ctrlpage(&mut self) -> (usize, usize, usize) {
        self.get_ctrl_page();
        let nbits : usize = bytearray_to_usize(self.ctrl_buffer.storage[0..8].to_vec());
        let nitems : usize =
            bytearray_to_usize(self.ctrl_buffer.storage[8..16].to_vec());
        let nbuckets : usize =
            bytearray_to_usize(self.ctrl_buffer.storage[16..24].to_vec());

        self.free_page =
            bytearray_to_usize(self.ctrl_buffer.storage[24..32].to_vec());
        self.bucket_to_page =
            bytevec_to_usize_vec(self.ctrl_buffer.storage[32..PAGE_SIZE].to_vec());
        (nbits, nitems, nbuckets)
    }

    pub fn write_ctrlpage(&mut self,
                          (nbits, nitems, nbuckets):
                          (usize, usize, usize)) {
        self.get_ctrl_page();

        let nbits_bytes = usize_to_bytearray(nbits);
        let nitems_bytes = usize_to_bytearray(nitems);
        let nbuckets_bytes = usize_to_bytearray(nbuckets);
        let free_page_bytes = usize_to_bytearray(self.free_page);
        let bucket_to_page_bytevec = usize_vec_to_bytevec(self.bucket_to_page.clone());
        let mut bucket_to_page_bytearray = vec![];
        bucket_to_page_bytearray.write(&bucket_to_page_bytevec);
        println!("nbits: {:?} nitems: {:?} nbuckets: {:?}", nbits_bytes,
                 nitems_bytes, nbuckets_bytes);
        mem_move(&mut self.ctrl_buffer.storage[0..8],
                 &nbits_bytes);
        mem_move(&mut self.ctrl_buffer.storage[8..16],
                 &nitems_bytes);
        mem_move(&mut self.ctrl_buffer.storage[16..24],
                 &nbuckets_bytes);
        mem_move(&mut self.ctrl_buffer.storage[24..32],
                 &free_page_bytes);
        mem_move(&mut self.ctrl_buffer.storage[32..PAGE_SIZE],
                 &bucket_to_page_bytearray);
        DbFile::write_page(&mut self.file,
                           0,
                           &self.ctrl_buffer.storage);
    }

    fn read_header(&mut self) {
        let num_records : usize = bytearray_to_usize(self.buffer.storage[0..8].to_vec());
        let next : usize = bytearray_to_usize(self.buffer.storage[8..16].to_vec());
        let prev : usize = bytearray_to_usize(self.buffer.storage[16..24].to_vec());
        self.buffer.num_records = num_records;
        self.buffer.next = if next != 0 {
            Some(next)
        } else {
            None
        };
        self.buffer.prev = if prev != 0 {
            Some(prev)
        } else {
            None
        };
    }

    fn write_header(&mut self) {
        println!("[write_header] id={:?} {:?} {:?}", self.page_id, self.buffer.next, self.bucket_to_page);

        // let mut num_records_bytes = vec![];
        // num_records_bytes.write(self.buffer.num_records);
        // let mut next_bytes = vec![];
        // next_bytes.write(&self.buffer.next);
        // let mut prev_bytes = vec![];
        // prev_bytes.write(&self.buffer.prev.unwrap_or(0));

        mem_move(&mut self.buffer.storage[0..8], &usize_to_bytearray(self.buffer.num_records));
        mem_move(&mut self.buffer.storage[8..16], &usize_to_bytearray(self.buffer.next.unwrap_or(0)));
        mem_move(&mut self.buffer.storage[16..24], &usize_to_bytearray(self.buffer.prev.unwrap_or(0)));

    }

    pub fn get_ctrl_page(&mut self) {
        self.file.seek(SeekFrom::Start(0))
            .expect("Could not seek to offset");
        self.file.read(&mut self.ctrl_buffer.storage)
            .expect("Could not read file");
    }

    fn bucket_to_page(&self, bucket_id: usize) -> usize {
        self.bucket_to_page[bucket_id]
    }

    fn get_bucket(&mut self, bucket_id: usize) {
        let page_id = self.bucket_to_page(bucket_id);
        self.get_page(page_id);
    }

    // Reads page to self.buffer
    pub fn get_page(&mut self, page_id: usize) {
        match self.page_id {
            Some(p) if p == page_id => (),
            Some(_) | None => {
                if self.dirty {
                    self.write_buffer();
                }
                self.dirty = false;
                // clear out buffer
                mem::replace(&mut self.buffer.storage, [0; 4096]);

                let offset = (page_id * PAGE_SIZE) as u64;
                self.file.seek(SeekFrom::Start(offset))
                    .expect("Could not seek to offset");
                self.file.read(&mut self.buffer.storage)
                    .expect("Could not read file");
                self.page_id = Some(page_id);
                self.buffer.id = page_id;
                self.read_header();
            },
        }
    }

    fn write_bucket(&mut self, mut file: &File, bucket_id: usize, data: &[u8]) {
        let page_id = self.bucket_to_page(bucket_id);
        DbFile::write_page(&self.file, page_id, data);
    }

    /// Writes data in `data` into page `page_id`
    pub fn write_page(mut file: &File, page_id: usize, data: &[u8]) {
        let offset = (page_id * PAGE_SIZE) as u64;
        file.seek(SeekFrom::Start(offset))
            .expect("Could not seek to offset");
        println!("wrote {:?} bytes from offset {}",
                 file.write(data), offset);
        file.flush().expect("flush failed");
    }

    /// Write record but don't increment `num_records`. Used when
    /// updating already existing record.
    pub fn write_record(&mut self,
                        page_id: usize,
                        row_num: usize,
                        key: &[u8],
                        val: &[u8]) {
        self.get_page(page_id);

        self.dirty = true;
        self.buffer.write_record(row_num, key, val);
    }

    /// Write record and increment `num_records`. Used when inserting
    /// new record.
    pub fn write_record_incr(&mut self, page_id: usize, row_num: usize,
                             key: &[u8], val: &[u8]) {
        self.buffer.incr_num_records();
        self.write_record(page_id, row_num, key, val);
    }

    /// Searches for `key` in `bucket`. A bucket is a linked list of
    /// pages.
    pub fn search_bucket(&mut self, bucket_id: usize, key: &[u8]) -> SearchResult {
        println!("[get] bucket_id: {}", bucket_id);
        let all_records_in_bucket =
            self.all_records_in_bucket(bucket_id);

        let mut first_free_row = SearchResult {
            page_id: None,
            row_num: None,
            val: None,
        };

        for (i, page_records) in all_records_in_bucket.into_iter() {
            let len = page_records.len();
            for (row_num, (k,v)) in page_records.into_iter().enumerate() {
                if slices_eq(&k, key) {
                    return SearchResult{
                        page_id: Some(i),
                        row_num: Some(row_num),
                        val: Some(v)
                    }
                }
            }

            if len < self.records_per_page {
                first_free_row = SearchResult {
                    page_id: Some(i),
                    row_num: Some(len),
                    val: None,
                }
            }
        }

        first_free_row
    }

    /// Add a new overflow page to a `bucket`.
    pub fn allocate_overflow(&mut self, bucket_id: usize) -> usize {
        // Write next of old page
        self.buffer.next = Some(self.free_page);
        self.write_buffer();

        let physical_index = self.allocate_new_page();
        self.bucket_to_page.push(physical_index);
        println!("{}'s next: physical_index: {}", self.buffer.id, physical_index);

        self.get_page(physical_index);
        self.buffer.prev = Some(self.bucket_to_page(bucket_id));
        self.write_buffer();


        // virtual address
        physical_index - 1
    }

    pub fn put(&mut self, bucket_id: usize, key: &[u8], val: &[u8]) {
        println!("[put] bucket_id: {}", bucket_id);
        self.get_bucket(bucket_id);
        self.dirty = true;
        self.buffer.put(key, val);
    }

    /// Write out page in `buffer` to file.
    pub fn write_buffer(&mut self) {
        self.dirty = false;
        self.write_header();
        DbFile::write_page(&mut self.file,
                           self.page_id.expect("No page buffered"),
                           &self.buffer.storage);
    }

    /// Returns a vec of (page_id, records_in_vec). ie. each inner
    /// vector represents the records in a page in the bucket.
    fn all_records_in_bucket(&mut self, bucket_id: usize)
                             -> Vec<(usize, Vec<(Vec<u8>,Vec<u8>)>)> {
        self.get_bucket(bucket_id);
        let mut records = Vec::new();

        let mut page_records = vec![];
        for i in 0..self.buffer.num_records {
            let (k, v) = self.buffer.read_record(i);
            let (dk, dv) = (k.to_vec(), v.to_vec());
            page_records.push((dk, dv));
        }
        records.push((self.page_id.unwrap(), page_records));

        while let Some(page_id) = self.buffer.next {
            if page_id == 0 {
                break;
            }

            self.get_page(page_id);
            let mut page_records = vec![];
            for i in 0..self.buffer.num_records {
                let (k, v) = self.buffer.read_record(i);
                let (dk, dv) = (k.to_vec(), v.to_vec());

                page_records.push((dk, dv));
            }
            records.push((page_id, page_records));
        }

        records
    }

    /// Allocate a new page.
    fn allocate_new_page(&mut self) -> usize {
        let page_id = self.free_page;
        let new_page = Page::new(self.keysize, self.valsize);
        mem::replace(&mut self.buffer, new_page);
        self.buffer.id = page_id;
        self.page_id = Some(page_id);
        self.dirty = false;
        self.write_buffer();
        self.free_page += 1;

        page_id
    }

    // NOTE: Old pages are not reclaimed at the moment
    pub fn clear_bucket(&mut self, bucket_id: usize) -> Vec<(Vec<u8>,Vec<u8>)> {
        let page_id = self.bucket_to_page(bucket_id);
        let new_page = Page::new(self.keysize, self.valsize);
        let records = flatten(self.all_records_in_bucket(bucket_id));
        mem::replace(&mut self.buffer, new_page);
        self.buffer.id = page_id;
        self.page_id = Some(page_id);
        self.dirty = false;
        self.write_buffer();

        records
    }

    pub fn allocate_new_bucket(&mut self) {
        let page_id = self.allocate_new_page();
        self.bucket_to_page.push(page_id);
    }

    pub fn close(&mut self) {
        self.write_buffer();
    }
}

#[cfg(test)]
mod tests {
    use DbFile;

    #[test]
    fn dbfile_tests () {
        // let mut bp = DbFile::new::<i32, String>("/tmp/buff");
        // bp.write_record(0, 14, String::from("samrat"));
        // bp.write_record(1, 12, String::from("foo"));
        // bp.write_buffer();
        // let v = bp.read_record::<i32, String>(1);
        // bp.all_records_in_page::<i32, String>(1);
        // bp.write_page(0, &bp.buffer.storage);
        assert_eq!(1+1,2);
    }
}
