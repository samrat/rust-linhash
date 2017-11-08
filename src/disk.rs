use std::collections::VecDeque;
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
const NUM_BUFFERS : usize = 16;

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
    pub buffers: VecDeque<Page>,
    pub records_per_page: usize,
    bucket_to_page: Vec<usize>,
    keysize: usize,
    valsize: usize,
    num_pages: usize,
    // overflow pages no longer in use
    free_list: Option<usize>,
    num_free: usize,
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

        let total_size = keysize + valsize;
        let records_per_page = (PAGE_SIZE - HEADER_SIZE) / total_size;

        let mut buffers : VecDeque<Page> =
            VecDeque::with_capacity(NUM_BUFFERS);
        for i in 0..NUM_BUFFERS {
            buffers.push_back(Page::new(keysize, valsize));
        }

        DbFile {
            path: String::from(filename),
            file: file,
            ctrl_buffer: Page::new(0, 0),
            buffers: buffers,
            records_per_page: records_per_page,
            bucket_to_page: vec![1, 2],
            keysize: keysize,
            valsize: valsize,
            num_pages: 3,
            free_list: Some(3),
            num_free: 0,
        }
    }

    // Control page layout:
    //
    // | nbits | nitems | nbuckets | num_pages | free_list root |
    // num_free | bucket_to_page mapping .... |
    pub fn read_ctrlpage(&mut self) -> (usize, usize, usize) {
        self.get_ctrl_page();
        let nbits : usize = bytearray_to_usize(self.ctrl_buffer.storage[0..8].to_vec());
        let nitems : usize =
            bytearray_to_usize(self.ctrl_buffer.storage[8..16].to_vec());
        let nbuckets : usize =
            bytearray_to_usize(self.ctrl_buffer.storage[16..24].to_vec());

        self.num_pages =
            bytearray_to_usize(self.ctrl_buffer.storage[24..32].to_vec());
        let free_list_head = bytearray_to_usize(self.ctrl_buffer.storage[32..40].to_vec());
        self.free_list =
            if free_list_head == 0 {
                None
            } else {
                Some(free_list_head)
            };
        self.num_free =
            bytearray_to_usize(self.ctrl_buffer.storage[40..48].to_vec());
        self.bucket_to_page =
            bytevec_to_usize_vec(self.ctrl_buffer.storage[48..PAGE_SIZE].to_vec());
        (nbits, nitems, nbuckets)
    }

    pub fn write_ctrlpage(&mut self,
                          (nbits, nitems, nbuckets):
                          (usize, usize, usize)) {
        self.get_ctrl_page();

        let nbits_bytes = usize_to_bytearray(nbits);
        let nitems_bytes = usize_to_bytearray(nitems);
        let nbuckets_bytes = usize_to_bytearray(nbuckets);
        let num_pages_bytes = usize_to_bytearray(self.num_pages);
        let free_list_bytes = usize_to_bytearray(self.free_list.unwrap_or(0));
        let num_free_bytes = usize_to_bytearray(self.num_free);
        let bucket_to_page_bytevec = usize_vec_to_bytevec(self.bucket_to_page.clone());
        let mut bucket_to_page_bytearray = vec![];
        bucket_to_page_bytearray.write(&bucket_to_page_bytevec)
            .expect("Write to ctrlpage failed");

        println!("nbits: {:?} nitems: {:?} nbuckets: {:?}", nbits_bytes,
                 nitems_bytes, nbuckets_bytes);
        mem_move(&mut self.ctrl_buffer.storage[0..8],
                 &nbits_bytes);
        mem_move(&mut self.ctrl_buffer.storage[8..16],
                 &nitems_bytes);
        mem_move(&mut self.ctrl_buffer.storage[16..24],
                 &nbuckets_bytes);
        mem_move(&mut self.ctrl_buffer.storage[24..32],
                 &num_pages_bytes);
        mem_move(&mut self.ctrl_buffer.storage[32..40],
                 &free_list_bytes);
        mem_move(&mut self.ctrl_buffer.storage[40..48],
                 &num_free_bytes);
        mem_move(&mut self.ctrl_buffer.storage[48..PAGE_SIZE],
                 &bucket_to_page_bytearray);
        DbFile::write_page(&mut self.file,
                           0,
                           &self.ctrl_buffer.storage);
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

    fn search_buffer_pool(&self, page_id: usize) -> Option<usize> {
        for (i, b) in self.buffers.iter().enumerate() {
            if b.id == page_id {
                return Some(i);
            }
        }
        None
    }

    /// Reads page to self.buffer
    pub fn fetch_page(&mut self, page_id: usize) -> usize {
        let bufpool_index = self.search_buffer_pool(page_id);
        match bufpool_index {
            None => {
                match self.buffers.pop_front() {
                    Some(mut old_page) => {
                        if old_page.dirty {
                            old_page.write_header();
                            DbFile::write_page(&self.file,
                                               old_page.id,
                                               &old_page.storage);
                        }
                    },
                    _ => (),
                }

                let offset = (page_id * PAGE_SIZE) as u64;
                let mut new_page = Page::new(self.keysize, self.valsize);
                new_page.id = page_id;
                let buffer_index = (NUM_BUFFERS - 1);

                self.file.seek(SeekFrom::Start(offset))
                    .expect("Could not seek to offset");
                self.file.read(&mut new_page.storage)
                    .expect("Could not read file");
                self.buffers.push_back(new_page);
                // println!("[fetch_page] id: {} {:?}", self.buffers[15].id, self.buffers[15].storage.to_vec());
                self.buffers[buffer_index].read_header();

                buffer_index
            },
            Some(p) => p,
        }
    }

    /// Writes data in `data` into page `page_id`
    pub fn write_page(mut file: &File, page_id: usize, data: &[u8]) {
        let offset = (page_id * PAGE_SIZE) as u64;
        file.seek(SeekFrom::Start(offset))
            .expect("Could not seek to offset");
        file.write(data).expect("write failed");
        file.flush().expect("flush failed");
    }

    /// Write record but don't increment `num_records`. Used when
    /// updating already existing record.
    pub fn write_record(&mut self,
                        page_id: usize,
                        row_num: usize,
                        key: &[u8],
                        val: &[u8]) {
        let buffer_index = self.fetch_page(page_id);
        self.buffers[buffer_index].dirty = true;
        self.buffers[buffer_index].write_record(row_num, key, val);
    }

    /// Write record and increment `num_records`. Used when inserting
    /// new record.
    pub fn write_record_incr(&mut self, page_id: usize, row_num: usize,
                             key: &[u8], val: &[u8]) {
        let buffer_index = self.fetch_page(page_id);
        self.buffers[buffer_index].incr_num_records();
        self.write_record(page_id, row_num, key, val);
    }

    /// Searches for `key` in `bucket`. A bucket is a linked list of
    /// pages. Return value:
    ///
    /// If key is present in bucket returns as struct, SearchResult
    /// (page_id, row_num, val).
    ///
    /// If key is not present and:
    ///   1. there is enough space in last page, returns (page_id, row_num, None)
    ///
    ///   2. there is not enough space in last page, returns
    ///      (last_page_id, None, None)
    pub fn search_bucket(&mut self, bucket_id: usize, key: &[u8]) -> SearchResult {
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

            let row_num = if len < self.records_per_page {
                Some(len)
            } else {
                None
            };
            first_free_row = SearchResult {
                page_id: Some(i),
                row_num: row_num,
                val: None,
            }
        }

        first_free_row
    }

    /// Add a new overflow page to a `bucket`.
    pub fn allocate_overflow(&mut self, bucket_id: usize,
                             last_page_id: usize) -> (usize, usize) {
        let physical_index = self.allocate_new_page();

        let new_page_buffer_index = self.fetch_page(physical_index);
        self.buffers[new_page_buffer_index].prev = Some(last_page_id);
        self.buffers[new_page_buffer_index].dirty = true;

        // Write next of old page
        let old_page_buffer_index = self.fetch_page(last_page_id);
        self.buffers[old_page_buffer_index].next = Some(physical_index);
        self.buffers[old_page_buffer_index].dirty = true;

        println!("setting next of buffer_id {}(page_id: {}) to {:?}", bucket_id, last_page_id, self.buffers[old_page_buffer_index].next);

        (physical_index, 0)
    }

    /// Write out page in bufferpool to file.
    pub fn write_buffer_page(&mut self, buffer_index: usize) {
        // Ignore page 0(ctrlpage)
        if self.buffers[buffer_index].id != 0 {
            self.buffers[buffer_index].dirty = false;
            self.buffers[buffer_index].write_header();
            DbFile::write_page(&mut self.file,
                               self.buffers[buffer_index].id,
                               &self.buffers[buffer_index].storage);
        }
    }

    fn all_records_in_page(&mut self, page_id: usize)
                           -> Vec<(Vec<u8>, Vec<u8>)> {
        let buffer_index = self.fetch_page(page_id);
        let mut page_records = vec![];
        println!("buffer_index: {} num_records: {}", buffer_index, self.buffers[buffer_index].num_records);
        for i in 0..self.buffers[buffer_index].num_records {
            let (k, v) = self.buffers[buffer_index].read_record(i);
            let (dk, dv) = (k.to_vec(), v.to_vec());
            page_records.push((dk, dv));
        }

        page_records
    }

    /// Returns a vec of (page_id, records_in_vec). ie. each inner
    /// vector represents the records in a page in the bucket.
    fn all_records_in_bucket(&mut self, bucket_id: usize)
                             -> Vec<(usize, Vec<(Vec<u8>,Vec<u8>)>)> {
        let first_page_id = self.bucket_to_page(bucket_id);
        let buffer_index = self.fetch_page(first_page_id);
        let mut records = Vec::new();
        records.push((self.buffers[buffer_index].id,
                      self.all_records_in_page(first_page_id)));

        let mut next_page = self.buffers[buffer_index].next;
        while let Some(page_id) = next_page {
            if page_id == 0 {
                break;
            }

            let buffer_index = self.fetch_page(page_id);
            records.push((page_id,
                          self.all_records_in_page(page_id)));

            next_page = self.buffers[buffer_index].next;
        }

        records
    }

    /// Allocate a new page. If available uses recycled overflow
    /// pages.
    fn allocate_new_page(&mut self) -> usize {
        let p = self.free_list;
        let page_id = p.expect("no page in free_list");
        println!("[allocate_new_page] allocating page_id: {}", page_id);
        let buffer_index = self.fetch_page(page_id);

        self.free_list = match self.buffers[buffer_index].next {
            Some(0) | None => {
                self.num_pages += 1;
                Some(self.num_pages)
            },
            _ => {
                self.num_free -= 1;
                self.buffers[buffer_index].next
            },
        };

        let new_page = Page::new(self.keysize, self.valsize);
        mem::replace(&mut self.buffers[buffer_index], new_page);
        self.buffers[buffer_index].id = page_id;
        self.buffers[buffer_index].dirty = false;
        self.buffers[buffer_index].next = None;
        self.buffers[buffer_index].prev = None;

        page_id
    }

    /// Empties out root page for bucket. Overflow pages are added to
    /// `free_list`
    pub fn clear_bucket(&mut self, bucket_id: usize) -> Vec<(Vec<u8>,Vec<u8>)> {
        let mut all_records = self.all_records_in_bucket(bucket_id);
        let records = flatten(all_records.clone());

        // Add overflow pages to free_list
        let bucket_len = all_records.len();
        if bucket_len > 1 {
            // second page onwards are overflow pages
            let (second_page_id, _) = all_records[1];
            println!("[clear_bucket] adding overflow chain starting page {} to free_list", second_page_id);
            let temp = self.free_list;
            self.free_list = Some(second_page_id);

            let second_page_buffer_index =
                self.fetch_page(second_page_id);
            // overflow pages only
            self.num_free += bucket_len - 1;
            self.buffers[second_page_buffer_index].next = temp;
        }

        let page_id = self.bucket_to_page(bucket_id);
        let buffer_index = self.fetch_page(page_id);
        let new_page = Page::new(self.keysize, self.valsize);
        mem::replace(&mut self.buffers[buffer_index], new_page);
        self.buffers[buffer_index].id = page_id;
        self.buffers[buffer_index].dirty = false;
        self.write_buffer_page(buffer_index);

        records
    }

    pub fn allocate_new_bucket(&mut self) {
        let page_id = self.allocate_new_page();
        self.bucket_to_page.push(page_id);
    }

    pub fn close(&mut self) {
        for b in 0..NUM_BUFFERS {
            self.write_buffer_page(b);
        }
    }
}

#[cfg(test)]
mod tests {
    use DbFile;

    #[test]
    fn dbfile_tests () {
        let mut bp = DbFile::new("/tmp/buff", 4, 4);
        let bark = "bark".as_bytes();
        let krab = "krab".as_bytes();
        bp.write_record(0, 14, bark, krab);
        assert_eq!(bp.buffers[0].read_record(14), (bark, krab));
        bp.close();

        let mut bp2 = DbFile::new("/tmp/buff", 4, 4);
        assert_eq!(bp.buffers[0].read_record(14), (bark, krab));
    }
}
