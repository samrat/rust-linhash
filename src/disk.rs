use std::io::prelude::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::str;
use std::mem;
use std::fmt::Debug;

use page;
use page::{Page, PAGE_SIZE, HEADER_SIZE};
use util::{mem_move, deserialize, deserialize_kv};

use bincode;
use bincode::{serialize, deserialize as bin_deserialize,
              Bounded};
use serde::ser::Serialize;
use serde::de::{Deserialize, DeserializeOwned};

const CTRL_HEADER_SIZE : usize = 32; // bytes

pub struct SearchResult<V> {
    pub page_id: Option<usize>,
    pub row_num: Option<usize>,
    pub val: Option<V>
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
    pub tuples_per_page: usize,
    // changes made to `buffer`? (TODO: this flag is not used right
    // now)
    dirty: bool,
    bucket_to_page: Vec<usize>,
    free_page: usize,
}

impl DbFile {
    pub fn new<K,V>(filename: &str) -> DbFile {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename);
        let file = match file {
            Ok(f) => f,
            Err(e) => panic!(e),
        };

        let keysize = mem::size_of::<K>();
        let valsize = mem::size_of::<V>();
        let total_size = HEADER_SIZE + keysize + valsize;
        let tuples_per_page = PAGE_SIZE / total_size;
        DbFile {
            path: String::from(filename),
            file: file,
            ctrl_buffer: Page::new(0, 0),
            buffer: Page::new(keysize, valsize),
            page_id: None,
            tuples_per_page: tuples_per_page,
            dirty: false,
            free_page: 3,
            bucket_to_page: vec![1, 2],
        }
    }

    // Control page layout:
    // | nbits | nitems | nbuckets | ....
    pub fn read_ctrlpage(&mut self) -> (usize, usize, usize) {
        self.get_ctrl_page();
        let nbits : usize =
            deserialize(&self.ctrl_buffer.storage[0..8]).unwrap();
        let nitems : usize =
            deserialize(&self.ctrl_buffer.storage[8..16]).unwrap();
        let nbuckets : usize =
            deserialize(&self.ctrl_buffer.storage[16..24]).unwrap();

        self.free_page =
            deserialize(&self.ctrl_buffer.storage[24..32]).unwrap();
        self.bucket_to_page =
            deserialize(&self.ctrl_buffer.storage[32..PAGE_SIZE]).unwrap();

        (nbits, nitems, nbuckets)
    }

    pub fn write_ctrlpage(&mut self,
                          (nbits, nitems, nbuckets):
                          (usize, usize, usize)) {
        self.get_ctrl_page();
        let nbits_bytes = &serialize(&nbits, Bounded(8)).unwrap();
        let nitems_bytes = &serialize(&nitems, Bounded(8)).unwrap();
        let nbuckets_bytes = &serialize(&nbuckets, Bounded(8)).unwrap();
        let free_page_bytes = &serialize(&self.free_page, Bounded(8)).unwrap();
        let bucket_to_page_size = PAGE_SIZE-CTRL_HEADER_SIZE;
        let bucket_to_page_bytes =
            &serialize(&self.bucket_to_page,
                       Bounded(bucket_to_page_size as u64)).unwrap();
        println!("nbits: {:?} nitems: {:?} nbuckets: {:?}", nbits_bytes,
                 nitems_bytes, nbuckets_bytes);
        mem_move(&mut self.ctrl_buffer.storage[0..8],
                 nbits_bytes);
        mem_move(&mut self.ctrl_buffer.storage[8..16],
                 nitems_bytes);
        mem_move(&mut self.ctrl_buffer.storage[16..24],
                 nbuckets_bytes);
        mem_move(&mut self.ctrl_buffer.storage[24..32],
                 free_page_bytes);
        mem_move(&mut self.ctrl_buffer.storage[32..PAGE_SIZE],
                 bucket_to_page_bytes);
        DbFile::write_page(&mut self.file,
                           0,
                           &self.ctrl_buffer.storage);
    }

    fn read_header(&mut self) {
        let num_tuples : usize = deserialize(&self.buffer.storage[0..8]).unwrap();
        let next : usize = deserialize(&self.buffer.storage[8..16]).unwrap();
        let prev : usize = deserialize(&self.buffer.storage[16..24]).unwrap();
        self.buffer.num_tuples = num_tuples;
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

        mem_move(&mut self.buffer.storage[0..8],
                 &serialize(&self.buffer.num_tuples, Bounded(8)).unwrap());
        mem_move(&mut self.buffer.storage[8..16],
                 &serialize(&self.buffer.next.unwrap_or(0), Bounded(10)).unwrap());
        mem_move(&mut self.buffer.storage[16..24],
                 &serialize(&self.buffer.next.unwrap_or(0), Bounded(10)).unwrap());
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
                    println!("writing {} and resetting dirty bit", self.buffer.id);
                    self.write_buffer();
                }
                self.dirty = false;
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

    /// Write tuple but don't increment `num_tuples`. Used when
    /// updating already existing record.
    pub fn write_tuple<K, V>(&mut self, page_id: usize, row_num: usize, key: K, val: V)
        where K: Serialize,
              V: Serialize {
        self.get_page(page_id);

        // The maximum sizes of the encoded key and val.
        let key_limit = Bounded(mem::size_of::<K>() as u64);
        let val_limit = Bounded(mem::size_of::<V>() as u64);

        self.dirty = true;
        self.buffer.write_tuple(row_num,
                                &serialize(&key, key_limit).unwrap(),
                                &serialize(&val, val_limit).unwrap());
    }

    /// Write tuple and increment `num_tuples`. Used when inserting
    /// new record.
    pub fn write_tuple_incr<K,V>(&mut self, page_id: usize, row_num: usize, key: K, val: V)
        where K: Serialize,
              V: Serialize {
        self.buffer.incr_num_tuples();
        self.write_tuple(page_id, row_num, key, val);
    }

    /// Searches for `key` in `bucket`. A bucket is a linked list of
    /// pages.
    pub fn search_bucket<K, V>(&mut self, bucket_id: usize, key: K) ->
        SearchResult<V>
        where K: DeserializeOwned + Debug + PartialEq,
              V: DeserializeOwned + Debug {
        println!("[get] bucket_id: {}", bucket_id);
        let all_tuples_in_bucket =
            self.all_tuples_in_bucket::<K,V>(bucket_id);

        let mut first_free_row = SearchResult {
            page_id: None,
            row_num: None,
            val: None,
        };

        for (i, page_tuples) in all_tuples_in_bucket.into_iter() {
            let len = page_tuples.len();
            for (row_num, (k,v)) in page_tuples.into_iter().enumerate() {
                if k == key {
                    return SearchResult{
                        page_id: Some(i),
                        row_num: Some(row_num),
                        val: Some(v)
                    }
                }
            }

            if len < self.tuples_per_page {
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
    pub fn allocate_overflow<K,V>(&mut self, bucket_id: usize) -> usize
        where K: DeserializeOwned + Debug,
              V: DeserializeOwned + Debug {
        // Write next of old page
        self.buffer.next = Some(self.free_page);
        self.write_buffer();

        let physical_index = self.allocate_new_page::<K,V>();
        self.bucket_to_page.push(physical_index);
        println!("{}'s next: physical_index: {}", self.buffer.id, physical_index);

        self.get_page(physical_index);
        self.buffer.prev = Some(self.bucket_to_page(bucket_id));
        self.write_buffer();


        // virtual address
        physical_index - 1
    }

    pub fn put<K,V>(&mut self, bucket_id: usize, key: K, val: V)
        where K: Serialize,
              V: Serialize {
        println!("[put] bucket_id: {}", bucket_id);
        self.get_bucket(bucket_id);
        let key_size = mem::size_of::<K>() as u64;
        let val_size = mem::size_of::<V>() as u64;
        let key_bytes = serialize(&key, Bounded(key_size)).unwrap();
        let val_bytes = serialize(&val, Bounded(val_size)).unwrap();
        self.dirty = true;
        self.buffer.put(&key_bytes, &val_bytes);
    }

    /// Write out page in `buffer` to file.
    pub fn write_buffer(&mut self) {
        self.dirty = false;
        self.write_header();
        DbFile::write_page(&mut self.file,
                           self.page_id.expect("No page buffered"),
                           &self.buffer.storage);
    }

    /// Returns a vec of (page_id, tuples_in_vec). ie. each inner
    /// vector represents the tuples in a page in the bucket.
    fn all_tuples_in_bucket<K, V>(&mut self, bucket_id: usize)
                                  -> Vec<(usize, Vec<(K,V)>)>
        where K: DeserializeOwned + Debug,
              V: DeserializeOwned + Debug {
        self.get_bucket(bucket_id);
        let mut records = Vec::new();

        let mut page_tuples = vec![];
        for i in 0..self.buffer.num_tuples {
            let (k, v) = self.buffer.read_tuple(i);
            println!("k,v = {:?}", (k,v));
            let (dk, dv) : (K, V) = deserialize_kv::<K,V>(&k, &v);
            page_tuples.push((dk, dv));
        }
        records.push((self.page_id.unwrap(), page_tuples));

        while let Some(page_id) = self.buffer.next {
            if page_id == 0 {
                break;
            }

            self.get_page(page_id);
            let mut page_tuples = vec![];
            for i in 0..self.buffer.num_tuples {
                let (k, v) = self.buffer.read_tuple(i);
                let (dk, dv) : (K, V) = deserialize_kv::<K,V>(&k, &v);

                page_tuples.push((dk, dv));
            }
            records.push((page_id, page_tuples));
        }

        records
    }

    /// Allocate a new page.
    fn allocate_new_page<K,V>(&mut self) -> usize {
        let page_id = self.free_page;
        let keysize = mem::size_of::<K>();
        let valsize = mem::size_of::<V>();
        let new_page = Page::new(keysize, valsize);
        mem::replace(&mut self.buffer, new_page);
        self.buffer.id = page_id;
        self.page_id = Some(page_id);
        self.dirty = false;
        self.write_buffer();
        self.free_page += 1;

        page_id
    }

    // NOTE: Old pages are not reclaimed at the moment
    pub fn clear_bucket<K,V>(&mut self, bucket_id: usize) -> Vec<(K,V)>
        where K: DeserializeOwned + Debug,
              V: DeserializeOwned + Debug {
        let page_id = self.bucket_to_page(bucket_id);
        let keysize = mem::size_of::<K>();
        let valsize = mem::size_of::<V>();
        let new_page = Page::new(keysize, valsize);
        let tuples = flatten(self.all_tuples_in_bucket::<K,V>(bucket_id));
        mem::replace(&mut self.buffer, new_page);
        self.buffer.id = page_id;
        self.page_id = Some(page_id);
        self.dirty = false;
        self.write_buffer();

        tuples
    }

    pub fn allocate_new_bucket<K,V>(&mut self)
        where K: DeserializeOwned + Debug,
              V: DeserializeOwned + Debug {
        let page_id = self.allocate_new_page::<K,V>();
        self.bucket_to_page.push(page_id);
        // println!("{:?}", self.bucket_to_page);
    }
}

#[cfg(test)]
mod tests {
    use DbFile;

    #[test]
    fn dbfile_tests () {
        // let mut bp = DbFile::new::<i32, String>("/tmp/buff");
        // bp.write_tuple(0, 14, String::from("samrat"));
        // bp.write_tuple(1, 12, String::from("foo"));
        // bp.write_buffer();
        // let v = bp.read_tuple::<i32, String>(1);
        // bp.all_tuples_in_page::<i32, String>(1);
        // bp.write_page(0, &bp.buffer.storage);
        assert_eq!(1+1,2);
    }
}
