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

pub struct DbFile {
    path: String,
    ctrl_file: File,
    file: File,
    ctrl_buffer: Page,
    pub buffer: Page,
    // which page is currently in `buffer`
    page_id: Option<usize>,
    tuples_per_page: usize,
    // changes made to `buffer`?
    dirty: bool,
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

        let mut ctrl_filename = String::from(filename);
        ctrl_filename.push_str("_ctrl");

        let ctrl_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(ctrl_filename);
        let ctrl_file = match ctrl_file {
            Ok(f) => f,
            Err(e) => panic!(e),
        };
        let keysize = mem::size_of::<K>();
        let valsize = mem::size_of::<V>();
        let total_size = keysize + valsize;
        let tuples_per_page = PAGE_SIZE / total_size;
        DbFile {
            path: String::from(filename),
            file: file,
            ctrl_file: ctrl_file,
            ctrl_buffer: Page::new(0, 0),
            buffer: Page::new(keysize, valsize),
            page_id: None,
            tuples_per_page: tuples_per_page,
            dirty: false,
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
        (nbits, nitems, nbuckets)
    }

    pub fn write_ctrlpage(&mut self,
                          (nbits, nitems, nbuckets):
                          (usize, usize, usize)) {
        self.get_ctrl_page();
        let nbits_bytes = &serialize(&nbits, Bounded(8)).unwrap();
        let nitems_bytes = &serialize(&nitems, Bounded(8)).unwrap();
        let nbuckets_bytes = &serialize(&nbuckets, Bounded(8)).unwrap();
        println!("nbits: {:?} nitems: {:?} nbuckets: {:?}", nbits_bytes,
                 nitems_bytes, nbuckets_bytes);
        mem_move(&mut self.ctrl_buffer.storage[0..8],
                 nbits_bytes);
        mem_move(&mut self.ctrl_buffer.storage[8..16],
                 nitems_bytes);
        mem_move(&mut self.ctrl_buffer.storage[16..24],
                 nbuckets_bytes);
        DbFile::write_page(&mut self.ctrl_file,
                           0,
                           &self.ctrl_buffer.storage);
    }

    fn read_header(&mut self) {
        let num_tuples : usize = deserialize(&self.buffer.storage[0..8]).unwrap();
        self.buffer.num_tuples = num_tuples;
    }

    fn write_header(&mut self) {
        mem_move(&mut self.buffer.storage[0..8],
                 &serialize(&self.buffer.num_tuples, Bounded(8)).unwrap());
    }

    pub fn get_ctrl_page(&mut self) {
        self.ctrl_file.seek(SeekFrom::Start(0))
            .expect("Could not seek to offset");
        self.ctrl_file.read(&mut self.ctrl_buffer.storage)
            .expect("Could not read file");
    }

    // Reads page to self.buffer
    pub fn get_page(&mut self, page_id: usize) {
        match self.page_id {
            Some(p) if p == page_id => (),
            Some(_) | None => {
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

    // Writes data in self.buffer into page `page_id`
    pub fn write_page(mut file: &File, page_id: usize, data: &[u8]) {
        let offset = (page_id * PAGE_SIZE) as u64;
        file.seek(SeekFrom::Start(offset))
            .expect("Could not seek to offset");
        println!("wrote {:?} bytes from offset {}",
                 file.write(data), offset);
        file.flush().expect("flush failed");
    }

    pub fn write_tuple<K, V>(&mut self, row_num: usize, key: K, val: V)
        where K: Serialize,
              V: Serialize {
        // TODO: get rid of row_num
        let page_index = (row_num / self.tuples_per_page);
        self.get_page(page_index);

        // The maximum sizes of the encoded key and val.
        let key_limit = Bounded(mem::size_of::<K>() as u64);
        let val_limit = Bounded(mem::size_of::<V>() as u64);

        self.dirty = true;
        self.buffer.put(&serialize(&key, key_limit).unwrap(),
                        &serialize(&val, val_limit).unwrap())
    }

    pub fn get<K, V>(&mut self, page_id: usize, key: K) -> Option<V>
        where K: Serialize + Debug,
              V: DeserializeOwned + Debug {
        println!("[get] page_id: {}", page_id);
        self.get_page(page_id);
        let key_size = mem::size_of::<K>() as u64;
        let key_bytes = serialize(&key, Bounded(key_size)).unwrap();

        if let Some(val_bytes) = self.buffer.get(&key_bytes) {
            Some(deserialize(&val_bytes).unwrap())
        } else {
            None
        }
    }

    pub fn put<K,V>(&mut self, page_id: usize, key: K, val: V)
        where K: Serialize,
              V: Serialize {
        println!("[put] page_id: {}", page_id);
        self.get_page(page_id);
        let key_size = mem::size_of::<K>() as u64;
        let val_size = mem::size_of::<V>() as u64;
        let key_bytes = serialize(&key, Bounded(key_size)).unwrap();
        let val_bytes = serialize(&val, Bounded(val_size)).unwrap();
        self.dirty = true;
        self.buffer.put(&key_bytes, &val_bytes);
        // TODO: avoid writing to file after every update. ie. only
        // write to file once the page needs to get evicted from
        // `buffer`.
        self.write_buffer();
    }

    /// Write out page in `buffer` to file.
    pub fn write_buffer(&mut self) {
        self.dirty = false;
        self.write_header();
        DbFile::write_page(&mut self.file,
                           self.page_id.expect("No page buffered"),
                           &self.buffer.storage);
    }

    pub fn all_tuples_in_page<K, V>(&mut self, page_id: usize)
                                    -> Vec<(K,V)>
        where K: DeserializeOwned + Debug,
              V: DeserializeOwned + Debug {
        self.get_page(page_id);
        let mut records = Vec::new();
        for i in 0..self.buffer.num_tuples {
            let (k, v) = self.buffer.read_tuple(i);
            let (dk, dv) : (K, V) = deserialize_kv::<K,V>(&k, &v);
            records.push((dk, dv));
        }

        records
    }

    pub fn allocate_new_page<K,V>(&mut self, page_id: usize) {
        let keysize = mem::size_of::<K>();
        let valsize = mem::size_of::<V>();
        let new_page = Page::new(keysize, valsize);
        mem::replace(&mut self.buffer, new_page);
        self.buffer.id = page_id;
        self.page_id = Some(page_id);
        self.dirty = false;
    }
}
