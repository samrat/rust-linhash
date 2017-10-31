use std::io::prelude::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::str;
use std::mem;
use std::fmt::Debug;

use page;
use page::{Page, PAGE_SIZE, HEADER_SIZE};
use util::mem_move;

use bincode::{serialize,
                    Bounded};
use serde::ser::Serialize;
use serde::de::{Deserialize, DeserializeOwned};

pub struct CtrlPage {
    nbuckets: usize,
    nbits: usize,
    items: usize,
}

pub struct DbFile {
    path: String,
    file: File,
    buffer: Page,
    // which page is currently in `buffer`
    page_id: Option<usize>,
    keysize: usize,
    valsize: usize,
    tuples_per_page: usize,
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
        let total_size = keysize + valsize;
        let tuples_per_page = PAGE_SIZE / total_size;
        DbFile {
            path: String::from(filename),
            file: file,
            buffer: Page::new(),
            page_id: None,
            keysize: keysize,
            valsize: valsize,
            tuples_per_page: tuples_per_page,
        }
    }

    fn read_header(&mut self) {
        let num_tuples : usize = page::deserialize(&self.buffer.storage[0..HEADER_SIZE]).unwrap();
        self.buffer.num_tuples = num_tuples;
    }

    fn write_header(&mut self) {
        mem_move(&mut self.buffer.storage[0..HEADER_SIZE],
                 &serialize(&self.buffer.num_tuples, Bounded(8)).unwrap());
    }

    // Reads page to self.buffer
    pub fn get_page(&mut self, page_id: usize) {
        match self.page_id {
            Some(0) => (),
            Some(_) | None => {
                let offset = (page_id * PAGE_SIZE) as u64;
                self.file.seek(SeekFrom::Start(offset))
                    .expect("Could not seek to offset");
                self.file.read(&mut self.buffer.storage)
                    .expect("Could not read file");
                self.page_id = Some(page_id);
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
        self.get_page(0);
        self.buffer.write_tuple::<K,V>(row_num, key, val)
    }

    pub fn read_tuple<K: DeserializeOwned + Debug,
                      V: DeserializeOwned + Debug> (&mut self, row_num: usize) -> V {
        self.get_page(0);
        self.buffer.read_tuple::<K,V>(row_num)
    }

    /// Write out page in `buffer` to file.
    pub fn write_buffer(&mut self) {
        self.write_header();
        DbFile::write_page(&mut self.file,
                           self.page_id.expect("No page buffered"),
                           &self.buffer.storage);
    }
}
