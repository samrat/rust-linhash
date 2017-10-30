use std::io::prelude::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::str;
use std::mem;
use std::fmt::Debug;


extern crate serde;
extern crate bincode;
use self::bincode::{serialize, deserialize as bin_deserialize,
                    Bounded};
use self::serde::ser::Serialize;
use self::serde::de::{Deserialize, DeserializeOwned};

const PAGE_SIZE : usize = 4096;     // bytes
const HEADER_SIZE : usize = 8;      // bytes

pub struct Page {
    storage: [u8; PAGE_SIZE],
    num_tuples: usize,
}

impl Page {
    pub fn new() -> Page {
        Page {
            num_tuples: 0,
            storage: [0; PAGE_SIZE]
        }
    }
}

pub struct DbFile {
    path: String,
    file: File,
    buffer: Page,
    // which page is currently in `buffer`
    page_id: Option<usize>,
    keysize: usize,
    valsize: usize,
}

pub fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
    where T: Deserialize<'a>
{
    bin_deserialize(bytes)
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
        DbFile {
            path: String::from(filename),
            file: file,
            buffer: Page::new(),
            page_id: None,
            keysize: keysize,
            valsize: valsize,
        }
    }

    fn read_header(&mut self) {
        let num_tuples : usize = deserialize(&self.buffer.storage[0..HEADER_SIZE]).unwrap();
        self.buffer.num_tuples = num_tuples;
    }

    fn write_header(&mut self) {
        DbFile::mem_move(&mut self.buffer.storage[0..HEADER_SIZE],
                         &serialize(&self.buffer.num_tuples, Bounded(8)).unwrap());
    }

    // Reads page to self.buffer
    pub fn get_page(&mut self, page_id: usize) {
        match self.page_id {
            Some(0) => (),
            Some(_) | None => {
                let offset = (page_id * PAGE_SIZE) as u64;
                self.file.seek(SeekFrom::Start(offset));
                self.file.read(&mut self.buffer.storage);
                self.page_id = Some(page_id);
                self.read_header();
            },
        }
    }

    // Writes data in self.buffer into page `page_id`
    pub fn write_page(mut file: &File, page_id: usize, data: &[u8]) {
        let offset = (page_id * PAGE_SIZE) as u64;
        file.seek(SeekFrom::Start(offset));
        println!("wrote {:?} bytes from offset {}",
                 file.write(data), offset);
        file.flush();
    }

    fn mem_move(dest: &mut [u8], src: &[u8]) {
        for (d, s) in dest.iter_mut().zip(src) {
            *d = *s
        }
    }

    pub fn write_tuple<K, V>(&mut self, row_num: usize, key: K, val: V)
        where K: Serialize,
              V: Serialize {
        self.get_page(0);
        // TODO: check if it's not just a overwrite
        self.buffer.num_tuples += 1;

        let key_size = mem::size_of::<K>();
        let val_size = mem::size_of::<V>();
        let total_size = key_size + val_size;

        let row_offset = row_num * total_size;
        let header_offset = row_offset;
        let key_offset = header_offset + HEADER_SIZE;
        let val_offset = key_offset + key_size;
        let row_end = val_offset + val_size;

        println!("[write_tuple]keyoffset: {}, valoffset:{}", key_offset, val_offset);

        // The maximum sizes of the encoded key and val.
        let key_limit = Bounded(key_size as u64);
        let val_limit = Bounded(val_size as u64);

        DbFile::mem_move(&mut self.buffer.storage[key_offset..val_offset],
                         &serialize(&key, key_limit).unwrap());
        DbFile::mem_move(&mut self.buffer.storage[val_offset..row_end],
                         &serialize(&val, val_limit).unwrap());
    }

    pub fn read_tuple<K: DeserializeOwned + Debug,
                      V: DeserializeOwned + Debug> (&mut self, row_num: usize) -> V {
        self.get_page(0);

        let key_size = mem::size_of::<K>();
        let val_size = mem::size_of::<V>();
        let total_size = key_size + val_size;

        let row_offset = row_num * total_size;
        let header_offset = row_offset;
        let key_offset = header_offset + HEADER_SIZE;
        let val_offset = key_offset + key_size;
        let row_end = val_offset + val_size;

        let decoded_key : K = deserialize(&self.buffer.storage[key_offset..val_offset]).unwrap();
        let decoded_val : V = deserialize(&self.buffer.storage[val_offset..row_end]).unwrap();

        println!("read: {:?} {:?}", decoded_key, decoded_val);
        decoded_val
    }

    /// Write out page in `buffer` to file.
    pub fn write_buffer(&mut self) {
        self.write_header();
        DbFile::write_page(&mut self.file,
                           self.page_id.expect("No page buffered"),
                           &self.buffer.storage);
    }
}

pub fn write_page(filename: &str, page_id: usize, buffer: &[u8]) {
    let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename);
    let mut f = match file {
            Ok(f) => f,
            Err(e) => panic!(e),
        };
    let offset = (page_id * PAGE_SIZE) as u64;
    f.seek(SeekFrom::Start(offset));
    println!("{:?}", buffer);
    println!("wrote {:?} bytes from offset {}", f.write(buffer), offset);
    f.flush();
}

pub fn read_page(mut f: File, page_id: usize) {
    let offset = (page_id * PAGE_SIZE) as u64;
    f.seek(SeekFrom::Start(offset));
    let mut buffer = [0; PAGE_SIZE];
    println!("reading from offset {}", offset);
    f.read(&mut buffer);
    println!("{:?}", str::from_utf8(&buffer));
}
