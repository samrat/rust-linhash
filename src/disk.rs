use std::io::prelude::*;
use std::fs::File;
use std::fs::OpenOptions;

use std::io::SeekFrom;
use std::str;
use std::mem;

extern crate byteorder;
use std::io::Cursor;
use self::byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};

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
}

impl DbFile {
    pub fn new(filename: &str) -> DbFile {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename);
        let file = match file {
            Ok(f) => f,
            Err(e) => panic!(e),
        };
        DbFile {
            path: String::from(filename),
            file: file,
            buffer: Page::new(),
            page_id: None,
        }
    }

    fn read_header(&mut self) {
        let mut header = vec![0; HEADER_SIZE];
        DbFile::mem_move(&mut header,
                         &self.buffer.storage[0..HEADER_SIZE]);
        println!("{:?}", header);
        let mut rdr = Cursor::new(header);
        let num_tuples = rdr.read_u64::<LittleEndian>().unwrap();
        self.buffer.num_tuples = num_tuples as usize;
        println!("{}", num_tuples);
    }

    fn write_header(&mut self) {
        let mut wtr = vec![];
        wtr.write_u64::<LittleEndian>(self.buffer.num_tuples as u64).unwrap();
        DbFile::mem_move(&mut self.buffer.storage[0..HEADER_SIZE], &wtr);
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
                println!("{:?}", str::from_utf8(&self.buffer.storage));
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

    pub fn write_tuple(&mut self, row_num: usize, t: (i32, &str)) {
        self.get_page(0);
        // TODO: check if it's not just a overwrite
        self.buffer.num_tuples += 1;

        let key_size = mem::size_of::<i32>();
        let val_size = 32;
        let total_size = key_size + val_size;

        let row_offset = row_num * total_size;
        let header_offset = row_offset;
        let key_offset = header_offset + HEADER_SIZE;
        let val_offset = key_offset + key_size;
        let row_end = val_offset + val_size;
        // println!("row_offset: {}", row_offset);
        
        // convert i32 to little endian
        let mut wtr = vec![];
        wtr.write_i32::<LittleEndian>(t.0).unwrap();
        println!("{:?}", wtr);

        DbFile::mem_move(&mut self.buffer.storage[key_offset..val_offset], &wtr);
        DbFile::mem_move(&mut self.buffer.storage[val_offset..row_end], &t.1.as_bytes());

        println!("tuple_mem: {:?}", str::from_utf8(&self.buffer.storage));
    }

    pub fn read_tuple(&mut self, row_num: usize) {
        self.get_page(0);

        let key_size = mem::size_of::<i32>();
        let val_size = 32;
        let total_size = key_size + val_size;

        let row_offset = row_num * total_size;
        let header_offset = row_offset;
        let key_offset = header_offset + HEADER_SIZE;
        let val_offset = key_offset + key_size;
        let row_end = val_offset + val_size;
        println!("row_offset: {}", row_offset);
        
        let mut key = vec![0; key_size];
        let mut val = vec![0; val_size];

        DbFile::mem_move(&mut key, &self.buffer.storage[key_offset..val_offset]);
        let mut rdr = Cursor::new(key);
        let key = rdr.read_u16::<LittleEndian>().unwrap();
        DbFile::mem_move(&mut val, &self.buffer.storage[val_offset..row_end]);
        println!("read: {} {:?}", key, str::from_utf8(&val));
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
