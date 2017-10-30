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
const NUM_PAGES : usize = 100;      // constant for now

pub struct Page {
    storage: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Page {
        Page {
            storage: [0; PAGE_SIZE]
        }
    }
}

pub struct BufferPool {
    file: File,
    // pages: Vec<Option<Page>>,
    buffer: Page,
}

impl BufferPool {
    pub fn new(filename: &str) -> BufferPool {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename);
        let file = match file {
            Ok(f) => f,
            Err(e) => panic!(e),
        };
        // let mut pages = Vec::with_capacity(NUM_PAGES);
        // for _ in 0..NUM_PAGES {
        //     pages.push(None);
        // }
        BufferPool {
            file: file,
            // pages: pages,
            buffer: Page::new(),
        }
    }

    // Reads page to self.buffer
    pub fn get_page(&mut self, page_id: usize) {
        let offset = (page_id * PAGE_SIZE) as u64;
        self.file.seek(SeekFrom::Start(offset));
        self.file.read(&mut self.buffer.storage);
        println!("{:?}", str::from_utf8(&self.buffer.storage));
    }

    // Writes data in self.buffer into page `page_id`
    pub fn write_page(&mut self, page_id: usize, data: &[u8]) {
        let offset = (page_id * PAGE_SIZE) as u64;
        self.file.seek(SeekFrom::Start(offset));
        println!("wrote {:?} bytes from offset {}",
                 self.file.write(data), offset);
        self.file.flush();
    }

    fn mem_move(dest: &mut [u8], src: &[u8]) {
        for (d, s) in dest.iter_mut().zip(src) {
            *d = *s
        }
    }

    pub fn write_tuple(&mut self, row_num: usize, t: (i32, &str, &str)) {
        let id_size = mem::size_of::<i32>();
        let name_size = 32;
        let email_size = 255;
        let total_size = id_size + name_size + email_size;

        let row_offset = row_num * total_size;
        println!("row_offset: {}", row_offset);
        let id_offset = row_offset;
        
        let name_offset = id_offset + id_size;
        let email_offset = name_offset + name_size;
        let row_end = email_offset + email_size;

        // convert i32 to little endian
        let mut wtr = vec![];
        wtr.write_i32::<LittleEndian>(t.0).unwrap();
        println!("{:?}", wtr);

        BufferPool::mem_move(&mut self.buffer.storage[id_offset..name_offset], &wtr);
        BufferPool::mem_move(&mut self.buffer.storage[name_offset..email_offset], &t.1.as_bytes());
        BufferPool::mem_move(&mut self.buffer.storage[email_offset..row_end], &t.2.as_bytes());

        println!("tuple_mem: {:?}", str::from_utf8(&self.buffer.storage));
    }

    pub fn read_tuple(&self, row_num: usize) {
        let id_size = mem::size_of::<i32>();
        let name_size = 32;
        let email_size = 255;
        let total_size = id_size + name_size + email_size;

        let row_offset = row_num * total_size;
        println!("row_offset: {}", row_offset);
        let id_offset = row_offset;
        
        let name_offset = id_offset + id_size;
        let email_offset = name_offset + name_size;
        let row_end = email_offset + email_size;

        let mut id = vec![0; id_size];
        let mut name = vec![0; name_size];
        let mut email = vec![0; email_size];

        BufferPool::mem_move(&mut id, &self.buffer.storage[id_offset..name_offset]);
        let mut rdr = Cursor::new(id);
        let id = rdr.read_u16::<LittleEndian>().unwrap();
        BufferPool::mem_move(&mut name, &self.buffer.storage[name_offset..email_offset]);
        BufferPool::mem_move(&mut email, &self.buffer.storage[email_offset..row_end]);
        println!("read: {} {:?} {:?}", id, str::from_utf8(&name), str::from_utf8(&email));
    }

    // TEMP
    pub fn write_buffer(&mut self) {
        write_page("/tmp/tupletest", 0, &self.buffer.storage);
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
