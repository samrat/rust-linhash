extern crate linhash;

mod disk;
use disk::write_page;
use disk::DbFile;

use linhash::LinHash;
use std::time::Instant;
use std::fs::File;
use std::fs::OpenOptions;

#[allow(dead_code)]
fn measure_perf(num_iters: i32) {
    // in each iteration, insert a larger number of records to see how
    // `insert` and `lookup` performs. `insert` should be O(n) and
    // `lookup` should be O(1).
    for i in 1..num_iters {
        let now = Instant::now();
        let mut h2 : LinHash<i32, i32> = LinHash::new();
        for k in 0..(1000000*i) {
            h2.put(k, k+1);
        }

        let time_get = Instant::now();
        for k in 10000..90000 {
            h2.get(k);
        }
        let time_get_done = Instant::now();
        println!("[get]{} million records {:?}", i, time_get_done.duration_since(time_get));

        let new_now = Instant::now();
        println!("[insert+get]{} million records {:?}", i, new_now.duration_since(now));
    }
}

fn main() {
    let mut h : LinHash<&str, i32> = LinHash::new();
    h.put("hello", 12);
    h.put("there", 13);
    h.put("foo", 14);
    h.put("bar", 15);
    h.remove("bar");

    // measure_perf(4);

    println!("{:?}", h.get("bar"));

    let mut bp = DbFile::new("/tmp/buff");
    // bp.write_page(0, b"flula");
    // bp.get_page(0);
    bp.write_tuple(0, (1, "samrat", "samrat@samrat.me"));
    bp.write_tuple(1, (12, "foo", "foobar@example.com"));
    bp.write_buffer();
    bp.read_tuple(1);
    // bp.write_page(0, &bp.buffer.storage);
}
