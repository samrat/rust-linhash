extern crate linhash;
extern crate serde;
extern crate bincode;

mod disk;
mod page;
mod util;
mod bucket;
use disk::DbFile;

use linhash::LinHash;
use std::time::Instant;


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

    let mut bp = DbFile::new::<i32, String>("/tmp/buff");
    bp.write_tuple(0, 14, String::from("samrat"));
    bp.write_tuple(1, 12, String::from("foo"));
    bp.write_buffer();
    let v = bp.read_tuple::<i32, String>(1);
    bp.all_tuples_in_buffer::<i32, String>();
    // bp.write_page(0, &bp.buffer.storage);

    println!("{:?}", bucket::Bucket::<i32, String>::from_page(bp.buffer));
}
