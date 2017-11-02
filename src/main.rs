extern crate linhash;
extern crate serde;
extern crate bincode;

mod disk;
mod page;
mod util;
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
        let mut h2 : LinHash<i32, i32> = LinHash::new("/tmp/measure_perf");
        for k in 0..(10000*i) {
            h2.put(k, k+1);
        }

        let time_get = Instant::now();
        for k in 100..900 {
            h2.get(k);
        }
        let time_get_done = Instant::now();
        println!("[get]{} million records {:?}", i, time_get_done.duration_since(time_get));

        let new_now = Instant::now();
        println!("[insert+get]{} million records {:?}", i, new_now.duration_since(now));
    }
}

fn main() {
    let mut h : LinHash<String, i32> = LinHash::new("/tmp/main_tests");
    h.put(String::from("hello"), 12);
    h.put(String::from("there"), 13);
    h.put(String::from("foo"), 14);
    h.put(String::from("bar"), 15);
    // h.remove(String::from("bar"));

    // measure_perf(4);

    println!("{:?}", h.get(String::from("hello")));
}
