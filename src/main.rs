extern crate linhash;

mod disk;
mod page;
mod util;
use disk::DbFile;

use linhash::LinHash;
use std::time::Instant;

#[allow(dead_code)]
// fn measure_perf(num_iters: i32) {
//     // in each iteration, insert a larger number of records to see how
//     // `insert` and `lookup` performs. `insert` should be O(n) and
//     // `lookup` should be O(1).
//     for i in 1..num_iters {
//         let now = Instant::now();
//         let mut h2 : LinHash<i32, i32> = LinHash::new("/tmp/measure_perf");
//         for k in 0..(1000*i) {
//             h2.put(k, k+1);
//         }

//         let time_get = Instant::now();
//         for k in 1000..9000 {
//             assert_eq!(h2.get(k), Some(k+1));
//         }
//         let time_get_done = Instant::now();
//         println!("[get]{} million records {:?}", i, time_get_done.duration_since(time_get));

//         let new_now = Instant::now();
//         println!("[insert+get]{} million records {:?}", i, new_now.duration_since(now));
//         // h2.close();
//     }

// }

fn main() {
    let mut h = LinHash::open("/tmp/main_tests", 32, 4);
    h.put("hello".as_bytes(), &[12]);
    h.put("there".as_bytes(), &[13]);
    h.put("foo".as_bytes(), &[14]);
    h.put("bar".as_bytes(), &[15]);
    h.put("linear".as_bytes(), &[16]);
    h.put("hashing".as_bytes(), &[17]);

    h.put("disk".as_bytes(), &[18]);
    h.put("space".as_bytes(), &[19]);
    h.put("random".as_bytes(), &[20]);
    h.put("keys".as_bytes(), &[21]);
    h.put("samrat".as_bytes(), &[22]);
    h.put("linhash".as_bytes(), &[21]);
    h.put("rust".as_bytes(), &[21]);
    h.put("3:30".as_bytes(), &[21]);
    h.put("xinu".as_bytes(), &[21]);
    h.put("linhash1".as_bytes(), &[21]);
    h.put("rust1".as_bytes(), &[22]);

    h.update("rust1".as_bytes(), &[99]);
    h.put("xinu3".as_bytes(), &[24]);

    // measure_perf(4);

    println!("{:?}", h.get("rust1".as_bytes()));
}
