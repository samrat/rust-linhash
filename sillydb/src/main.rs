extern crate linhash;

use linhash::LinHash;
use std::time::Instant;
use std::fs;
use linhash::util::*;

#[allow(dead_code)]
fn measure_perf(num_iters: i32) {
    // in each iteration, insert a larger number of records to see how
    // `insert` and `lookup` performs. `insert` should be O(n) and
    // `lookup` should be O(1).
    for i in 1..num_iters {
        let now = Instant::now();
        let mut h2 = LinHash::open("/tmp/measure_perf", 4, 4);
        for k in 0..(10000*i) {
            h2.put(&linhash::util::i32_to_bytearray(k),
                   &linhash::util::i32_to_bytearray(k+1));
        }

        let time_get = Instant::now();
        for k in 1000..9000 {
            assert_eq!(h2.get(&linhash::util::i32_to_bytearray(k)),
                       Some(linhash::util::i32_to_bytearray(k+1).to_vec()));
            println!("{}", k);
        }
        let time_get_done = Instant::now();
        println!("[get]{} million records {:?}", i, time_get_done.duration_since(time_get));

        let new_now = Instant::now();
        println!("[insert+get]{} million records {:?}", i, new_now.duration_since(now));
        h2.close();
        fs::remove_file("/tmp/measure_perf");
    }

}

fn main() {
    let mut h = LinHash::open("/tmp/main_tests", 32, 4);
    h.put(b"Spin", &i32_to_bytearray(9));
    h.put(b"Axis", &i32_to_bytearray(6));
    h.put(b"foo", &[14]);
    h.put(b"bar", &[15]);
    h.put(b"linear", &[16]);
    h.put(b"hashing", &[17]);
    h.put(b"disk", &[18]);
    h.put(b"space", &[19]);
    h.put(b"random", &[20]);
    h.put(b"keys", &[21]);
    h.put(b"samrat", &[22]);
    h.put(b"linhash", &[21]);
    h.put(b"rust", &[21]);
    h.put(b"3:30", &[21]);
    h.put(b"xinu", &[21]);
    h.put(b"linhash1", &[21]);
    h.put(b"rust1", &[22]);
    h.put(b"rust2", &[51]);
    h.put(b"rust3", &[52]);
    h.put(b"rust4", &[53]);
    h.put(b"rust5", &[54]);

    h.update(b"rust1", &[99]);
    h.put(b"xinu3", &[24]);
    h.close();

    measure_perf(2);

    println!("{:?}", h.get("rust3".as_bytes()));
}
