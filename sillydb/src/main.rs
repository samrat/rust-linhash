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
    h.put("Spin".as_bytes(), &i32_to_bytearray(9));
    h.put("Axis".as_bytes(), &i32_to_bytearray(6));
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
    h.put("rust2".as_bytes(), &[51]);
    h.put("rust3".as_bytes(), &[52]);
    h.put("rust4".as_bytes(), &[53]);
    h.put("rust5".as_bytes(), &[54]);

    h.update("rust1".as_bytes(), &[99]);
    h.put("xinu3".as_bytes(), &[24]);
    h.close();

    measure_perf(3);

    println!("{:?}", h.get("rust3".as_bytes()));
}
