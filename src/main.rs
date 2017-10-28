extern crate linhash;

use linhash::LinHash;

fn main() {
    let mut h : LinHash<&str, i32> = LinHash::new();
    h.put("hello", 12);
    h.put("there", 13);
    h.put("foo", 14);
    h.put("bar", 15);
    println!("{:?}", h.get("hello"));
}
