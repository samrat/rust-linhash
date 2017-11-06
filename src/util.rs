use std::fmt::Debug;
use std::mem::{transmute,size_of};

pub fn mem_move(dest: &mut [u8], src: &[u8]) {
    for (d, s) in dest.iter_mut().zip(src) {
        *d = *s
    }
}

pub fn usize_to_bytearray(n: usize) -> [u8; 8] {
    unsafe {
        transmute::<usize, [u8;8]>(n)
    }
}

pub fn i32_to_bytearray(n: i32) -> [u8; 4] {
    unsafe {
        transmute::<i32, [u8;4]>(n)
    }
}

pub fn usize_vec_to_bytevec(v: Vec<usize>) -> Vec<u8> {
    let mut bv : Vec<u8> = vec![];
    for i in v {
        bv.append(&mut usize_to_bytearray(i).to_vec());
    }
    bv
}

pub fn bytevec_to_usize_vec(b: Vec<u8>) -> Vec<usize> {
    let mut v = vec![];
    for i in 0..(b.len() / 8) {
        v.push(bytearray_to_usize(b[i*8..(i+1)*8].to_vec()));
    }

    v
}

pub fn bytearray_to_usize(b: Vec<u8>) -> usize {
    assert_eq!(b.len(), 8);
    let mut a = [0; 8];

    for i in 0..b.len() {
        a[i] = b[i];
    }

    unsafe {
        transmute::<[u8;8], usize>(a)
    }
}

pub fn slices_eq<T: PartialEq>(s1: &[T], s2: &[T]) -> bool {
    s1.iter().zip(s2).all(|(a,b)| a == b)
}
