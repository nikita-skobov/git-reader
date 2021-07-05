use super::OidMap;
use std::mem::MaybeUninit;

/// 65536
pub const B16: usize = 2usize.pow(16);
/// 32768
pub const B15: usize = 2usize.pow(15);
/// 16384
pub const B14: usize = 2usize.pow(14);
/// 8192 
pub const B13: usize = 2usize.pow(13);
/// 4096 
pub const B12: usize = 2usize.pow(12);
/// 2048 
pub const B11: usize = 2usize.pow(11);
/// 1024 
pub const B10: usize = 2usize.pow(10);
/// 512 
pub const B9:  usize = 2usize.pow(9);
/// 256 
pub const B8:  usize = 2usize.pow(8);


pub const fn bitmask(n: usize) -> u128 {
    match n {
        B8 =>  0b11111111_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        B9 =>  0b11111111_10000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        B10 => 0b11111111_11000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        B11 => 0b11111111_11100000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        B12 => 0b11111111_11110000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        B13 => 0b11111111_11111000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        B14 => 0b11111111_11111100_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        B15 => 0b11111111_11111110_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        B16 => 0b11111111_11111111_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
        // this is not safe. basically
        // dont use this library with const N
        // of anything other than B8-B14...
        _ => 0u128
    }
}
pub const fn bitshift(n: usize) -> usize {
    match n {
        B8 => 120,
        B9 => 119,
        B10 => 118,
        B11 => 117,
        B12 => 116,
        B13 => 115,
        B14 => 114,
        B15 => 113,
        B16 => 112,
        _ => 0,
    }
}


impl<T, const N: usize> Default for OidMap<T, N> {   
    fn default() -> Self {
        // originally I had a proc macro to generate large arrays, but compilation
        // time was wayyyy too slow... so instead we create it dynamically.
        // this snippet was taken from:
        // https://docs.rs/array-init/2.0.0/src/array_init/lib.rs.html#1-374
        let mut arr: MaybeUninit<[Vec<(u128, T)>; N]> = MaybeUninit::uninit();
        let mut ptr_i = arr.as_mut_ptr() as *mut Vec<(u128, T)>;
        let root = unsafe {
            for _ in 0..N {
                let val = vec![];
                ptr_i.write(val);
                ptr_i = ptr_i.add(1);
            }
            arr.assume_init()
        };
        Self { root }
    }
}

