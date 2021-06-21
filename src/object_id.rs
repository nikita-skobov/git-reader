

use std::io;
use crate::{ioerre, ioerr};

/// NOTE: we represent sha1 hash keys as u128, when they are really
/// 160 bits. We do this because even at 128 bits the chance of
/// a collision is miniscule.
/// (see: https://stackoverflow.com/questions/1867191/probability-of-sha1-collisions)
pub type Oid = u128;

/// A full representation of an Oid. (8 * 20 = 160)
/// a simple type alias and used to convert
/// to an OidTruncated.
pub type OidFull = [u8; 20];

/// A truncated version of OidFull. used to convert
/// to an Oid. (8 * 16 = 128)
pub type OidTruncated = [u8; 16];

/// A hex string of 32 characters. Can be turned into
/// an OidTruncated which can be turned into an Oid
pub struct OidStrTruncated(pub [u8; 32]);

impl Default for OidStrTruncated {
    fn default() -> Self {
        Self([b'0'; 32])
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct PartialOid {
    pub oid: Oid,
    pub shift_by: usize,
    pub oid_shifted: u128,
}

impl PartialOid {
    pub fn from_hash(hash: &str) -> io::Result<PartialOid> {
        let hash_len = hash.len();
        let (oid_str, bits_set) = if hash_len < 32 {
            // not enough bytes, so we need to add 0s to
            // the end:
            let mut oid_str = OidStrTruncated::default();
            oid_str.0[0..hash_len].copy_from_slice(&hash[..].as_bytes());
            (oid_str, hash_len * 4)
        } else {
            // we have enough bytes, copy the entire 32
            let mut oid_str = OidStrTruncated::default();
            oid_str.0[..].copy_from_slice(&hash[0..32].as_bytes());
            (oid_str, 32 * 4)
        };
        let oid = oid_str_truncated_to_oid(oid_str)?;
        // because an Oid is 128 bits, the number of bits set
        // is less than 128, so 128 - bits_set
        // tells us how many bits we need to shift an actual oid by
        // in order to compare it to this partial oid.
        let shift_by = 128 - bits_set;
        let shifted = oid >> shift_by;
        Ok(PartialOid {
            oid,
            shift_by,
            oid_shifted: shifted,
        })
    }

    pub fn matches(&self, oid: Oid) -> bool {
        let shifted = oid >> self.shift_by;
        self.oid_shifted == shifted
    }
}

pub fn hex_u128_to_str(h: Oid) -> String {
    let hash_str = format!("{:x}", h);
    // an oid is 128 bits, so should be 32 hex chars.
    // if we dont have 32 hex chars, we need to prepend 0s:
    let len = hash_str.len();
    if len == 32 {
        return hash_str;
    }

    let prepend_0s = "0".repeat(32 - len);
    format!("{}{}", prepend_0s, hash_str)
}

/// Not very well optimized, I know, but we only need
/// to print full hex strings for debugging purposes probably
pub fn oid_full_to_string(h: OidFull) -> String {
    let mut s = String::with_capacity(40);
    for byte in h.iter() {
        let byte = *byte;
        let hex_str = format!("{:02x}", byte);
        s.push_str(&hex_str);
    }
    s
}

pub fn oid_str_truncated_to_oid(oid_str: OidStrTruncated) -> io::Result<Oid> {
    let oid_str = std::str::from_utf8(&oid_str.0).map_err(|e| ioerr!("{}", e))?;
    let oid = Oid::from_str_radix(oid_str, 16).map_err(|e| ioerr!("{}", e))?;
    Ok(oid)
}

pub fn get_first_byte_of_oid(oid: Oid) -> u8 {
    let mask: u128 = 0xff_00_00_00_00_00_00_00_00_00_00_00_00_00_00_00;
    let masked = oid & mask;
    // shift 120 bits because we want the 8 bits
    // that are in the MSB position
    (masked >> 120) as u8
}

pub fn full_oid_from_str(hash: &str) -> Option<OidFull> {
    let first_40 = hash.get(0..40)?;
    let mut oid_full = OidFull::default();
    for i in 0..20 {
        // this should give us 2 characters at a time:
        let hex_range_start = i * 2;
        let hex_range = hex_range_start..(hex_range_start + 2);
        let hex = &first_40[hex_range];
        // now we parse those 2 hex chars into a u8:
        let byte = u8::from_str_radix(hex, 16).ok()?;
        oid_full[i] = byte;
    }
    Some(oid_full)
}

pub fn hash_str_to_oid(hash: &str) -> io::Result<Oid> {
    let trunc_str = hash.get(0..32)
        .ok_or_else(|| ioerr!("Your hash '{}' must be at least 32 hex chars long", hash))?;
    let mut oid_str_trunc = OidStrTruncated::default();
    oid_str_trunc.0[..].copy_from_slice(&trunc_str[..].as_bytes());
    let oid = oid_str_truncated_to_oid(oid_str_trunc)?;
    Ok(oid)
}

pub fn hash_object_file_and_folder(folder: &str, filename: &str) -> io::Result<Oid> {
    let mut oid_str = OidStrTruncated::default();
    oid_str.0[0..2].copy_from_slice(&folder[0..2].as_bytes());
    oid_str.0[2..32].copy_from_slice(&filename[0..30].as_bytes());
    // now our oid_str is an array of hex characters, 32 long.
    // we can convert that to a string, and then
    // convert to a u128 using from radix:
    oid_str_truncated_to_oid(oid_str)
}

pub fn trunc_oid_to_u128_oid(trunc: OidTruncated) -> Oid {
    let num = u128::from_be_bytes(trunc);
    num
}

pub fn full_oid_to_u128_oid(full: OidFull) -> Oid {
    let mut trunc = OidTruncated::default();
    trunc.copy_from_slice(&full[0..16]);
    trunc_oid_to_u128_oid(trunc)
}

/// Only call this if you know your slice has at least 16 bytes
pub fn full_slice_oid_to_u128_oid(full: &[u8]) -> Oid {
    let mut trunc = OidTruncated::default();
    trunc.copy_from_slice(&full[0..16]);
    trunc_oid_to_u128_oid(trunc)
}

/// 256 values of hex bytes. each value is an array
/// of 2 ascii values that represents that byte in ascii hex.
/// eg: if we want to find the hex code for the value 11,
/// wed look at index 11 in this table and find [48, 98]
/// which is ascii for ['0', 'b'], and "0b" in hex is 11.
pub const HEX_BYTES: &[[u8; 2]; 256] = &[
    [48, 48],
    [48, 49],
    [48, 50],
    [48, 51],
    [48, 52],
    [48, 53],
    [48, 54],
    [48, 55],
    [48, 56],
    [48, 57],
    [48, 97],
    [48, 98],
    [48, 99],
    [48, 100],
    [48, 101],
    [48, 102],
    [49, 48],
    [49, 49],
    [49, 50],
    [49, 51],
    [49, 52],
    [49, 53],
    [49, 54],
    [49, 55],
    [49, 56],
    [49, 57],
    [49, 97],
    [49, 98],
    [49, 99],
    [49, 100],
    [49, 101],
    [49, 102],
    [50, 48],
    [50, 49],
    [50, 50],
    [50, 51],
    [50, 52],
    [50, 53],
    [50, 54],
    [50, 55],
    [50, 56],
    [50, 57],
    [50, 97],
    [50, 98],
    [50, 99],
    [50, 100],
    [50, 101],
    [50, 102],
    [51, 48],
    [51, 49],
    [51, 50],
    [51, 51],
    [51, 52],
    [51, 53],
    [51, 54],
    [51, 55],
    [51, 56],
    [51, 57],
    [51, 97],
    [51, 98],
    [51, 99],
    [51, 100],
    [51, 101],
    [51, 102],
    [52, 48],
    [52, 49],
    [52, 50],
    [52, 51],
    [52, 52],
    [52, 53],
    [52, 54],
    [52, 55],
    [52, 56],
    [52, 57],
    [52, 97],
    [52, 98],
    [52, 99],
    [52, 100],
    [52, 101],
    [52, 102],
    [53, 48],
    [53, 49],
    [53, 50],
    [53, 51],
    [53, 52],
    [53, 53],
    [53, 54],
    [53, 55],
    [53, 56],
    [53, 57],
    [53, 97],
    [53, 98],
    [53, 99],
    [53, 100],
    [53, 101],
    [53, 102],
    [54, 48],
    [54, 49],
    [54, 50],
    [54, 51],
    [54, 52],
    [54, 53],
    [54, 54],
    [54, 55],
    [54, 56],
    [54, 57],
    [54, 97],
    [54, 98],
    [54, 99],
    [54, 100],
    [54, 101],
    [54, 102],
    [55, 48],
    [55, 49],
    [55, 50],
    [55, 51],
    [55, 52],
    [55, 53],
    [55, 54],
    [55, 55],
    [55, 56],
    [55, 57],
    [55, 97],
    [55, 98],
    [55, 99],
    [55, 100],
    [55, 101],
    [55, 102],
    [56, 48],
    [56, 49],
    [56, 50],
    [56, 51],
    [56, 52],
    [56, 53],
    [56, 54],
    [56, 55],
    [56, 56],
    [56, 57],
    [56, 97],
    [56, 98],
    [56, 99],
    [56, 100],
    [56, 101],
    [56, 102],
    [57, 48],
    [57, 49],
    [57, 50],
    [57, 51],
    [57, 52],
    [57, 53],
    [57, 54],
    [57, 55],
    [57, 56],
    [57, 57],
    [57, 97],
    [57, 98],
    [57, 99],
    [57, 100],
    [57, 101],
    [57, 102],
    [97, 48],
    [97, 49],
    [97, 50],
    [97, 51],
    [97, 52],
    [97, 53],
    [97, 54],
    [97, 55],
    [97, 56],
    [97, 57],
    [97, 97],
    [97, 98],
    [97, 99],
    [97, 100],
    [97, 101],
    [97, 102],
    [98, 48],
    [98, 49],
    [98, 50],
    [98, 51],
    [98, 52],
    [98, 53],
    [98, 54],
    [98, 55],
    [98, 56],
    [98, 57],
    [98, 97],
    [98, 98],
    [98, 99],
    [98, 100],
    [98, 101],
    [98, 102],
    [99, 48],
    [99, 49],
    [99, 50],
    [99, 51],
    [99, 52],
    [99, 53],
    [99, 54],
    [99, 55],
    [99, 56],
    [99, 57],
    [99, 97],
    [99, 98],
    [99, 99],
    [99, 100],
    [99, 101],
    [99, 102],
    [100, 48],
    [100, 49],
    [100, 50],
    [100, 51],
    [100, 52],
    [100, 53],
    [100, 54],
    [100, 55],
    [100, 56],
    [100, 57],
    [100, 97],
    [100, 98],
    [100, 99],
    [100, 100],
    [100, 101],
    [100, 102],
    [101, 48],
    [101, 49],
    [101, 50],
    [101, 51],
    [101, 52],
    [101, 53],
    [101, 54],
    [101, 55],
    [101, 56],
    [101, 57],
    [101, 97],
    [101, 98],
    [101, 99],
    [101, 100],
    [101, 101],
    [101, 102],
    [102, 48],
    [102, 49],
    [102, 50],
    [102, 51],
    [102, 52],
    [102, 53],
    [102, 54],
    [102, 55],
    [102, 56],
    [102, 57],
    [102, 97],
    [102, 98],
    [102, 99],
    [102, 100],
    [102, 101],
    [102, 102],
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_parsing_works() {
        let folder = "00";
        // since we take a 40 hex char sha1 hash and turn it into
        // a 128bit hash integer, we discard everything after the
        // 30th hex character: ----------------------,
        //                                           v
        let file = "00000000000000000000000000000100000000";
        assert!(file.len() == 38);
        let hash = hash_object_file_and_folder(folder, file).unwrap();
        assert_eq!(hash, 1);

        // test big number too:
        let folder = "ff";
        let file = "ffffffffffffffffffffffffffffff00000000";
        assert!(file.len() == 38);
        let hash = hash_object_file_and_folder(folder, file).unwrap();
        assert_eq!(hash, u128::MAX);
        let expected_hex_str = "ffffffffffffffffffffffffffffffff";
        let hex_str = hex_u128_to_str(hash);
        assert_eq!(hex_str, expected_hex_str);
    }

    #[test]
    fn getting_first_byte_works() {
        let oid_str = "aaf00000000000000000000000000000";
        let oid = hash_str_to_oid(oid_str).unwrap();
        let first_byte = get_first_byte_of_oid(oid);
        // aa == 170
        assert_eq!(first_byte, 170);
    }
}
