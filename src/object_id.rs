

use std::io;
use crate::ioerr;

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
pub type OidStrTruncated = [u8; 32];

#[derive(Debug, Copy, Clone)]
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
            oid_str[0..hash_len].copy_from_slice(&hash[..].as_bytes());
            let zeros = vec![b'0'; 32 - hash_len];
            oid_str[hash_len..].copy_from_slice(&zeros[..]);
            (oid_str, hash_len * 4)
        } else {
            // we have enough bytes, copy the entire 32
            let mut oid_str = OidStrTruncated::default();
            oid_str[..].copy_from_slice(&hash[0..32].as_bytes());
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
    let oid_str = std::str::from_utf8(&oid_str).map_err(|e| ioerr!("{}", e))?;
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

pub fn get_partial_oid_from_hash(hash: &str) -> io::Result<Oid> {
    let hash_len = hash.len();
    let oid_str = if hash_len < 32 {
        // not enough bytes, so we need to add 0s to
        // the end:
        let mut oid_str = OidStrTruncated::default();
        oid_str[0..hash_len].copy_from_slice(&hash[..].as_bytes());
        let zeros = vec![b'0'; 32 - hash_len];
        oid_str[hash_len..].copy_from_slice(&zeros[..]);
        oid_str
    } else {
        // we have enough bytes, copy the entire 32
        let mut oid_str = OidStrTruncated::default();
        oid_str[..].copy_from_slice(&hash[0..32].as_bytes());
        oid_str
    };
    oid_str_truncated_to_oid(oid_str)
}

pub fn hash_str_to_oid(hash: &str) -> io::Result<Oid> {
    let trunc_str = hash.get(0..32)
        .ok_or_else(|| ioerr!("Your hash '{}' must be at least 32 hex chars long", hash))?;
    let mut oid_str_trunc = OidStrTruncated::default();
    oid_str_trunc[..].copy_from_slice(&trunc_str[..].as_bytes());
    let oid = oid_str_truncated_to_oid(oid_str_trunc)?;
    Ok(oid)
}

pub fn hash_object_file_and_folder(folder: &str, filename: &str) -> io::Result<Oid> {
    let mut oid_str = OidStrTruncated::default();
    oid_str[0..2].copy_from_slice(&folder[0..2].as_bytes());
    oid_str[2..32].copy_from_slice(&filename[0..30].as_bytes());
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
