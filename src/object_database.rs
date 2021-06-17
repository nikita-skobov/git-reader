use std::{path::{Path, PathBuf}, collections::HashMap, fs::DirEntry, io};

use crate::{ioerr, fs_helpers};


/// TODO: use this to do multithreaded loose object searching.
/// if we are multithreaded, then we can just iterate all of these
/// instead of doing a reddir and then traversing the ones we find.
pub const HEX_256_TABLE: &[&str; 256] = &["00", "01", "02",
    "03", "04", "05", "06", "07", "08", "09", "0a", "0b", "0c",
    "0d", "0e", "0f", "10", "11", "12", "13", "14", "15", "16",
    "17", "18", "19", "1a", "1b", "1c", "1d", "1e", "1f", "20",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "2a",
    "2b", "2c", "2d", "2e", "2f", "30", "31", "32", "33", "34",
    "35", "36", "37", "38", "39", "3a", "3b", "3c", "3d", "3e",
    "3f", "40", "41", "42", "43", "44", "45", "46", "47", "48",
    "49", "4a", "4b", "4c", "4d", "4e", "4f", "50", "51", "52",
    "53", "54", "55", "56", "57", "58", "59", "5a", "5b", "5c",
    "5d", "5e", "5f", "60", "61", "62", "63", "64", "65", "66",
    "67", "68", "69", "6a", "6b", "6c", "6d", "6e", "6f", "70",
    "71", "72", "73", "74", "75", "76", "77", "78", "79", "7a",
    "7b", "7c", "7d", "7e", "7f", "80", "81", "82", "83", "84",
    "85", "86", "87", "88", "89", "8a", "8b", "8c", "8d", "8e",
    "8f", "90", "91", "92", "93", "94", "95", "96", "97", "98",
    "99", "9a", "9b", "9c", "9d", "9e", "9f", "a0", "a1", "a2",
    "a3", "a4", "a5", "a6", "a7", "a8", "a9", "aa", "ab", "ac",
    "ad", "ae", "af", "b0", "b1", "b2", "b3", "b4", "b5", "b6",
    "b7", "b8", "b9", "ba", "bb", "bc", "bd", "be", "bf", "c0",
    "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "ca",
    "cb", "cc", "cd", "ce", "cf", "d0", "d1", "d2", "d3", "d4",
    "d5", "d6", "d7", "d8", "d9", "da", "db", "dc", "dd", "de",
    "df", "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8",
    "e9", "ea", "eb", "ec", "ed", "ee", "ef", "f0", "f1", "f2",
    "f3", "f4", "f5", "f6", "f7", "f8", "f9", "fa", "fb", "fc",
    "fd", "fe", "ff"];

pub fn hex_u128_to_str(h: u128) -> String {
    let mut out_str = String::with_capacity(32);

    let mut mask: u128 = 0xff_00_00_00_00_00_00_00_00_00_00_00_00_00_00_00;
    let mut mask_shift = 120;
    for _ in 0..16 {
        let byte_at = h & mask;
        let byte_at = (byte_at >> mask_shift) as usize;
        out_str.push_str(HEX_256_TABLE[byte_at]);
        mask = mask >> 8;
        mask_shift -= 8;
    }

    out_str
}

pub fn hex_str_to_u8(h: &str) -> Option<u8> {
    let value = match &h[0..2] {
        "00" => 0x00,
        "01" => 0x01,
        "02" => 0x02,
        "03" => 0x03,
        "04" => 0x04,
        "05" => 0x05,
        "06" => 0x06,
        "07" => 0x07,
        "08" => 0x08,
        "09" => 0x09,
        "0a" => 0x0a,
        "0b" => 0x0b,
        "0c" => 0x0c,
        "0d" => 0x0d,
        "0e" => 0x0e,
        "0f" => 0x0f,
        "10" => 0x10,
        "11" => 0x11,
        "12" => 0x12,
        "13" => 0x13,
        "14" => 0x14,
        "15" => 0x15,
        "16" => 0x16,
        "17" => 0x17,
        "18" => 0x18,
        "19" => 0x19,
        "1a" => 0x1a,
        "1b" => 0x1b,
        "1c" => 0x1c,
        "1d" => 0x1d,
        "1e" => 0x1e,
        "1f" => 0x1f,
        "20" => 0x20,
        "21" => 0x21,
        "22" => 0x22,
        "23" => 0x23,
        "24" => 0x24,
        "25" => 0x25,
        "26" => 0x26,
        "27" => 0x27,
        "28" => 0x28,
        "29" => 0x29,
        "2a" => 0x2a,
        "2b" => 0x2b,
        "2c" => 0x2c,
        "2d" => 0x2d,
        "2e" => 0x2e,
        "2f" => 0x2f,
        "30" => 0x30,
        "31" => 0x31,
        "32" => 0x32,
        "33" => 0x33,
        "34" => 0x34,
        "35" => 0x35,
        "36" => 0x36,
        "37" => 0x37,
        "38" => 0x38,
        "39" => 0x39,
        "3a" => 0x3a,
        "3b" => 0x3b,
        "3c" => 0x3c,
        "3d" => 0x3d,
        "3e" => 0x3e,
        "3f" => 0x3f,
        "40" => 0x40,
        "41" => 0x41,
        "42" => 0x42,
        "43" => 0x43,
        "44" => 0x44,
        "45" => 0x45,
        "46" => 0x46,
        "47" => 0x47,
        "48" => 0x48,
        "49" => 0x49,
        "4a" => 0x4a,
        "4b" => 0x4b,
        "4c" => 0x4c,
        "4d" => 0x4d,
        "4e" => 0x4e,
        "4f" => 0x4f,
        "50" => 0x50,
        "51" => 0x51,
        "52" => 0x52,
        "53" => 0x53,
        "54" => 0x54,
        "55" => 0x55,
        "56" => 0x56,
        "57" => 0x57,
        "58" => 0x58,
        "59" => 0x59,
        "5a" => 0x5a,
        "5b" => 0x5b,
        "5c" => 0x5c,
        "5d" => 0x5d,
        "5e" => 0x5e,
        "5f" => 0x5f,
        "60" => 0x60,
        "61" => 0x61,
        "62" => 0x62,
        "63" => 0x63,
        "64" => 0x64,
        "65" => 0x65,
        "66" => 0x66,
        "67" => 0x67,
        "68" => 0x68,
        "69" => 0x69,
        "6a" => 0x6a,
        "6b" => 0x6b,
        "6c" => 0x6c,
        "6d" => 0x6d,
        "6e" => 0x6e,
        "6f" => 0x6f,
        "70" => 0x70,
        "71" => 0x71,
        "72" => 0x72,
        "73" => 0x73,
        "74" => 0x74,
        "75" => 0x75,
        "76" => 0x76,
        "77" => 0x77,
        "78" => 0x78,
        "79" => 0x79,
        "7a" => 0x7a,
        "7b" => 0x7b,
        "7c" => 0x7c,
        "7d" => 0x7d,
        "7e" => 0x7e,
        "7f" => 0x7f,
        "80" => 0x80,
        "81" => 0x81,
        "82" => 0x82,
        "83" => 0x83,
        "84" => 0x84,
        "85" => 0x85,
        "86" => 0x86,
        "87" => 0x87,
        "88" => 0x88,
        "89" => 0x89,
        "8a" => 0x8a,
        "8b" => 0x8b,
        "8c" => 0x8c,
        "8d" => 0x8d,
        "8e" => 0x8e,
        "8f" => 0x8f,
        "90" => 0x90,
        "91" => 0x91,
        "92" => 0x92,
        "93" => 0x93,
        "94" => 0x94,
        "95" => 0x95,
        "96" => 0x96,
        "97" => 0x97,
        "98" => 0x98,
        "99" => 0x99,
        "9a" => 0x9a,
        "9b" => 0x9b,
        "9c" => 0x9c,
        "9d" => 0x9d,
        "9e" => 0x9e,
        "9f" => 0x9f,
        "a0" => 0xa0,
        "a1" => 0xa1,
        "a2" => 0xa2,
        "a3" => 0xa3,
        "a4" => 0xa4,
        "a5" => 0xa5,
        "a6" => 0xa6,
        "a7" => 0xa7,
        "a8" => 0xa8,
        "a9" => 0xa9,
        "aa" => 0xaa,
        "ab" => 0xab,
        "ac" => 0xac,
        "ad" => 0xad,
        "ae" => 0xae,
        "af" => 0xaf,
        "b0" => 0xb0,
        "b1" => 0xb1,
        "b2" => 0xb2,
        "b3" => 0xb3,
        "b4" => 0xb4,
        "b5" => 0xb5,
        "b6" => 0xb6,
        "b7" => 0xb7,
        "b8" => 0xb8,
        "b9" => 0xb9,
        "ba" => 0xba,
        "bb" => 0xbb,
        "bc" => 0xbc,
        "bd" => 0xbd,
        "be" => 0xbe,
        "bf" => 0xbf,
        "c0" => 0xc0,
        "c1" => 0xc1,
        "c2" => 0xc2,
        "c3" => 0xc3,
        "c4" => 0xc4,
        "c5" => 0xc5,
        "c6" => 0xc6,
        "c7" => 0xc7,
        "c8" => 0xc8,
        "c9" => 0xc9,
        "ca" => 0xca,
        "cb" => 0xcb,
        "cc" => 0xcc,
        "cd" => 0xcd,
        "ce" => 0xce,
        "cf" => 0xcf,
        "d0" => 0xd0,
        "d1" => 0xd1,
        "d2" => 0xd2,
        "d3" => 0xd3,
        "d4" => 0xd4,
        "d5" => 0xd5,
        "d6" => 0xd6,
        "d7" => 0xd7,
        "d8" => 0xd8,
        "d9" => 0xd9,
        "da" => 0xda,
        "db" => 0xdb,
        "dc" => 0xdc,
        "dd" => 0xdd,
        "de" => 0xde,
        "df" => 0xdf,
        "e0" => 0xe0,
        "e1" => 0xe1,
        "e2" => 0xe2,
        "e3" => 0xe3,
        "e4" => 0xe4,
        "e5" => 0xe5,
        "e6" => 0xe6,
        "e7" => 0xe7,
        "e8" => 0xe8,
        "e9" => 0xe9,
        "ea" => 0xea,
        "eb" => 0xeb,
        "ec" => 0xec,
        "ed" => 0xed,
        "ee" => 0xee,
        "ef" => 0xef,
        "f0" => 0xf0,
        "f1" => 0xf1,
        "f2" => 0xf2,
        "f3" => 0xf3,
        "f4" => 0xf4,
        "f5" => 0xf5,
        "f6" => 0xf6,
        "f7" => 0xf7,
        "f8" => 0xf8,
        "f9" => 0xf9,
        "fa" => 0xfa,
        "fb" => 0xfb,
        "fc" => 0xfc,
        "fd" => 0xfd,
        "fe" => 0xfe,
        "ff" => 0xff,
        _ => return None,
    };
    Some(value)
}


/// A loose object is either unresolved, in which case
/// it points to a file: 00/xyzdadadebebe that contains
/// the actual object, and we can read that file, and then
/// turn this into a resolved loose object, which has
/// the data loaded into memory.
#[derive(Debug)]
pub enum PartiallyResolvedLooseObject {
    Unresolved(PathBuf),
    Resolved(Vec<u8>),
}

/// git objects directory can have many loose
/// objects, where the first 2 characters of the sha hash
/// are the name of the folder, and then within that folder
/// are files that are the remainder of that sha hash.
/// This partially resolved loose map contains
/// a hash map of each of those sha hashes (first 2 chars of
/// folder name combined with file names within that folder),
/// and the value is an enum that is either the full object file
/// read into memory, or the path of that file that is ready to be
/// read.
/// NOTE: we represent sha1 hash keys as u128, when they are really
/// 160 bits. We do this because even at 128 bits the chance of
/// a collision is miniscule.
/// (see: https://stackoverflow.com/questions/1867191/probability-of-sha1-collisions)
#[derive(Debug)]
pub struct PartiallyResolvedLooseMap {
    pub map: HashMap<u128, PartiallyResolvedLooseObject>,
}

pub fn hash_object_file_and_folder(folder: &str, filename: &str) -> Option<u128> {
    // the folder is the first 8 bits:
    let first_8_bits = hex_str_to_u8(folder)?;

    let mut shift = 120;
    let mut out_val: u128 = (first_8_bits as u128) << shift;
    // now we want the next 120 bits from the filename,
    // which means we only want the first 30 chars
    let filename = &filename[0..30];
    for i in 0..15 {
        shift -= 8;
        let hex_str = &filename[(i * 2)..];
        let value = hex_str_to_u8(hex_str)?;
        out_val = out_val + ((value as u128) << shift);
    }
    Some(out_val)
}

#[inline(always)]
pub fn filter_to_object_folder(
    direntry: &DirEntry
) -> Option<Vec<(u128, PathBuf)>> {
    let ftype = direntry.file_type().ok()?;
    if !ftype.is_dir() {
        return None;
    }
    let dname = direntry.file_name();
    let dname_str = dname.to_str()?;
    if dname_str.len() != 2 {
        return None;
    }
    // now we know this is an object folder,
    // so lets search through it, find all of the object files
    // and return a vec of map entries that should be
    // filled in later:
    let mut map_entries = vec![];
    let dirpath = direntry.path();
    let _ = fs_helpers::search_folder(dirpath, |objfile| -> Option<bool> {
        let objfiletype = if let Ok(t) = objfile.file_type() {
            t
        } else {
            return None;
        };

        if !objfiletype.is_file() {
            return None;
        }

        let objfilename = objfile.file_name();
        let objfilename = if let Some(s) = objfilename.to_str() {
            s
        } else {
            return None;
        };

        // if this is a valid obj file name, it should be 38 hex chars
        if objfilename.len() != 38 {
            return None;
        }

        // now, we know this is a file we want, so lets
        // parse its file name/folder name into its u128 hash,
        // and also enter it into our map entries
        let hash = if let Some(h) = hash_object_file_and_folder(dname_str, objfilename) {
            h
        } else {
            return None;
        };
        map_entries.push((hash, objfile.path()));

        // we dont need to return/collect anything here,
        // because we are appending our mutable map entries above
        None
    });
    return Some(map_entries);
}

impl PartiallyResolvedLooseMap {
    /// the given path should be the absolute path to the folder that contains
    /// all of the loose object folders, ie: /.../.git/objects/
    pub fn from_path<P: AsRef<Path>>(path: P) -> io::Result<PartiallyResolvedLooseMap> {
        let entries = fs_helpers::search_folder(path, filter_to_object_folder)?;
        let mut map = HashMap::new();
        for e in entries {
            for (hash, filepath) in e {
                map.insert(hash, PartiallyResolvedLooseObject::Unresolved(filepath));
            }
        }
        Ok(PartiallyResolvedLooseMap { map })
    }
}

#[derive(Debug)]
pub enum PartiallyResolvedPackAndIndex {
    /// pointer to index, and pack file respectively
    Unresolved(PackAndIndex),

    /// The index file is resolved, an in memory,
    /// but the pack file is still just the path to the file
    IndexResolved(Vec<u8>, PathBuf),

    /// both are resolved and in memory:
    BothResolved(Vec<u8>, Vec<u8>),
}

/// pointer to the index (*.idx) and pack (*.pack) files
#[derive(Debug)]
pub struct PackAndIndex {
    pub pack: PathBuf,
    pub index: PathBuf,
}

#[derive(Debug)]
pub struct ObjectDB {
    loose: PartiallyResolvedLooseMap,
    /// I am not sure if there is any significance to the sha hash
    /// of the *.pack files themselves, and as such I don't think
    /// we need to look them up? As such they will be put into a vec
    /// instead of a map.
    packs: Vec<PartiallyResolvedPackAndIndex>,
}

#[inline(always)]
pub fn get_pack_file_prefix_string(direntry: &DirEntry) -> Option<String> {
    let fileobj = direntry.path();
    if !fileobj.is_file() {
        return None;
    }

    let filename = fileobj.file_name()?.to_str()?;
    if !filename.starts_with("pack") || !filename.ends_with(".idx") {
        return None;
    }

    // TODO: is it safe to assume that
    // pack files will always be this length?
    // pack-{40 hex chars}.idx -> we want first 45 chars:
    match filename.get(0..45) {
        Some(s) => Some(s.to_string()),
        None => None
    }
}

/// path should be the absolute path to /.../.git/objects/
pub fn get_vec_of_unresolved_packs<P: AsRef<Path>>(
    path: P
) -> io::Result<Vec<PartiallyResolvedPackAndIndex>> {
    let mut out = vec![];
    let mut search_packs_path = path.as_ref().to_path_buf();
    search_packs_path.push("pack");

    let prefixes = fs_helpers::search_folder(
        &search_packs_path, get_pack_file_prefix_string)?;

    // now we have a vec of prefixes, where each prefix
    // is "pack-{40 hex chars}"
    // so we want to convert that to pathbufs and add the .idx, and .pack
    // extensions
    for prefix in prefixes {
        let mut pack = search_packs_path.clone();
        let mut index = search_packs_path.clone();
        let mut pack_file_name = prefix.clone();
        let mut idx_file_name = prefix;
        pack_file_name.push_str(".pack");
        idx_file_name.push_str(".idx");
        pack.push(pack_file_name);
        index.push(idx_file_name);
        let pack_and_index = PackAndIndex {
            pack,
            index,
        };
        out.push(PartiallyResolvedPackAndIndex::Unresolved(pack_and_index));
    }

    Ok(out)
}

impl ObjectDB {
    /// path should be the absolute path to the objects folder
    /// ie: /.../.git/objects/
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<ObjectDB> {
        let canon_path = path.as_ref().to_path_buf().canonicalize()?;
        let odb = ObjectDB {
            loose: PartiallyResolvedLooseMap::from_path(&canon_path)?,
            packs: get_vec_of_unresolved_packs(&canon_path)?,
        };
        Ok(odb)
    }
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
}
