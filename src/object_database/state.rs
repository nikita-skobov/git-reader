use super::{DoesMatch, MAX_PATH_TO_DB_LEN, main_sep_byte, packed::{open_idx_file_light, IDXFileLight}};
use std::{collections::{BTreeMap}, io};
use crate::{object_id::{Oid, get_first_byte_of_oid, HEX_BYTES, OidFull, hash_object_file_and_folder_full, full_oid_to_u128_oid, oid_full_to_string_no_alloc}, ioerr, fs_helpers, ioerre};
use tinyvec::{tiny_vec, TinyVec};


/// Lightest possible state. it saves:
/// - path to the objects directory.
/// - a reusable zlib decompressor
/// - if discovered, it saves all known loose object paths
/// - if discovered, it saves all known pack/idx paths
pub trait LightState {
    /// this is necessary in order to prevent re-allocating pathbufs each time we
    /// wish to read a file. Instead, we can create a stack allocated array
    /// of bytes that contains the path to the object DB, and then
    /// convert that as a string. that is returned along with the count
    /// of bytes that is currently in this array.
    /// a simple implementation would be:
    /// ```
    /// let my_db_str = "../.git/objects/";
    /// let mut my_arr = [0; MAX_PATH_TO_DB_LEN];
    /// my_arr[0..my_db_str.len()].copy_from_slice(my_db_str.as_bytes())
    /// (my_db_str.len(), my_arr)
    /// ```
    fn get_path_to_db_as_bytes(&self) -> (usize, [u8; MAX_PATH_TO_DB_LEN]);

    fn learn_loose_oid(&mut self, oid: Oid, oid_full: OidFull);

    /// returns false if the state does not know loose oids
    /// for this byte. otherwise, if the state does know, then
    /// it should iterate the known loose oids, and
    /// call the user's callback for every partial match
    fn knows_loose_oids_for_byte<F, P: DoesMatch>(&self, b: u8, partial: P, cb: &mut F)
        -> bool where F: FnMut(Oid);

    fn learn_pack_id(&mut self, pack_id: OidFull);

    /// returns false if the state does not know of any packs.
    /// if the state DOES know about the packs, then
    /// it should be able to iterate and find all matches for this
    /// partial oid.
    fn knows_all_packs<F, P: DoesMatch>(&self, partial: P, cb: &mut F)
        -> io::Result<bool> where F: FnMut(Oid);

    /// helper function to get a stack allocated array of bytes
    /// that can be converted to a string.
    /// extend_by should be valid utf-8 slice.
    /// we extend our self.path_to_db_bytes by the extend by slice
    /// and return an array that can be turned into a stack
    /// allocated string, as well as the index that you should
    /// take the slice up to.
    #[inline(always)]
    fn get_static_path_str(&self, extend_by: &[u8]) -> (usize, [u8; MAX_PATH_TO_DB_LEN]) {
        let (path_to_db_bytes_start, mut stack_arr) = self.get_path_to_db_as_bytes();
        let extend_num = extend_by.len();
        let take_slice_to = path_to_db_bytes_start + extend_num;
        stack_arr[path_to_db_bytes_start..take_slice_to].copy_from_slice(extend_by);
        (take_slice_to, stack_arr)
    }

    /// stack allocated strings cannot be returned. Our workaround
    /// is to let the user pass a callback. we create a string on
    /// the stack, and then pass it to the users callback.
    #[inline(always)]
    fn with_static_path_str<F, T>(&self, extend_by: &[u8], cb: F) -> io::Result<T>
        where F: FnMut(&str) -> T
    {
        let mut cb = cb;
        let (take_to, stack_arr) = self.get_static_path_str(extend_by);
        let stack_alloc_str = std::str::from_utf8(&stack_arr[0..take_to])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
        Ok(cb(stack_alloc_str))
    }

    #[inline(always)]
    fn get_idx_file_str_array_from_hash(&self, hex_str: &[u8]) -> (usize, [u8; MAX_PATH_TO_DB_LEN]) {
        let mut out: [u8; 54] = [
            b'p', b'a', b'c', b'k', main_sep_byte(),
            b'p', b'a', b'c', b'k', b'-',
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            b'.', b'i', b'd', b'x'
        ];
        // and we copy our hex str to replace the 40 0s:
        out[10..50].copy_from_slice(&hex_str[0..40]);
        // now we have our filename, and pack/ part, we want
        // to append it to our base object db path:
        self.get_static_path_str(&out)
    }

    fn read_idx_file(&self, filename: &str) -> io::Result<IDXFileLight> {
        // our file name should be at least 45 chars long:
        // pack-{40hexchars}.idx
        // we want just the 40 hex chars:
        let idx_hex_str = filename.get(5..45)
            .ok_or_else(|| ioerr!("Failed to extract hex chars from idx file name: {}", filename))?;
        self.read_idx_file_from_hex_slice(&idx_hex_str.as_bytes())
    }

    /// this is a convenience helper. id here should really be an OidFull,
    /// but it is more convenient to treat it as a slice here:
    fn read_idx_file_from_hex_slice(&self, id: &[u8]) -> io::Result<IDXFileLight> {
        let (take_to, idx_str_array) = self.get_idx_file_str_array_from_hash(&id);
        let search_path_str = std::str::from_utf8(&idx_str_array[0..take_to])
            .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
        // println!("reading idx file: {}", search_path_str);
        let idx_file = open_idx_file_light(search_path_str)?;
        Ok(idx_file)
    }

    fn read_idx_file_from_id(&self, id: OidFull) -> io::Result<IDXFileLight> {
        // our id is 20 bytes, we want to turn it into
        // a 40 byte hex array:
        let hex_arr = oid_full_to_string_no_alloc(id);
        self.read_idx_file_from_hex_slice(&hex_arr)
    }
}

/// given a loose folder to search (ie: .git/object/00)
// traverse that folder, and for every filename we find,
/// create an Oid from the hex_str/filename combo
/// (ie: hex_str = 00, filename = 38 hex chars after that, so
/// oid would be 00xxyyzz...)
/// for each oid we find, check if it partially matches
/// the user's requested partial oid, and if so
/// call the user's callback.
pub fn search_loose_folder_for_matches<F, S: LightState, P: DoesMatch>(
    loose_folder: &str,
    hex_str: &str,
    partial_oid: P,
    state: &mut S,
    cb: &mut F,
) -> io::Result<()>
    where F: FnMut(Oid)
{
    fs_helpers::search_folder_out(loose_folder, |entry| {
        let entryname = entry.file_name();
        let filename = entryname.to_str()
            .ok_or_else(|| ioerr!("Failed to convert {:?} to string", entryname))?;
        let oid_full = hash_object_file_and_folder_full(hex_str, &filename)?;
        let oid = full_oid_to_u128_oid(oid_full);
        state.learn_loose_oid(oid, oid_full);
        if partial_oid.matches(oid) {
            cb(oid);
        }
        Ok(())
    })
}

/// search through the loose objects of the folder
/// that corresponds to the user's first byte of their partial oid.
/// if possible, use state to search to not have to readdir, and
/// instead just ask state to iterate for us.
pub fn find_matching_oids_loose<F, P: DoesMatch, S: LightState>(
    partial_oid: P,
    state: &mut S,
    cb: &mut F,
) -> io::Result<()>
    where F: FnMut(Oid)
{
    let first_byte = partial_oid.get_first_byte();
    let state_knows = state.knows_loose_oids_for_byte(first_byte, partial_oid, cb);
    if state_knows { return Ok(()); }

    // otherwise, the state does not know that loose oid folder yet,
    // so lets read it, and teach it all oids inside it:
    let first_byte = first_byte as usize;
    let hex_first_byte: [u8; 2] = HEX_BYTES[first_byte];
    let (take_to, stack_arr) = state.get_static_path_str(&hex_first_byte);
    let stack_alloc_str = std::str::from_utf8(&stack_arr[0..take_to])
        .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
    // safe to do because our hex bytes are always valid utf8
    let hex_str = unsafe { std::str::from_utf8_unchecked(&hex_first_byte) };
    search_loose_folder_for_matches(stack_alloc_str, hex_str, partial_oid, state, cb)
}

pub fn search_idx_for_matches<F, P: DoesMatch>(
    idx_file: &IDXFileLight,
    partial_oid: P,
    partial_oid_first_byte: u8,
    cb: &mut F
) where F: FnMut(Oid) {
    idx_file.walk_all_oids_from(Some(partial_oid_first_byte), |oid| {
        let found_oid_first_byte = get_first_byte_of_oid(oid);
        if partial_oid.matches(oid) {
            cb(oid);
        }
        // if the oid first byte that we just found in the file
        // is greater than the first byte of our
        // partial oid, this means we can stop reading
        // because the .idx file is sorted by oid.
        found_oid_first_byte > partial_oid_first_byte
    });
}

/// search through the pack/ directory, and for each pack/index
/// file we find, teach the state its location, as well as
/// open the file and try to read it and see if the partial
/// oid matches anything in this file.
pub fn search_all_packs_for_matches<F, S: LightState, P: DoesMatch>(
    pack_folder: &str,
    partial_oid: P,
    state: &mut S,
    cb: &mut F
) -> io::Result<()>
    where F: FnMut(Oid)
{
    let partial_oid_first_byte = partial_oid.get_first_byte();
    fs_helpers::search_folder_out(pack_folder, |entry| {
        let filename = entry.file_name();
        let filename = filename.to_str()
            .ok_or_else(|| ioerr!("Failed to convert {:?} to string", filename))?;
        if ! filename.ends_with(".idx") {
            // skip non-index files
            return Ok(());
        }
        let idx_file = match state.read_idx_file(filename) {
            Ok(f) => f,
            // TODO: should we stop all iteration
            // if a single idx file failed to read?
            // I think not? so here I just return Ok
            // and continue the iteration at the next idx filename
            Err(_) => { return Ok(()) },
        };
        // teach the state about this idx id:
        state.learn_pack_id(idx_file.id);
        search_idx_for_matches(&idx_file, partial_oid, partial_oid_first_byte, cb);
        Ok(())
    })
}

pub fn find_matching_oids_packed<F, P: DoesMatch, S: LightState>(
    partial_oid: P,
    state: &mut S,
    cb: &mut F,
) -> io::Result<()>
    where F: FnMut(Oid)
{
    let state_knows = state.knows_all_packs(partial_oid, cb)?;
    if state_knows { return Ok(()); }
    // otherwise state doesn't know all of the packs yet, so
    // lets teach it:

    // first we load every .idx file we find in the database/packs
    // directory
    let packs_dir = b"pack";
    let (take_index, big_str_array) = state.get_static_path_str(packs_dir);
    let search_path_str = std::str::from_utf8(&big_str_array[0..take_index])
        .map_err(|e| ioerr!("Failed to convert path string to utf8...\n{}", e))?;
    search_all_packs_for_matches(search_path_str, partial_oid, state, cb)
}

pub fn find_matching_oids<F, P: DoesMatch, S: LightState>(
    partial_oid: P,
    state: &mut S,
    cb: F,
) -> io::Result<()>
    where F: FnMut(Oid)
{
    let mut cb = cb;
    find_matching_oids_loose(partial_oid, state, &mut cb)?;
    find_matching_oids_packed(partial_oid, state, &mut cb)?;
    Ok(())
}

pub struct LightStateDB {
    pub path_to_db_bytes: [u8; MAX_PATH_TO_DB_LEN],
    pub db_bytes_len: usize,

    pub loose_map: [BTreeMap<Oid, OidFull>; 256],

    pub known_packs: TinyVec<[OidFull; 64]>,
}

impl LightStateDB {
    pub fn new(p: &str) -> io::Result<LightStateDB> {
        // hard to imagine a path would be longer than this right?...
        let p_len = p.len();
        // we probably wont extend the path_to_db by more than 60 chars ever...
        let max_extend_by = 60;
        if p_len >= MAX_PATH_TO_DB_LEN - max_extend_by {
            return ioerre!("Path '{}' is too long for us to represent it without allocations", p);
        }
        // we create a static array that contains the utf8 bytes
        // of the path string. We do this so that
        // we can create path strings of other files in the object DB
        // without allocating, ie: we can use this stack allocated
        // array to create strings like {path_to_db}/pack-whatever...
        let mut path_to_db_bytes = [0; MAX_PATH_TO_DB_LEN];
        path_to_db_bytes[0..p_len].copy_from_slice(p.as_bytes());
        path_to_db_bytes[p_len] = main_sep_byte();

        let out = LightStateDB {
            path_to_db_bytes,
            db_bytes_len: p_len + 1,
            loose_map: default_loose_map(),
            known_packs: tiny_vec!(),
        };
        Ok(out)
    }
}

impl LightState for LightStateDB {
    fn get_path_to_db_as_bytes(&self) -> (usize, [u8; MAX_PATH_TO_DB_LEN]) {
        (self.db_bytes_len, self.path_to_db_bytes)
    }

    fn learn_loose_oid(&mut self, oid: Oid, oid_full: OidFull) {
        let first_byte = get_first_byte_of_oid(oid) as usize;
        self.loose_map[first_byte].insert(oid, oid_full);
    }

    fn knows_loose_oids_for_byte<F, P: DoesMatch>(&self, b: u8, partial: P, cb: &mut F)
        -> bool where F: FnMut(Oid)
    {
        let byte_usize = b as usize;
        // if we dont have any objects at that
        if self.loose_map[byte_usize].is_empty() { return false; }

        // otherwise we do, so lets iterate over it and return all partial
        // matches:
        for (oid, _oid_full) in self.loose_map[byte_usize].iter() {
            if partial.matches(*oid) {
                cb(*oid);
            }
        }
        true
    }

    fn knows_all_packs<F, P: DoesMatch>(&self, partial: P, cb: &mut F)
        -> io::Result<bool> where F: FnMut(Oid)
    {
        // if we havent seen any packs yet, we don't know any packs:
        if self.known_packs.is_empty() { return Ok(false); }

        let partial_first_byte = partial.get_first_byte();
        for pack_id in self.known_packs.iter() {
            let idx_file = self.read_idx_file_from_id(*pack_id)?;
            search_idx_for_matches(&idx_file, partial, partial_first_byte, cb);
        }
        Ok(true)
    }

    fn learn_pack_id(&mut self, pack_id: OidFull) {
        self.known_packs.push(pack_id);
    }
}

pub fn default_loose_map() -> [BTreeMap<Oid, OidFull>; 256] {
    [
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
        BTreeMap::new(), BTreeMap::new(), BTreeMap::new(), BTreeMap::new(),
    ]
}
