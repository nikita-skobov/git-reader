use std::io;

use git_walker::{ioerr, object_id::{PartialOid, Oid, OidFull, oid_parts_to_full, get_first_byte_of_oid}};
use git_walker::{object_database::{LightObjectDB, loose::{commit_object_parsing, ParsedObject, ParseObject, blob_object_parsing, tree_object_parsing}, packed, state::{State, MinState}}, ioerre};
use git_walker::{object_database::{FoundPackedLocation, Location, oidmap_u128::{defaults::{B10, B14}, OidMap}}};
use packed::{PackFile, IDXFileLight, open_pack_file};
use io::{Write, stdout, StdoutLock};

/// Like git log, but defaults to --oneline and without pagination/coloring
/// eg:
/// git --no-pager log <HASH> --pretty=format:'%H %s' --no-color --first-parent

pub struct MyCustomParser {}
impl ParseObject for MyCustomParser {
    // here you can choose which parsing types your application needs:
    type Commit = commit_object_parsing::CommitOnlyParentsAndMessage;
    type Blob = blob_object_parsing::BlobObjectNone;
    type Tree = tree_object_parsing::TreeNone;
}


#[derive(Debug, Copy, Clone)]
pub enum OidLocationType {
    /// the 'rest' bits of this full oid.
    /// these last 32 bits combined with the 128 bits
    /// of the Oid key should be enough to find the full loose object
    Loose(u32),
    /// contains the index of where this .idx file information exists
    /// in our idx list, as well as the packfile offset for that .idx file
    Packed(usize, u64),
}

pub struct IdxSearched {
    pub continue_from: Option<u8>,
    pub file: IDXFileLight,
    pub fully_searched: bool,
    /// a map of the Oid as the key, and the value
    /// is the fanout index. you use this value to then
    /// find the packfile offset.
    pub known_oids: Option<OidMap<usize, B14>>,
}

pub struct SearchedPack {
    pub id: OidFull,
    pub idx_file: IdxSearched,
    pub pack_file: PackFile,
}

impl SearchedPack {
    /// returns the u64 packfile offset of where this object should
    /// exist in the packfile. returns None if not found.
    /// returns an error only if there was some read error, or
    /// if we expected to find a packfile offset but failed.
    pub fn find_oid_location(&mut self, oid: Oid) -> io::Result<Option<u64>> {
        // first check if its in the known oids, and if so, then we can
        // guarantee its in this file, so we can retrieve it fairly quicky
        let searched = &mut self.idx_file;
        // unnecessary: its always Some(...)
        if let Some(map) = &searched.known_oids {
            match map.get(&oid) {
                Some(fanout_index) => {
                    let packfile_offset = searched.file.find_packfile_index_from_fanout_index(*fanout_index)
                        .ok_or_else(|| ioerr!("Expected to find packfile index for {:032x} but failed", oid))?;
                    return Ok(Some(packfile_offset));
                }
                None => {}
            }
        }
        // at this point, we know this oid is not in the map of what we have
        // already searched. so lets try continue searching.
        // if we know we've already searched the entirety of this file,
        // we just exit now because no point in searching what
        // we already know:
        if searched.fully_searched {
            return Ok(None);
        }

        // safe because its always some
        let mut tmp_map = searched.known_oids.take().unwrap();
        let tmp_map_ref = &mut tmp_map;
        // searched.known_oids = Sparse128Map::<usize, B14>::default();
        // we have not fully searched yet, so lets finish searching:
        let idx_file = &searched.file;
        let mut was_fully_searched = true;
        let mut found = None;
        let mut had_error = Ok(());
        idx_file.walk_all_oids_with_index_and_from(searched.continue_from, |found_oid, fanout_index| {
            tmp_map_ref.insert(found_oid, fanout_index);
            if found_oid == oid {
                // we found what we are looking for, we can exit now.
                // note that we have not fully searched this idx file
                was_fully_searched = false;
                // if we found it, lets get its packfile offset:
                match idx_file.find_packfile_index_from_fanout_index(fanout_index) {
                    Some(packfile_offset) => {
                        found = Some(packfile_offset);
                    }
                    // very unlikely that we found an oid
                    // in an index file, but failed to find its packfile
                    // offset. if this happens though, its an error:
                    None => {
                        had_error = ioerre!("Expeected to find packfile index for {:032x} but failed", oid);
                    }
                }
                return true;
            }
            false
        });
        searched.known_oids = Some(tmp_map);
        // return the error if there was one:
        if let Err(e) = had_error {
            return Err(e);
        }

        // if we were fully searched, mark that so we avoid
        // reading this in the future:
        if was_fully_searched {
            searched.fully_searched = true;
        } else {
            // advance our continue from so that the next
            // time we read, we dont read things we've already read:
            let oid_first_byte = get_first_byte_of_oid(oid);
            searched.continue_from = Some(oid_first_byte);
        }

        if let Some(packfile_offset) = found {
            return Ok(Some(packfile_offset));
        }

        // otherwise we failed to find it, but its ok
        // because we still updated our information,
        // and this will make future searches faster:
        Ok(None)
    }
}

#[derive(Default)]
pub struct OidMap2 {
    /// keys are the Oid, the value is the last 32
    /// bits needed to make an OidFull
    pub loose_map: OidMap<u32, B10>,
    pub searched_packs: Vec<SearchedPack>,
    pub unsearched_packs: Vec<OidFull>,
    pub last_object_was_in_pack: bool,
}

impl OidMap2 {
    pub fn initialize(odb: &LightObjectDB) -> io::Result<OidMap2> {
        let mut out = OidMap2::default();
        odb.iter_all_known_objects(&mut |location| {
            match location {
                Location::Loose(oid, rest) => {
                    out.loose_map.insert(oid, rest);
                }
                Location::Packed(idx_id) => {
                    out.unsearched_packs.push(idx_id);
                }
            }
        })?;
        Ok(out)
    }

    pub fn disambiguate(&self, ambiguous_oid: &str) -> io::Result<(Oid, OidLocationType)> {
        let partial_oid =  PartialOid::from_hash(ambiguous_oid)?;
        let mut found: Option<(Oid, OidLocationType)> = None;
        let mut err_str = String::with_capacity(0);
        for (oid, oid_rest) in self.loose_map.range(partial_oid.oid..) {
            if !partial_oid.matches(*oid) {
                break;
            }
            if found.is_none() {
                let location = OidLocationType::Loose(*oid_rest);
                found = Some((*oid, location));
            } else {
                if err_str.is_empty() {
                    err_str = format!("{:032x}", found.unwrap().0);
                }
                err_str = format!("{}\n{:032x}", err_str, oid);
            }
        }
        // TODO: implement searching through the searched and unsearched packs...
        // right now this only tries to disambiguate loose objects, but doesnt
        // consider that there can be packed objects that are also partial matches...
        // TODO: if using packed objects, change signature to be mutable
        // because we probably want to save that state
        if err_str.is_empty() {
            match found {
                Some(i) => Ok(i),
                None => {
                    return ioerre!("Failed to find anything matching {:032x}", partial_oid.oid);
                }
            }
        } else {
            ioerre!("{}", err_str)
        }
    }

    pub fn get_object_from_location<S: State>(
        &self,
        oid: Oid,
        location: OidLocationType,
        odb: &LightObjectDB,
        state: &mut S,
    ) -> io::Result<ParsedObject<MyCustomParser>> {
        let obj = match location {
            OidLocationType::Loose(restbits) => {
                // for loose objects, we need to reconstruct
                // the full oid:
                let oid_full = oid_parts_to_full(oid, restbits);
                let object: ParsedObject<MyCustomParser> = odb.get_loose_object_from_oid_full(oid_full, state)?;
                object
            }
            OidLocationType::Packed(idx_file_index, packfile_offset) => {
                let idx_file = &self.searched_packs[idx_file_index];
                let idx_file_id = idx_file.id;
                let location = FoundPackedLocation {
                    id: idx_file_id,
                    object_starts_at: packfile_offset,
                    oid_index: 0, // we dont need this
                };
                // TODO: this state should have access to our oid map, otherwise
                // it will unnecessarily read another index file...
                let pack = &idx_file.pack_file;
                let object: ParsedObject<MyCustomParser> = odb.get_packed_object_packfile_loaded(&location, &pack, state)?;
                object
            }
        };
        Ok(obj)
    }

    pub fn try_resolve_unchecked_packs(
        &mut self,
        oid: Oid,
        odb: &LightObjectDB,
    ) -> io::Result<OidLocationType> {
        // first, check through the packs we've already read/opened. if we
        // dont find it there, then move on to the next pack and keep trying
        // until we've read all packs. if still not found then error.

        // TODO: make this go in reverse, because then
        // were checking the most recent useful packs
        let mut index = 0;
        for searched_pack in self.searched_packs.iter_mut() {
            // look through this partially searched pack, and return the position
            // of this oid within its respective packfile. if not found, then try
            // the next searched pack.
            if let Some(packfile_offset) = searched_pack.find_oid_location(oid)? {
                // the actual index is the inverse of index because we iterate in reverse:
                // let len = self.searched_packs.len();
                // let index = len - index - 1;
                let location = OidLocationType::Packed(index, packfile_offset);
                return Ok(location);
            }
            index += 1;
        }

        // otherwise, we failed to find it in one of our searched packs,
        // so lets now try reading a pack that we have not read yet:
        let mut found_in_file = None;
        let oid_first_byte = get_first_byte_of_oid(oid);
        let unsearched_packs_len = self.unsearched_packs.len();
        for (unsearched_index, idx_id) in self.unsearched_packs.iter().rev().enumerate() {
            let idx_file = odb.read_idx_file_from_id(*idx_id)?;
            // we read through this file, and stop if we find the oid we are
            // looking for. if we find the oid in this file, then
            // we move it into the searched_packs list.
            // otherwise, we keep it in the unsearched_packs, because
            // we only want to load useful idx files into memory.

            let mut found = None;
            let mut had_error = Ok(());
            idx_file.walk_all_oids_with_index_and_from(Some(oid_first_byte), |found_oid, fanout_index| {
                if found_oid == oid {
                    match idx_file.find_packfile_index_from_fanout_index(fanout_index) {
                        Some(packfile_offset) => {
                            found = Some(packfile_offset);
                        }
                        // very unlikely that we found an oid
                        // in an index file, but failed to find its packfile
                        // offset. if this happens though, its an error:
                        None => {
                            had_error = ioerre!("Expected to find packfile index for {:032x} but failed", oid);
                        }
                    }
                    return true;
                }
                false
            });
            if let Err(e) = had_error {
                return Err(e);
            }

            // if we found the oid in this idx file, then we
            // can stop iterating, and just use this idx file:
            if let Some(packfile_offset) = found {
                // println!("Found {:032x} in idx id: {:?}. this was unsearchindex: {}", oid, idx_id, unsearched_index);
                let remove_index = unsearched_packs_len - unsearched_index - 1;
                // println!("But actually should remove index: {}", remove_index);
                found_in_file = Some((remove_index, idx_file, packfile_offset));
                break;
            }
        }

        // if we found it in one of the above files,
        // we want to:
        // - remove that entry from the unsearched packs list,
        // and make it into a searched pack,
        // - return the location info to the user:
        if let Some((unsearched_index, idx_file, packfile_offset)) = found_in_file {
            // println!("removing unsearched item at index {}", unsearched_index);
            // println!("out idx_file id: {:?}", idx_file.id);
            let idx_id = self.unsearched_packs.remove(unsearched_index);
            // println!("Id of the idx file we removed from unsearched: {:?}", idx_id);
            let idx_searched = IdxSearched {
                continue_from: None,
                // we know how many objects will be in this
                // map, so we can preallocate!
                known_oids: Some(OidMap::new_with_prealloc_m_objects_and_percent(idx_file.num_objects, 1.3)),
                file: idx_file,
                fully_searched: false,
            };
            let (pack_str_arr, take_to) = odb.get_pack_file_str_array(idx_id);
            let pack_str_path = std::str::from_utf8(&pack_str_arr[0..take_to])
                .map_err(|_| ioerr!("Failed to ecreate pack file string path"))?;
            let pack = open_pack_file(pack_str_path, idx_id)?;
            let searched_pack = SearchedPack {
                id: idx_id,
                idx_file: idx_searched,
                pack_file: pack,
            };
            self.searched_packs.push(searched_pack);
            let location = OidLocationType::Packed(index, packfile_offset);
            return Ok(location);
        }
        
        // otherwise we failed to find it in our searched packs,
        // and we failed to find it in any of our unsearched packs.
        // this oid probably doesnt exist:
        ioerre!("Failed to find {:032x} in any searched or unsearched packs", oid)
    }

    pub fn get_object_from_oid<S: State>(
        &mut self,
        oid: Oid,
        odb: &LightObjectDB,
        state: &mut S
    ) -> io::Result<ParsedObject<MyCustomParser>> {
        // get the location. either its in the loose map,
        // or we have to look through the seached/unsearched packs

        // we try to optimize here by checking if the last object
        // was in the packfile. if so, we skip checking the loose map,
        // and instead try to get it from the packfile, otherwise,
        // if we fail to find it there, we check in the loose map
        let location = if self.last_object_was_in_pack {
            // TODO: change this to return Ok(Some)... because otheriwse
            // we cant differentiate between 'not found' vs 'error'...
            if let Ok(location) = self.try_resolve_unchecked_packs(oid, odb) {
                location
            } else {
                match self.loose_map.get(&oid) {
                    Some(rest_bits) => {
                        self.last_object_was_in_pack = false;
                        OidLocationType::Loose(*rest_bits)
                    }
                    None => {
                        return ioerre!("Failed to find obj in pack or in loose");
                    }
                }
            }
        } else {
            // otherwise, the last object was a loose object,
            // so lets try checking the loose first:
            match self.loose_map.get(&oid) {
                Some(rest_bits) => {
                    OidLocationType::Loose(*rest_bits)
                }
                None => {
                    self.last_object_was_in_pack = true;
                    // try to resolve one of the unchecked packs:
                    self.try_resolve_unchecked_packs(oid, odb)?
                }
            }
        };
        self.get_object_from_location(oid, location, odb, state)
    }
}

pub fn lo7_lookup<S: State>(
    all_oids: &mut OidMap2,
    next_parent_id: Oid,
    odb: &LightObjectDB,
    state: &mut S,
) -> io::Result<ParsedObject<MyCustomParser>> {
    all_oids.get_object_from_oid(next_parent_id, odb, state)
}

/// I dont want to inline this for debugging/profiling purposes.
#[inline(never)]
pub fn lo7_log(
    obj: &ParsedObject<MyCustomParser>,
    handle: &mut StdoutLock,
    this_oid: &mut Oid,
) -> bool {
    if let ParsedObject::Commit(ref c) = obj {
        let _ = writeln!(handle, "{:032x} {}", this_oid, c.message);
        // TODO: using handle.write_all is a bit better i think
        if c.parent_one == 0 {
            // initial commit, we are done
            return true;
        }
        let next_parent_id = c.parent_one;
        *this_oid = next_parent_id;
        return false;
    }
    true
}

pub fn realmain() -> io::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let path = args.get(1)
        .ok_or_else(|| ioerr!("Must provide a path to git objects db"))?;
    let ambiguous_oid = args.get(2)
        .ok_or_else(|| ioerr!("Must provide an OID to search"))?;
    let counter_stop = match args.get(3) {
        Some(s) => s.parse::<usize>().unwrap_or(usize::MAX),
        None => usize::MAX,
    };

    let mut state = MinState::new(path)?;
    let odb = LightObjectDB::new(&path)?;
    let mut all_oids = OidMap2::initialize(&odb)?;
    let (found_oid, found_location) = all_oids.disambiguate(ambiguous_oid)?;
    let mut obj = all_oids.get_object_from_location(
        found_oid, found_location, &odb, &mut state)?;

    let stdo = stdout();
    let mut handle = stdo.lock();
    let mut this_oid = found_oid;
    let mut counter = 0;
    loop {
        let should_break = lo7_log(&obj, &mut handle, &mut this_oid);
        if should_break { break; }
        // the lo7_log call sets the next parent id to this_oid
        obj = lo7_lookup(&mut all_oids, this_oid, &odb, &mut state)?;
        counter += 1;
        if counter >= counter_stop {
            break;
        }
    }
    std::process::exit(0);
    Ok(())
}

pub fn main() {
    if let Err(e) = realmain() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}
