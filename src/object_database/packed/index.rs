use std::{path::{Path, PathBuf}, io, fmt::Debug};
use byteorder::{BigEndian, ByteOrder};
use crate::{ioerre, fs_helpers};
use memmap2::Mmap;
use super::PartiallyResolvedPackFile;

/// see: https://git-scm.com/docs/pack-format#_version_2_pack_idx_files_support_packs_larger_than_4_gib_and
pub const V2_IDX_SIGNATURE: [u8; 4] = [255, b't', b'O', b'c'];
pub const V2_SKIP_VERSION_NUMBER_SIZE: usize = 4;
pub const V2_IDX_VERSION_NUMBER_BYTES: [u8; 4] = [0, 0, 0, 2];
pub const V2_IDX_VERSION_NUMBER: u32 = 2;
pub const FANOUT_LENGTH: usize = 256;
pub const FANOUT_ENTRY_SIZE: usize = 4;
pub const SHA1_SIZE: usize = 20;
/// according to docs, it looks like trailer is just 2 checksums?
pub const IDX_TRAILER_SIZE: usize = SHA1_SIZE * 2;
pub const MINIMAL_IDX_FILE_SIZE: usize = IDX_TRAILER_SIZE + FANOUT_LENGTH * FANOUT_ENTRY_SIZE;


#[derive(Debug)]
pub enum PartiallyResolvedPackAndIndex {
    /// pointer to index file
    Unresolved(PathBuf),

    /// The index file is resolved, an in memory,
    /// but the pack file may or may not be resolved yet.
    /// The IDXFile struct decides if/when to resolve
    /// the pack file it 'owns'
    IndexResolved(IDXFile),
}

pub struct IDXFile {
    pub fanout_table: [u32; 256],
    pub version: IDXVersion,
    pub num_objects: u32,
    pub mmapped_file: Mmap,
    pub pack: PartiallyResolvedPackFile,
}

/// We implement debug manually because we
/// dont want the mmapped file to be debugged...
impl Debug for IDXFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IDXFile")
            .field("fanout_table", &self.fanout_table)
            .field("num_objects", &self.num_objects)
            .field("version", &self.version)
            .finish()
    }
}

#[derive(Debug)]
pub enum IDXVersion {
    V1,
    V2,
}

/// inspired by both:
/// https://github.com/speedata/gogit/blob/c5cbd8f9b7205cd5390219b532ca35d0f76b9eab/repository.go#L87
/// https://github.com/Byron/gitoxide/blob/157b6ff7b55ba2b7f8f90f66864212906426f8d7/git-pack/src/index/init.rs#L36
pub fn open_idx_file<P: AsRef<Path>>(
    path: P
) -> io::Result<IDXFile> {
    let idx_file_path = path.as_ref().to_path_buf();
    let pack_file_path = idx_file_path.with_extension("pack");
    if !pack_file_path.is_file() {
        return ioerre!("Failed to find corresponding pack file: {:?}", pack_file_path);
    }
    let mmapped = fs_helpers::get_mmapped_file(path)?;
    let idx_size = mmapped.len();
    if idx_size < MINIMAL_IDX_FILE_SIZE {
        return ioerre!("IDX file is too small to be a valid idx file");
    }
    let v2_idx_sig_len = V2_IDX_SIGNATURE.len();
    let version = if &mmapped[0..v2_idx_sig_len] == V2_IDX_SIGNATURE {
        // 4 byte version number... docs say it has to be == 2,
        // if we detected a V2 idx signature:
        let version_range = v2_idx_sig_len..(v2_idx_sig_len + V2_SKIP_VERSION_NUMBER_SIZE);
        let version_bytes = &mmapped[version_range];
        let version_number = BigEndian::read_u32(version_bytes);
        if version_number != V2_IDX_VERSION_NUMBER {
            return ioerre!("Invalid .idx version number. Expected version number of {}, found {}", V2_IDX_VERSION_NUMBER, version_number);
        }
        IDXVersion::V2
    } else {
        IDXVersion::V1
    };

    // now get the data, for v1 its the entirety
    // of the file, but for v2 its everything after the
    // first 8 bytes
    let data = match version {
        IDXVersion::V1 => &mmapped[..],
        IDXVersion::V2 => {
            let skip_index = v2_idx_sig_len + V2_SKIP_VERSION_NUMBER_SIZE;
            &mmapped[skip_index..]
        }
    };

    let mut fanout_table = [0; FANOUT_LENGTH];
    fill_fan(&mut fanout_table, data);
    let num_objects = fanout_table[FANOUT_LENGTH - 1];

    let idxfile = IDXFile {
        fanout_table,
        version,
        num_objects,
        mmapped_file: mmapped,
        pack: PartiallyResolvedPackFile::Unresolved(pack_file_path),
    };
    Ok(idxfile)
}


/// taken from:
/// https://github.com/Byron/gitoxide/blob/157b6ff7b55ba2b7f8f90f66864212906426f8d7/git-pack/src/index/init.rs#L84
/// and slightly modified: we take a pre-instantiated fan, and fill it in.
/// Also, I cant figure out why we care about returning bytes read?
fn fill_fan(fan: &mut [u32; FANOUT_LENGTH], d: &[u8]) -> usize {
    for (c, f) in d.chunks(FANOUT_ENTRY_SIZE).zip(fan.iter_mut()) {
        *f = BigEndian::read_u32(c);
    }
    FANOUT_LENGTH * FANOUT_ENTRY_SIZE
}
