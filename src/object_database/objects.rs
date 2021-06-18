
use crate::{fs_helpers, object_id::Oid, ioerr, ioerre};
use std::{io, path::Path, fs::File, fmt::Debug, str::FromStr};
use flate2::{Decompress, Status, FlushDecompress};
use io::{BufRead, Read};

/// TODO: finish parsing
#[derive(Debug)]
pub struct BlobObject {

}

/// TODO: finish parsing
#[derive(Debug)]
pub struct TagObject {

}

/// TODO: finish parsing
#[derive(Debug)]
pub struct TreeObject {

}

#[derive(Debug, Default)]
pub struct CommitObject {
    pub parents: Vec<Oid>,
    // TODO: need to parse commit message,
    // commit tree/blob structure...
}

/// Each object type variant contains
/// the size of that object, and
/// then the actual struct of that object
#[derive(Debug)]
pub enum ObjectType {
    Tree(TreeObject),
    Blob(BlobObject),
    Commit(CommitObject),
    Tag(TagObject),
}

#[derive(Debug, PartialOrd, PartialEq)]
pub enum UnparsedObjectType {
    Tree,
    Blob,
    Commit,
    Tag,
}

impl FromStr for UnparsedObjectType {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let objtype = match s {
            "tree" => UnparsedObjectType::Tree,
            "tag" => UnparsedObjectType::Tag,
            "commit" => UnparsedObjectType::Commit,
            "blob" => UnparsedObjectType::Blob,
            _ => { return ioerre!("Failed to parse object type of '{}'", s); },
        };
        Ok(objtype)
    }
}

#[derive(Debug)]
pub struct UnparsedObject {
    pub object_type: UnparsedObjectType,
    pub payload: Vec<u8>,
}

/// returns the type of object, the size of the actual decompressed object
/// (the value the object header), and the index of where the
/// rest of the payload starts from the decompressed data.
/// Returns null if failed to decode header, ie: its an invalid header
pub fn decode_object_header(input: &[u8]) -> Option<(&str, usize, usize)> {
    let null_byte_index = input.iter().position(|&i| i == 0)?;
    let header = &input[0..null_byte_index];
    // the header should just be an ascii string:
    let header_str = std::str::from_utf8(&header).ok()?;
    let mut split = header_str.split(' ');
    let object_type_str = split.next()?;
    let object_size_str = split.next()?;
    let object_size = object_size_str.parse::<usize>().ok()?;
    Some((object_type_str, object_size, null_byte_index + 1))
}

pub fn decode_object_header_res<D: Debug>(
    input: &[u8],
    filename: D,
) -> io::Result<(&str, usize, usize)> {
    decode_object_header(input)
        .ok_or(ioerr!("Failed to decode header of file {:?}", filename))
}

/// returns information about what the first read returned.
/// contains necessary offsets in case a second read is required
pub struct FirstReadInfo {
    /// number of bytes you can read from the file if necessary
    pub remaining_file_bytes_to_read: usize,
    /// index of where the payload starts at in the decompressed buffer.
    pub payload_starts_at: usize,
    /// size of the payload of the object file.
    /// this is the value of `<size>` from the header of:
    /// `<type><space><size><nullbyte>`
    pub payload_size: usize,
    /// this is the buffer of what was read from the file.
    /// it is potentially already full. if `remaining_file_bytes_to_read == 0`
    /// then this compressed buf is already the entire file.
    pub compressed_buf: Vec<u8>,
    /// this is the buffer of what has been decompressed so far
    pub decompressed_buf: [u8; 128],
    pub object_type: UnparsedObjectType,
    /// a zlib decompressor. should contain the state necessary
    /// to continue decompressing if necessary
    pub decompressor: Decompress,
    /// every time you call decompressor.decompress(...)
    /// you get returned a state of if its done, has more to read/output, or
    /// had an error. It is HIGHLY unlikely that this decompressed state
    /// is done after the first read, but we should check anyway
    pub decompressed_state: Status,
}

pub fn read_and_extract_header<D: Debug>(
    file: &mut File,
    filename: D,
) -> io::Result<FirstReadInfo> {
    // we expect a git object to contain a zlib header
    let will_contain_zlib_header = true;
    let mut decompressor = Decompress::new(will_contain_zlib_header);
    
    // only read 2kb at first.
    // this should be guaranteed to contain the header,
    // and for commits/tree objects it should also be enough
    // to read the entire file. After parsing the header,
    // if we find that this is a blob object, we don't want
    // to load the rest of it. But if its a commit/tree then
    // we will load the rest of it if the 2kb wasn't enough
    let read_max = 2048;
    let file_size = file.metadata()?.len() as usize;
    let mut buf = if file_size >= read_max {
        vec![0; read_max]
    } else {
        vec![0; file_size]
    };
    file.read_exact(&mut buf)
        .map_err(|e| ioerr!("Failed to read file {:?}\n{}", filename, e))?;
    // the header should only need 128 bytes (even less..)
    // to be properly parsed. Once we parse the header, we decide what to do next
    let mut header_buf = [0; 128];
    let decompressed_state = decompressor.decompress(
        &buf, &mut header_buf, FlushDecompress::None)?;
    let (
        object_type,
        payload_size,
        payload_starts_at
    ) = decode_object_header_res(&header_buf, filename)?;

    let read_info = FirstReadInfo {
        remaining_file_bytes_to_read: file_size - buf.len(),
        payload_starts_at,
        payload_size,
        compressed_buf: buf,
        object_type: UnparsedObjectType::from_str(object_type)?,
        decompressor,
        decompressed_state,
        decompressed_buf: header_buf,
    };

    Ok(read_info)
}


/// I couldnt figure out the decompress logic, so I stole this from:
/// https://github.com/Byron/gitoxide/blob/057016e2df3138992c4857f9b65bf19dc2c9a097/git-features/src/zlib/stream/inflate.rs#L22
/// Read bytes from `rd` and decompress them using
/// `state` into a pre-allocated fitting buffer `dst`,
/// returning the amount of bytes written.
pub fn decompress_remaining(
    rd: &mut impl BufRead,
    state: &mut Decompress,
    mut dst: &mut [u8]
) -> io::Result<usize> {
    let mut total_written = 0;
    loop {
        let (written, consumed, ret, eof);
        {
            let input = rd.fill_buf()?;
            eof = input.is_empty();
            let before_out = state.total_out();
            let before_in = state.total_in();
            let flush = if eof {
                FlushDecompress::Finish
            } else {
                FlushDecompress::None
            };
            ret = state.decompress(input, dst, flush);
            written = (state.total_out() - before_out) as usize;
            total_written += written;
            dst = &mut dst[written..];
            consumed = (state.total_in() - before_in) as usize;
        }
        rd.consume(consumed);

        match ret {
            // The stream has officially ended, nothing more to do here.
            Ok(Status::StreamEnd) => return Ok(total_written),
            // Either input our output are depleted even though the stream is not depleted yet.
            Ok(Status::Ok) | Ok(Status::BufError) if eof || dst.is_empty() => return Ok(total_written),
            // Some progress was made in both the input and the output, it must continue to reach the end.
            Ok(Status::Ok) | Ok(Status::BufError) if consumed != 0 || written != 0 => continue,
            // A strange state, where zlib makes no progress but isn't done either. Call it out.
            Ok(Status::Ok) | Ok(Status::BufError) => return ioerre!("Unexpected defalte status. Definitely a bug somewhere"),
            Err(..) => return ioerre!("corrupt deflate stream"),
        }
    }
}


pub fn read_raw_object<P: AsRef<Path>>(
    path: P,
    should_read_blobs: bool,
) -> io::Result<UnparsedObject> {
    let mut file = fs_helpers::get_readonly_handle(&path)?;

    let first_read_info = read_and_extract_header(&mut file, path.as_ref())?;
    if !should_read_blobs && first_read_info.object_type == UnparsedObjectType::Blob {
        // this is a blob, and the user did not want to
        // read it, so we just return with an empty vec:
        return Ok(UnparsedObject {
            object_type: first_read_info.object_type,
            payload: vec![],
        })
    }

    // otherwise, we want to read the entirety of the file:
    let entire_file_buf = if first_read_info.remaining_file_bytes_to_read == 0 {
        // we already read the file:
        first_read_info.compressed_buf
    } else {
        // read the rest of the file:
        let mut buf = first_read_info.compressed_buf;
        let bytes_read_so_far = buf.len();
        let desired_buf_len = bytes_read_so_far + first_read_info.remaining_file_bytes_to_read;
        buf.resize(desired_buf_len, 0);
        file.read_exact(&mut buf[bytes_read_so_far..])
            .map_err(|e| ioerr!("Failed to perform second read on file {:?}\n{}", path.as_ref(), e))?;
        buf
    };

    // now we have the entire file in memory, so we can continue
    // decompressing it from where we left off:
    // to do so, we need to first resize our output
    // buffer to be the exact size that we expect to put into it.
    // it should be the size of the payload that we decoded from the header
    let output_buffer = first_read_info.decompressed_buf;
    let desired_output_buffer_len = first_read_info.payload_size;
    let mut desired_out = vec![0; desired_output_buffer_len];
    // because the original output buffer might have some data in it other
    // than the header, we want to copy that to the beginning of this new output
    // buffer.
    let desired_data_starts_at = first_read_info.payload_starts_at;
    let desired_bytes = output_buffer.len() - desired_data_starts_at;
    let desired_slice_to_copy = &first_read_info.decompressed_buf[desired_data_starts_at..];
    desired_out[0..desired_bytes].copy_from_slice(desired_slice_to_copy);
    let mut output_buffer = desired_out;

    let mut decompressor = first_read_info.decompressor;
    let bytes_input = decompressor.total_in() as usize;
    let bytes_out = decompressor.total_out() as usize - desired_data_starts_at;
    
    // I think you're supposed to check if the state of the first
    // decompression is StreamEnd, but I think that is impossible if we pass in
    // an output buffer of 128 bytes?
    decompress_remaining(
        &mut &entire_file_buf[bytes_input..],
        &mut decompressor,
        &mut output_buffer[bytes_out..],
    ).map_err(|e| ioerr!("Failed to decompress remaining bytes of {:?}\n{}", path.as_ref(), e))?;

    Ok(UnparsedObject {
        object_type: first_read_info.object_type,
        // TODO: this includes the header, which we dont want usually...
        payload: output_buffer,
    })
}
