
use crate::object_id::Oid;

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
    Tree(usize, TreeObject),
    Blob(usize, BlobObject),
    Commit(usize, CommitObject),
    Tag(usize, TagObject),
}

impl ObjectType {
    pub fn new(header_bytes: &[u8]) -> Option<ObjectType> {
        let (type_str, size, _) = decode_object_header(header_bytes)?;
        let obj = match type_str {
            "blob" => ObjectType::Blob(size, BlobObject {}),
            "commit" => ObjectType::Commit(size, CommitObject::default()),
            "tag" => ObjectType::Tag(size, TagObject {}),
            "tree" => ObjectType::Tree(size, TreeObject {}),
            _ => return None,
        };
        Some(obj)
    }
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
