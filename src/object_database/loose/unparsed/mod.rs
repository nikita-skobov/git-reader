use std::{io, str::FromStr};
use crate::ioerre;
use tinyvec::TinyVec;

pub mod decode;
pub use decode::*;

/// maximum size we want to allocate on the stack
/// for reading raw git objects. if a git object is larger
/// than this amount, TinyVec will turn it into a heap
/// allocated vector. This is very efficient if
/// the majority of what youre parsing is commits/trees
/// as most of them should fit in this size.
/// but for blobs, it most likely will always be heap allocated.
pub const UNPARSED_PAYLOAD_STATIC_SIZE: usize = 4096;

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
    pub payload: TinyVec<[u8; UNPARSED_PAYLOAD_STATIC_SIZE]>,
}
