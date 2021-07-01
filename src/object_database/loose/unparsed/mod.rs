use std::{io, str::FromStr};
use crate::ioerre;

pub mod decode;
pub use decode::*;

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
