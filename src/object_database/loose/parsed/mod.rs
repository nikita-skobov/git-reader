
use std::path::PathBuf;
use commit_object_parsing::ParseCommit;

pub mod commit_object_parsing;
pub mod tree_object_parsing;

/// TODO: do we ever want to load
/// an entire blob into memory?
#[derive(Debug)]
pub struct BlobObject {

}

/// TODO: care about tags?
#[derive(Debug)]
pub struct TagObject {

}

#[derive(Debug)]
pub enum ParsedObject<T: ParseCommit> {
    Commit(T),
    Tree(tree_object_parsing::TreeObject),
    Blob(BlobObject),
    Tag(TagObject),
}

pub enum PartiallyParsedLooseObject<T: ParseCommit> {
    Unresolved(PathBuf),
    Parsed(ParsedObject<T>)
}
