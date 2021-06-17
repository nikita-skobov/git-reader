
use std::path::PathBuf;

pub enum PartiallyResolvedPackFile {
    Unresolved(PathBuf),
    Resolved(PathBuf),
}

pub struct PackFile {

}
