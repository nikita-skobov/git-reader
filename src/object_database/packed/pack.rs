
use std::path::PathBuf;

pub enum PartiallyResolvedPackFile {
    Unresolved(PathBuf),
    Resolved(PackFile),
}

pub struct PackFile {

}
