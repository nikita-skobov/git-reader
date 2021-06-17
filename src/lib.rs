use std::{io, path::{Path, PathBuf}};

pub mod repository;
pub mod object_database;
pub mod fs_helpers;
pub mod object_id;

/// returns the absolute path of the actual .git/ folder
/// from your search path
pub fn get_repository_directory<P: AsRef<Path>>(
    search_path: P
) -> io::Result<PathBuf> {
    // first check if there is a .git/ folder
    // and use that if one exists.
    let mut search_path = search_path.as_ref().to_path_buf();
    search_path.push(".git/");
    let search_path = if search_path.is_dir() {
        // search_path/.git/ exists, use this
        search_path
    } else {
        // maybe the search path is already the .git/ dir?
        search_path.pop();
        if !search_path.exists() {
            return ioerre!("{:?} does not exist", search_path);
        }
        search_path
    };

    // we know search_path exists, now check if
    // its actually a git dir, ie: does it have the
    // necessary files to make it a git dir?

    panic!()
}




/// used to make a simple io error with a string formatted message
/// use this when you want to do `some_call().map_err(ioerr!("message"))?;`
#[macro_export]
macro_rules! ioerr {
    ($($arg:tt)*) => ({
        ::std::io::Error::new(::std::io::ErrorKind::Other, format!($($arg)*))
    })
}

/// same as `ioerr` except this actually wraps it in an `Err()`
/// use this when you want to do: `return ioerre!("message")`
#[macro_export]
macro_rules! ioerre {
    ($($arg:tt)*) => ({
        Err($crate::ioerr!($($arg)*))
    })
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
