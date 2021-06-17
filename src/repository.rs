
/// contains the filepaths that are needed
/// for future operations on this repository.
/// these file paths must be guaranteed to exist.
/// ie: if you have a repo object, and it has a objects
/// field with a path, then that path must point to the
/// objects directory, and it must exist.
/// TODO: there are obviously many more files/folders
/// that can be in the git repo folder, but for now, I think
/// these are the only ones we care about. In the future, update this
/// to contain other folders/files if we need them. See:
/// https://git-scm.com/docs/gitrepository-layout
pub struct Repo {
    
}