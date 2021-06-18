use git_walker::object_database;

pub fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let first = match args.get(1) {
        Some(f) => f,
        None => {
            eprintln!("Must provide a path to the .git/objects/ directory");
            std::process::exit(1);
        }
    };

    let right_now = std::time::Instant::now();
    let mut odb = match object_database::UnparsedObjectDB::new(first) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Failed to create object DB: {}", e);
            std::process::exit(1);
        }
    };

    match odb.loose.resolve_all() {
        Err(e) => {
            eprintln!("Failed to resolve all loose objects: {}", e);
            std::process::exit(1);
        }
        Ok(_) => {}
    }

    let num_objects_traversed = odb.loose.map.len();
    println!("Traversed {} raw objects in {}ms", num_objects_traversed, right_now.elapsed().as_millis());
}
