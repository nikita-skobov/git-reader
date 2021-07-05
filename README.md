# git-reader

> a library to read/parse git objects

This is a **EXPERIMENTAL** low level library that provides fast, convenient and low-memory usage ways to read git objects from the .git/objects/ database.

There are examples in the `examples/` directory which implement a few git commands using this library, but the purpose of this library is not to be a CLI, or even a high level git traversal library. Instead, this library's only goal is to provide low level primitive methods for reading/parsing git objects.

See the examples directory for how to use this library.

## License

This code is licensed under AGPL3, but I would not have been able to make it without referencing the git documentation, as well as these two amazing projects: 

- https://github.com/Byron/gitoxide
- https://github.com/speedata/gogit

If anyone mentioned above somehow sees this library, and for some reason wants to use a part of this code, I would be more than happy to offer a license exception
