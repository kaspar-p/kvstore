# Building

## Clone

First, clone the project with `git clone https://github.com/kaspar-p/kvstore`

The project uses git submodules to import the hashing function library [xxHash](https://github.com/Cyan4973/xxHash). It exists within the `third_party` directory. When cloning the package, it won't automatically come with, initialize it with:

```sh
git submodules update --all
```

## Build

```sh
cmake -S . -B build
cmake --build build
```

This builds two executables, the `main.cpp` executable, and the tests executable. The main executable isn't that useful, and is mostly just for debugging and making sure everything is working end-to-end. The tests cover all use-cases of the main executable.

The test executable is more interesting, and will run using Google's [GoogleTest](https://github.com/google/googletest) library.

The `main.cpp` compiles into a binary `kvstore`, run with:

```sh
./build/kvstore
```

The test binary can be run with:

```sh
./build/tests/kvstore_test
```
