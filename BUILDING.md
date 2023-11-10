# Building

## Clone

First, clone the project with `git clone https://github.com/kaspar-p/kvstore`

The project uses git submodules to import the hashing function library [xxHash](https://github.com/Cyan4973/xxHash).
It exists within the `third_party` directory. When cloning the package, it won't automatically come
with, initialize it with:
```
git submodules update --all
```

## Build

```sh
cmake -S . -B build
cmake --build build
```
for the debug build. For release:
```
cmake -S . -B build -D CMAKE_BUILD_TYPE=Release
cmake --build build
```

The `main.cpp` compiles into a binary `kvstore`, run with:
```sh
./build/kvstore
```

But most likely the binary to run is the test binary.

## Test

The same commands, but the executable is placed differently in the filesystem:

```sh
cmake -S . -B build
cmake --build build
./build/tests/kvstore_test
```
