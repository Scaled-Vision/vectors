# Vector Work

The vector work performed in September 2019 was to investigate HNSW (not completed) and to investigate bulk loading and unloading of vectors into and out of mysql after encoding in base64.

This work includes a c++ header [base64.hpp](base64.hpp) that encodes/decodes memory blobs (vectors) into and out of base64. The header does not perform any memory allocations except on the stack.

## Code Dependencies

There are a few code dependencies that cannot be checked into github.

+ Fassis -- all of fassis does not need to be included here. Only the Swift Vector reading routines are needed. These are extracted into [orca_vh.cpp](orca_vh.cpp)

+ The Swift 1M vector set. Unpack these vectors into a subdirectory named `sift1M`. This is exactly what the fassis demo programs expect. The 1M vector set can be found [here](http://corpus-texmex.irisa.fr/).

+ A running mysql instance. The credentials, server address and port are embedded in [orca_vh.cpp](orca_vh.cpp).

## Performance
Running in a virtual machine on a MacBook Pro.

```
$ ./orca_vh
[0.000 s] Loading train set
[0.040 s] Done Loading train set d=128 N=100000
[0.040 s] Loading database
[0.419 s] Indexing database, d=128 N=1000000
[0.419 s] Loading queries
[0.423 s] Queries database, d=128 N=10000
inserted 10000 vectors in 0.652s
inserted 100000 vectors in 2.731s
inserted 1000000 vectors in 29.449s
retrieveded 10000 vectors in 30.699s
retrieveded 100000 vectors in 32.713s
retrieveded 1000000 vectors in 43.529s
```

### Mysql Settings
  Mysql defaults:

  | Default  | Value |
  | :------- | :----- |
  | **Host** | 127.0.0.1 |
  | **Port** | 3306 |
  | **User** | vectoruser |
  | **Password** | vectorpw |

### Compilation

```
g++-8 -O3 -I/home/bcarp/json/include -g -o orca_vh orca_vh.cpp -lmysqlclient
```
