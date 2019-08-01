# LSMTree.jl

## About

This package provides a fast key/value store that is efficient for high-volume 
random access reads and writes. This is a implementation of a write-optimized 
data structure called log-structured merge-tree in Julia.

## Quick Start

### Create New Data Store

```julia
julia> s = LSMTree.Store{Int64, Int64}("./db")
LSMTree.Store{Int64, Int64} with 0 entries
```

### Put Data

```julia
julia> put!(s, 1, 3)
3

julia> put!(s, 2, 15)
15

julia> s
LSMTree.Store{Int64, Int64} with 2 entries
```

Or you can simply use brackets to assign an index:

```julia
julia> s[1] = 12

julia> s[134] = 13

julia> s
LSMTree.Store{Int64, Int64} with 3 entries
```

### Get Data

```julia
julia> get(s, 1)
12

julia> get(s, 2)
15

julia> get(s, 134)
13
```

Or you can simply use brackets access an index:

```julia
julia> s[1]
12

julia> s[2]
15

julia> s[134]
13
```

### Close Data Store

Always call `close` function after you done with store. This function will flush
all data in memory buffer to disk and make a restorable file of store on disk.

```julia
julia> close(s)
LSMTree.Store{Int64, Int64} with id 2 closed
```

### Restore Old Data Store

```julia 
julia> s = LSMTree.restore(Int64, Int64, "./db", 2)
LSMTree.Store{Int64, Int64} with 3 entries
```

### Iterate Over Data

```julia
julia> let iter = LSMTree.Iterator(s), st = LSMTree.iter_init(iter)
           while !LSMTree.iter_done(iter, st)
               (e, st) = LSMTree.iter_next(iter, st)
               println(e)
           end
       end
Main.LSMTree.Entry{Int64,Int64}(1, 12, false)
Main.LSMTree.Entry{Int64,Int64}(2, 15, false)
Main.LSMTree.Entry{Int64,Int64}(134, 13, false)
```