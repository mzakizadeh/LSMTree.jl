struct Index{K}
    key::K
    pos::Int64
end

mutable struct Level{K, V}
    id::Integer
    size::Integer
    max_size::Integer
    index::BlobVector{Index{K}}
    bf::BloomFilter
    forcewrite::Bool
    function Level{K, V}(id::Integer, max_size::Integer, forcewrite::Bool=false) where {K, V}
        T = Index{K}
        data = Blob{T}(Libc.malloc(sizeof(T) * max_size), 0, sizeof(T) * max_size)
        bv = BlobVector{T}(data, max_size)
        new{K, V}(id, 0, max_size, bv, BloomFilter{K}(convert(Int, max_size * 0.5)), forcewrite)
    end
end

isfull(l::Level) = l.size == l.max_size
getpath(l::Level) = joinpath(@__DIR__, "..", "blobs", "$(l.id)") 
Base.empty!(l::Level) = l.size = 0

Base.isless(i1::Index, i2::Index) = i1.key < i2.key

function read(l::Level{K, V}) where {K, V}
    open(getpath(l), "r") do f
        T = Entry{K, V}
        data = Blob{T}(Libc.malloc(sizeof(T) * l.size), 0, sizeof(T) * l.size)
        bv = BlobVector{T}(data, l.size)

        i = 1
        b = Blobs.malloc_and_init(Entry{K, V})
        while i <= l.size
            unsafe_read(f, pointer(b), sizeof(T))
            bv[i] = b[]
            i += 1
        end

        return bv
    end
end

function Base.merge!(l::Level{K, V}, bv::BlobVector{Entry{K, V}}) where {K, V}
    for e in bv
        if isset(l.bf, e.key)
            i = bsearch(l.index, 1, l.size, e.key)
            (i != 0) ? update_entry(l, e, i) : add_entry(l, e)
        else add_entry(l, e) end
    end
end

function update_entry(l::Level{K, V}, e::Entry{K, V}, i) where {K, V}
    open(getpath(l), "w") do f
        seek(f, l.index[i].pos)
        b = Blobs.malloc_and_init(typeof(e))
        b[] = e
        unsafe_write(f, pointer(b), sizeof(e))
        Blobs.free(b)
    end
end

function add_entry(l::Level{K, V}, e::Entry{K, V}) where {K, V}
    open(getpath(l), "w+") do f
        seek(f, l.size * sizeof(e))
        i = l.size
        index = Index{K}(e.key, position(f))
        while i > 0 && isless(index, l.index[i])
            l.index[i + 1] = l.index[i]
            i -= 1
        end
        l.index[i + 1] = index
        l.size += 1
        b = Blobs.malloc_and_init(typeof(e))
        b[] = e
        unsafe_write(f, pointer(b), sizeof(e))
        Blobs.free(b)
    end
    set(l.bf, e.key)
end

function Base.get(l::Level{K, V}, k) where {K, V} 
    k = convert(K, k)
    if !isset(l.bf, k) return end
    i = bsearch(l.index, 1, l.size, k)
    if i != 0
        result = missing
        open(getpath(l), "r") do f
            T = Entry{K, V}
            b = Blobs.malloc_and_init(T)
            unsafe_read(f, pointer(b), sizeof(T))
            result = b[].val
            Blobs.free(b)
        end
        return result
    end
    return nothing
end