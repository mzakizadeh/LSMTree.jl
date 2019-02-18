mutable struct Level{K, V}
    id::Integer
    size::Integer
    max_size::Integer
    entries::BlobVector{Entry{K, V}}
    bf::BloomFilter
    function Level{K, V}(id::Integer, max_size::Integer) where {K, V}
        T = Entry{K, V}
        data = Blob{T}(Libc.malloc(sizeof(T) * max_size), 0, sizeof(T) * max_size)
        bv = BlobVector{T}(data, max_size)
        new{K, V}(id, 0, max_size, bv, BloomFilter{K}(convert(Int, max_size * 0.5)))
    end
end

isfull(l::Level) = l.size == l.max_size
Base.empty!(l::Level) = l.size = 0

function write(l::Level{K, V}) where {K, V}
    fname = joinpath(@__DIR__, "..", "blobs", "$(l.id)") 
    open(fname, "w+") do f
        size = l.max_size * sizeof(Entry{K, V})
        unsafe_write(f, pointer(l.entries.data), size)
    end
end

function read(l::Level{K, V}) where {K, V}
    fname = joinpath(@__DIR__, "..", "blobs", "$(l.id)") 
    open(fname, "r") do f
        size = l.max_size * sizeof(Entry{K, V})
        unsafe_read(f, pointer(l.entries.data), size)
    end
end

function Base.insert!(l::Level{K, V}, e::Entry{K, V}) where {K, V} 
    set(l.bf, e.key)
    l.size += 1
    l.entries[l.size] = e
end

function Base.get(l::Level, k::K) where K 
    if !isset(l.bf, k) return end
    read(l)
    for e in collect(l.entries)
        if(e.key == k)
            return isdeleted(e) ? nothing : e.val
        end
    end
    return nothing
end