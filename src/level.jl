mutable struct Level{K, V}
    id::Integer
    size::Integer
    max_size::Integer
    blob::Blob{BlobVector{Entry{K, V}}}
    # bf::BloomFilter
    function Level{K, V}(id::Integer, max_size::Integer) where {K, V}
        bbv = Blobs.malloc_and_init(BlobVector{Entry{K, V}}, max_size)
        new{K, V}(id, 0, max_size, bbv)
    end
end

isfull(l::Level) = l.size == l.max_size
Base.empty!(l::Level) = l.size = 0

function write(l::Level{K, V}) where {K, V}
    fname = joinpath(@__DIR__, "..", "blobs", l.id) 
    open(fname, "w") do f
        size = l.max_size * sizeof(Entry{K, V})
        unsafe_write(f, l.blob, size)
    end
end

function read(l::Level{K, V}) where {K, V}
    fname = joinpath(@__DIR__, "..", "blobs", l.id) 
    open(fname, "r") do f
        size = l.max_size * sizeof(Entry{K, V})
        unsafe_read(f, l.entries.data, size)
    end
end

function Base.insert!(l::Level, e::Entry) 
    # set(l.bloom_filter, e.key)
    l.size = l.size + 1
    l.blob[][l.size] = e
end

function Base.get(l::Level, k::K) where K 
    # if !isset(l.bloom_filter, k) return end
    read(l)
    for e in collect(l.entries)
        if(e.key == k)
            return isdeleted(e) ? nothing : e.val
        end
    end
    return nothing
end