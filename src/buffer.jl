mutable struct Buffer{K, V}
    size::Integer
    max_size::Integer
    entries::Dict{K, Entry{K, V}}
    function Buffer{K, V}(max_size::Integer) where {K, V} 
        new{K, V}(0, max_size, Dict{K, Entry{K, V}}())
    end
end

isfull(b::Buffer) = b.size >= b.max_size

function Base.empty!(b::Buffer{K, V}) where {K, V}
    empty!(b.entries)
    b.size = 0
end

function Base.put!(b::Buffer{K, V}, key, val, deleted=false) where {K, V}
    b.size += haskey(b.entries, key) ? 0 : 1
    b.entries[key] = Entry(key, val, deleted)
end

function Base.get(b::Buffer{K, V}, key) where {K, V}
    haskey(b.entries, key) && return b.entries[key]
    nothing
end

function to_blob(b::Buffer{K, V}) where {K, V} 
    c = collect(b.entries)
    sort!(c)
    bv = Blobs.malloc_and_init(BlobVector{Entry{K, V}}, b.size)
    for i in 1:b.size
        bv[i].key[] = c[i][1]
        bv[i].val[] = c[i][2].val
        bv[i].deleted[] = c[i][2].deleted
    end
    return bv
end
