mutable struct Buffer{K, V}
    bf::Blob{BloomFilter}
    size::Integer
    max_size::Integer
    entries::Vector{Entry{K, V}}
    function Buffer{K, V}(max_size::Integer) where {K, V} 
        bf = Blobs.malloc_and_init(BloomFilter, max_size, 0.001, 10)
        new{K, V}(bf, 0, max_size, Vector{Entry{K, V}}())
    end
end

isfull(b::Buffer) = b.size >= b.max_size

function Base.empty!(b::Buffer{K, V}) where {K, V}
    b.entries = Vector{Entry{K, V}}()
    b.size = 0
end

function Base.put!(b::Buffer{K, V}, key, val, deleted=false) where {K, V}
    # if in(key, b.bf[])
    #     sort!(b.entries)
    #     i = bsearch(b.entries, 1, length(b.entries), key)
    #     i > 0 && deleteat!(b.entries, i)
    # end
    e = Entry{K, V}(key, val, deleted)
    b.size += 1
    # add!(b.bf[], key)
    push!(b.entries, e)
end

function Base.get(b::Buffer{K, V}, key) where {K, V}
    !in(b.bf[], key) && nothing
    sort!(b.entries)
    i = bsearch(b.entries, 1, length(b.entries), key)
    i > 0 ? (isdeleted(b.entries[i]) ? nothing : b.entries[i].val) : nothing
end

function to_blob(b::Buffer{K, V}) where {K, V} 
    sort!(b.entries)
    size = length(b.entries)
    bv = Blobs.malloc_and_init(BlobVector{Entry{K, V}}, size)
    for i in 1:size
        bv[i].key[] = b.entries[i].key
        bv[i].val[] = b.entries[i].val
        bv[i].deleted[] = b.entries[i].deleted
    end
    return bv
end