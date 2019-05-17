mutable struct Buffer{K, V}
    bf::BloomFilter
    size::Integer
    max_size::Integer
    entries::Vector{Entry{K, V}}
    function Buffer{K, V}(max_size::Integer) where {K, V} 
        bf = BloomFilter(floor(Int, max_size / sizeof(Entry{K, V})), 0.001)
        new{K, V}(bf, max_size, Vector{Entry{K, V}}())
    end
end

isfull(b::Buffer) = b.size >= b.max_size

function Base.empty!(b::Buffer{K, V}) where {K, V}
    b.entries = Vector{Entry{K, V}}()
    b.size = 0
end

# TODO: Check for duplicates
function Base.put!(b::Buffer{K, V}, key, val, deleted=false) where {K, V}
    if in(key, b.bf)
        sort!(b.entries)
        i = bsearch(b.entries, 1, length(b.entries), key)
        i > 0 && deleteat!(b.entries, i)
    end
    e = Entry{K, V}(key, val, deleted)
    b.size += sizeof(e)
    add!(b.bf, key)
    push!(b.entries, e)
end

function Base.get(b::Buffer{K, V}, key) where {K, V}
    !in(b.bf, key) && nothing
    sort!(b.entries)
    i = bsearch(b.entries, 1, length(b.entries), key)
    i > 0 ? (isdeleted(b.entries[i]) ? nothing : b.entries[i].val) : nothing
end