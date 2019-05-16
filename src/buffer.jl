mutable struct Buffer{K, V}
    size::Integer
    max_size::Integer
    entries::Vector{Entry{K, V}}
    Buffer{K, V}(max_size::Integer) where {K, V} = 
        new{K, V}(0, max_size, Vector{Entry{K, V}}())
end

isfull(b::Buffer) = b.size >= b.max_size

function Base.empty!(b::Buffer{K, V}) where {K, V}
    b.entries = Vector{Entry{K, V}}()
    b.size = 0
end

# TODO: Check for duplicates
function Base.put!(b::Buffer{K, V}, key, val, deleted=false) where {K, V}
    e = Entry{K, V}(key, val, deleted)
    b.size += sizeof(e)
    push!(b.entries, e)
end

function Base.get(b::Buffer{K, V}, key) where {K, V}
    sort!(b.entries)
    i = bsearch(b.entries, 1, length(b.entries), key)
    i > 0 ? (isdeleted(b.entries[i]) ? nothing : b.entries[i].val) : nothing
end