mutable struct Buffer{K, V}
    max_size::Integer
    entries::Vector{Blob{Entry{K, V}}}
    Buffer{K, V}(max_size::Integer) where {K, V} = new{K, V}(max_size, Vector{Blob{Entry{K, V}}}())
end

# calling empty! function on vector of blobs removed blobs itself
Base.empty!(b::Buffer{K, V}) where {K, V} = b.entries = Vector{Blob{Entry{K, V}}}()
isfull(b::Buffer) = length(b.entries) == b.max_size

# Insertion sort
function Base.put!(b::Buffer{K, V}, key, val, deleted=false) where {K, V}
    e = Blobs.malloc_and_init(Entry{K, V})
    e.key[], e.val[], e.deleted[] = key, val, deleted
    push!(b.entries, e)
    i = length(b.entries) - 1
    while i > 0 && isless(e[], b.entries[i][])
        b.entries[i + 1] = b.entries[i]
        b.entries[i] = e
        i -= 1
    end
    b.entries[i + 1] = e
end

# Binary search
function Base.get(b::Buffer{K, V}, key) where {K, V}
    i = bsearch(b.entries, 1, b.size, convert(K, key))
    if i > 0
        result = b.entries[i]
        return isdeleted(result) ? nothing : result.val
    end
    return nothing
end