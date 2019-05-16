mutable struct Buffer{K, V}
    size::Integer
    max_size::Integer
    entries::Vector{Entry{K, V}}
    Buffer{K, V}(max_size::Integer) where {K, V} = 
        new{K, V}(0, max_size, Vector{Entry{K, V}}())
end

isfull(b::Buffer) = b.size >= b.max_size

function Base.empty!(b::Buffer{K, V}) where {K, V}
    b.entries = Vector{Blob{Entry{K, V}}}()
    b.size = 0
end

# Insertion sort
# TODO: Check for duplicates
function Base.put!(b::Buffer{K, V}, key, val, deleted=false) where {K, V}
    i = bsearch(b.entries, 1, length(b.entries), convert(K, key))
    if i > 0
        e = b.entries[i]
        e.key[], e.val[], e.deleted[] = key, val, deleted
    else
        e = Blobs.malloc_and_init(Entry{K, V})
        e.key[], e.val[], e.deleted[] = key, val, deleted
        b.size += sizeof(e)
        push!(b.entries, e)
        i = Base.length(b.entries) - 1
        while i > 0 && isless(e[], b.entries[i][])
            b.entries[i + 1] = b.entries[i]
            b.entries[i] = e
            i -= 1
        end
        b.entries[i + 1] = e
    end
end

# Binary search
function Base.get(b::Buffer{K, V}, key) where {K, V}
    i = bsearch(b.entries, 1, Base.length(b.entries), convert(K, key))
    if i > 0
        result = b.entries[i][]
        return isdeleted(result) ? nothing : result.val
    end
    return nothing
end