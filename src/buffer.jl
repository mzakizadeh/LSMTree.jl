mutable struct Buffer{K, V}
    size::Integer
    max_size::Integer
    entries::BlobVector{Entry{K, V}}
    function Buffer{K, V}(max_size::Integer) where {K, V}
        T = Entry{K, V}
        data = Blob{T}(Libc.malloc(sizeof(T) * max_size), 0, sizeof(T) * max_size)
        bv = BlobVector{T}(data, max_size)
        new{K, V}(0, max_size, bv)
    end
end

Base.empty!(b::Buffer) = b.size = 0

# Binary search
function Base.get(b::Buffer{K, V}, key) where {K, V}
    i = bsearch(b.entries, 1, b.size, convert(K, key))
    if i > 0
        result = b.entries[i]
        return isdeleted(result) ? nothing : result.val
    end
    return nothing
end

# Insertion sort
function Base.put!(b::Buffer{K, V}, key, val, deleted=false) where {K, V}
    if b.size == b.max_size
        return false
    else
        i = b.size
        e = Entry{K, V}(key, val, deleted)
        while i > 0 && isless(e, b.entries[i])
            b.entries[i + 1] = b.entries[i]
            i -= 1
        end
        b.entries[i + 1] = e
        b.size += 1
        return true
    end
end