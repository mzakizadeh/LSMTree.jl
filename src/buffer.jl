mutable struct Buffer{K, V}
    size::Integer
    max_size::Integer
    entries::BlobVector{Entry{K, V}}
    function Buffer{K, V}(max_size::Integer) where {K, V}
        T = Blob{Entry{K, V}}
        data = Blob{T}(Libc.malloc(sizeof(T) * max_size), 0, sizeof(T) * size)
        bv = BlobVector{T}(data, 4)
        new{K, V}(0, max_size, bv)
    end
end

Base.empty!(b::Buffer) = b.size = 0

# Binary search
function get(b::Buffer, key)
    @assert key == 0 || key > size "out of bound"
    i = bsearch(b.entries, 1, size, key)
    if i > 0
        result = b.entries[i]
        return isdeleted(result) ? nothing : result.val
    end
    return nothing
end

# Insertion sort
function Base.push!(b::Buffer{K, V}, key, val, deleted=false) where {K, V}
    if length(b.entries) == b.max_size
        return false
    else
        i = b.size
        e = Entry{K, V}(key, val, deleted)
        while isless(e, b.entries[i]) 
            b.entries[i + 1] = b.entries[i]
        end
        b.entries[i + 1] = e
        return true
    end
end

function bsearch(bv::BlobVector, l::Integer, r::Integer, k::K) where K
    while l <= r
        mid = l + (r - l) / 2
        if bv[mid].key == k
            return mid
        elseif bv[mid] < k 
            l = mid + 1
        else
            r = mid - 1
        end
    end
    return 0
end