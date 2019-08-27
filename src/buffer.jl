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

function to_blob(::Type{PAGE}, b::Buffer{K, V}) where {K, V, PAGE} 
    c = collect(b.entries)
    sort!(c)
    
    T = BlobVector{Entry{K, V}}
    size = Blobs.self_size(T) + Blobs.child_size(T, b.size)
    page = malloc_page(PAGE, size)
    blob = Blob{T}(pointer(page), 0, size)
    used = Blobs.init(blob, b.size)
    @assert used - blob == size

    for i in 1:b.size
        blob[i].key[] = c[i][1]
        blob[i].val[] = c[i][2].val
        blob[i].deleted[] = c[i][2].deleted
    end

    blob, page
end
