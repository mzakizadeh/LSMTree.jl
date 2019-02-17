mutable struct Buffer{K, V}
    max_size::Integer
    entries::SortedSet{Entry{K, V}}
    Buffer{K, V}(max_size::Integer) where {K, V} = new(max_size, SortedSet{Entry{K, V}}()) 
end

Base.empty!(b::Buffer) = b.entries = empty(b.entries)

function get(b::Buffer, key)
    result = searchsortedfirst(b.entries, key)
    if result.key != key 
        return nothing
    end
    return isdeleted(result) ? nothing : result.val
end

function Base.push!(b::Buffer{K, V}, key, value, deleted=false) where {K, V}
    if length(b.entries) == b.max_size
        return false
    else
        entry = Entry{K, V}(key, value, deleted)
        push!(b.entries, entry)
        return true
    end
end