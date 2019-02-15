mutable struct Buffer
    max_size::Integer
    entries::SortedSet{Entry}

    Buffer(max_size::Integer) = new(max_size, SortedSet{Entry}()) 
end

function get(b::Buffer, key)
    result = searchsortedfirst(b.entries, key)
    if result.key != key 
        return nothing
    end
    return isdeleted(result) ? nothing : result.val
end

function Base.push!(b::Buffer, key, value, deleted=false)
    if length(b.entries) == b.max_size
        return false
    else
        entry = Entry{typeof(key), typeof(value)}(key, value, deleted)
        push!(b.entries, entry)
        return true
    end
end

empty!(b::Buffer) = b.entries = empty(b.entries)