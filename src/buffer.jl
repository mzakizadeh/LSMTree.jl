mutable struct Buffer
    max_size::Integer
    entries::SortedDict{Entry}

    Buffer(max_size::Integer) = new(max_size, SortedDict{Entry}()) 
end

function get(b::Buffer, key)
    result = searchsortedfirst(b.entries, key)
    if result.key != key 
        return nothing
    end
    return result
end

function Base.push!(b::Buffer, key, value)
    if length(b.entries) == b.max_size
        return false
    else
        entry = Entry(unsigned(key), value)
        push!(b.entries, entry)
        return true
    end
end

empty!(b::Buffer) = b.entries = empty(b.entries)