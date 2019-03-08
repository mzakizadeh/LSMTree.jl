# struct Index{K}
#     key::K
#     offset::Integer
#     size::Integer
# end

struct Table{K, V}
    # index::Vector{Index{K}}
    entries::Vector{Blob{Entry{K, V}}}
    size::Int64
    min::K
    max::K
end

Base.sizeof(t::Table) = sizeof(t.entries)
Base.length(t::Table) = length(t.entries)
min(t::Table) = t.entries[1]
max(t::Table) = t.entries[length(t.entries)]

function Base.get(t::Table{K, V}, key::K) where {K, V} 
    i = bsearch(t.entries, 1, t.size, convert(K, key))
    if i > 0
        result = t.entries[i]
        return isdeleted(result) ? nothing : result.val
    end
    return nothing
end

function Base.merge(t1::Table{K, V}, t2::Table{K, V}) where {K, V}
    # res_index, res_entries = Vector{Index{K}}(), Vector{Blob{Entry{K, V}}}()
    res_min = t1.min < t2.min ? t1.min : t2.min
    res_max = t1.max > t2.max ? t1.max : t2.max
    res_entries = Vector{Blob{Entry{K, V}}}()
    i, j = 1, 1
    while i <= length(t1) && j <= length(t2)
        if t1.entries[i] < t2.entries[j]
            push!(res_entries, t1.entries[i])
            i += 1
        else
            push!(res_entries, t2.entries[j])
            j += 1
        end
    end
    while i <= length(t1)
        push!(res_entries, t1.entries[i])
    end
    while j <= length(t2)
        push!(res_entries, t2.entires[j])
    end
    return Table{K, V}(res_entries, length(t1) + length(t2), res_min, res_max)
end

#TODO: split based on size
function split(t::Table{K, V}) where {K, V} 
    mid = t.size / 2
    p1_entries = t[1:mid]
    p2_entries = t[mid + 1:t.size]
    return (
        Table{K, V}(p1_entries, length(p1_entries), p1_entries[1], p1_entries[mid]), 
        Table{K, V}(p2_entries, length(p2_entries), p2_entries[1], p2_entries[length(p2_entries)]))
end