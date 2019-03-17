# struct Index{K}
#     key::K
#     offset::Integer
#     size::Integer
# end

struct Table{K, V}
    # index::Vector{Index{K}}
    entries::Vector{Blob{Entry{K, V}}}
    size::Int64
    Table{K, V}(entries::Vector{Blob{Entry{K, V}}}) where {K, V} = new{K, V}(entries, length(entries))
end

Base.sizeof(t::Table) = sizeof(t.entries)
Base.length(t::Table) = length(t.entries)
min(t::Table) = t.entries[1].key[]
max(t::Table) = t.entries[length(t.entries)].key[]

function Base.get(t::Table{K, V}, key::K) where {K, V} 
    i = bsearch(t.entries, 1, t.size, convert(K, key))
    if i > 0
        result = t.entries[i][]
        return isdeleted(result) ? nothing : result.val
    end
    return nothing
end

function Base.merge(t::Table{K, V}, v::Vector{Blob{Entry{K, V}}}, force_remove=false) where {K, V}
    length(t) == 0 && return Table{K, V}(v)
    # res_index, res_entries = Vector{Index{K}}(), Vector{Blob{Entry{K, V}}}()
    res_entries = Vector{Blob{Entry{K, V}}}()
    i, j = 1, 1
    while i <= length(t) && j <= length(v)
        if isequal(t.entries[i][], v[j][])
            if !force_remove || !v[j].deleted[]
                push!(res_entries, v[j])
            end
            i += 1
            j += 1
        elseif t.entries[i][] < v[j][]
            if !force_remove || !t.entries[i].deleted[] 
                push!(res_entries, t.entries[i])
            end
            i += 1
        else
            if !force_remove || !v[j].deleted[] 
                push!(res_entries, v[j])
            end
            j += 1
        end
    end
    while i <= length(t)
        push!(res_entries, t.entries[i])
        i += 1
    end
    while j <= length(v)
        push!(res_entries, v[j])
        j += 1
    end
    return Table{K, V}(res_entries)
end

#TODO: split based on size
function split(t::Table{K, V}) where {K, V} 
    mid = floor(Int, t.size / 2)
    p1_entries = t.entries[1:mid]
    p2_entries = t.entries[mid + 1:t.size]
    return (Table{K, V}(p1_entries), Table{K, V}(p2_entries))
end