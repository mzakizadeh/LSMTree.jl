struct Table{K, V}
    id::Int64
    size::Int64
    entries::BlobVector{Entry{K, V}}
end

inmemory_tables = Dict{Int64, Blob{Table}}()

Base.length(t::Table) = length(t.entries)
min(t::Table) = t.entries[1].key[]
max(t::Table) = t.entries[length(t.entries)].key[]

function table(id::Int64,
               size::Int64) where {K, V}
    t = Blobs.malloc_and_init(Table{K, V}, size)
    return t
end

function Blobs.child_size(::Type{Table{K, V}}, capacity::Int) where {K, V}
    T = Table{K, V}
    Blobs.child_size(fieldtype(T, :entries), capacity)
end

function Blobs.init(l::Blob{Table{K, V}}, 
                    free::Blob{Nothing}, 
                    capacity::Int) where {K, V}
    free = Blobs.init(l.entries, free, capacity)
    l.size[] = 0
    free
end

function Base.get(t::Table{K, V}, key::K) where {K, V} 
    i = bsearch(t.entries, 1, length(t), convert(K, key))
    if i > 0
        result = t.entries[i][]
        return isdeleted(result) ? nothing : result.val
    end
    return nothing
end

function Base.merge(t::Table{K, V}, v::Vector{Blob{Entry{K, V}}}, force_remove=false) where {K, V}
    length(t) == 0 && return Table{K, V}(v, sizeof(v))
    # res_index, res_entries = Vector{Index{K}}(), Vector{Blob{Entry{K, V}}}()
    res_entries = Vector{Blob{Entry{K, V}}}()
    size = 0
    i, j = 1, 1
    while i <= length(t) && j <= length(v)
        if isequal(t.entries[i][], v[j][])
            if !force_remove || !v[j].deleted[]
                push!(res_entries, v[j])
                size += sizeof(v[j])
            end
            i += 1
            j += 1
        elseif t.entries[i][] < v[j][]
            if !force_remove || !t.entries[i].deleted[] 
                push!(res_entries, t.entries[i])
                size += sizeof(t.entries[i])
            end
            i += 1
        else
            if !force_remove || !v[j].deleted[] 
                push!(res_entries, v[j])
                size += sizeof(v[j])
            end
            j += 1
        end
    end
    while i <= length(t)
        push!(res_entries, t.entries[i])
        size += sizeof(t.entries[i])
        i += 1
    end
    while j <= length(v)
        push!(res_entries, v[j])
        size += sizeof(v[j])
        j += 1
    end
    return Table{K, V}(res_entries, size)
end

# TODO: split based on size
function split(t::Table{K, V}) where {K, V} 
    len = length(t)
    mid = floor(Int, len / 2)
    p1_entries = t.entries[1:mid]
    p1_size = sizeof(t.entries[1:mid])
    p2_entries = t.entries[mid + 1:len]
    p2_size = sizeof(t.entries[mid + 1:len])
    return (Table{K, V}(p1_entries, p1_size), Table{K, V}(p2_entries, p2_size))
end