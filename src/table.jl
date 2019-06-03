struct Table{K, V}
    id::Int64
    size::Int64
    entries::BlobVector{Entry{K, V}}
end

inmemory_tables = Dict{Int64, Blob{Table}}()

Base.length(t::Table) = length(t.entries)
min(t::Table) = t.entries[1].key[]
max(t::Table) = t.entries[t.size].key[]
new_tid() = reverse(sort(collect(keys(inmemory_levels)))) + 1

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

function Base.merge(t::Table{K, V},
                    v::BlobVector{Entry{K, V}},
                    start_index::Int64,
                    end_index::Int64,
                    force_remove) where {K, V}
    length(t) == 0 && return Table{K, V}(new_tid(), v, sizeof(v))
    size = t.size[] + length(v[])
    res_entries = Blobs.malloc_and_init(BlobVector{Entry{K, V}}, size)
    i, j, size = 1, 1, 0
    while i <= length(t) && j <= length(v)
        if isequal(t.entries[i][], v[j][])
            if !force_remove || !v[j].deleted[]
                res_entries[i + j - 1] = v[j]
                size += 1
            end
            i += 1
            j += 1
        elseif t.entries[i][] < v[j][]
            if !force_remove || !t.entries[i].deleted[] 
                res_entries[i + j - 1] = t.entries[j]
                size += 1
            end
            i += 1
        else
            if !force_remove || !v[j].deleted[] 
                res_entries[i + j - 1] = v[j]
                size += 1
            end
            j += 1
        end
    end
    while i <= length(t)
        res_entries[i + j - 1] = t.entries[j]
        size += 1
        i += 1
    end
    while j <= length(v)
        res_entries[i + j - 1] = v[j]
        size += 1
        j += 1
    end
    return Table{K, V}(size, res_entries, size)
end

function split(t::Table{K, V}) where {K, V} 
    mid = floor(Int, t.size / 2)
    p1_entries = Blobs.malloc_and_init(BlobVector{Entry{K, V}}, mid)
    for i in 1:mid p1_entries[i] = t.entries[i] end
    t1 = Table{K, V}(new_tid(), p1_entries, mid)
    p2_entries = Blobs.malloc_and_init(BlobVector{Entry{K, V}}, t.size - mid)
    for i in mid + 1:t.size p2_entries[i - mid] = t.entries[i] end
    t2 = Table{K, V}(new_tid(), p2_entries, t.size - mid)
    return (t1, t2)
end