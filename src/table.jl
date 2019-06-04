struct Table{K, V}
    id::Int64
    size::Int64
    entries::BlobVector{Entry{K, V}}
end

inmemory_tables = Dict{Int64, Blob{Table}}()

Base.length(t::Table) = t.size[]
min(t::Table) = t.entries[1].key[]
max(t::Table) = t.entries[t.size].key[]
new_tid() = reverse(sort(collect(keys(inmemory_levels)))) + 1

function Blobs.child_size(::Type{Table{K, V}}, 
                          entries::Vector{Blob{Entry{K, V}}}) where {K, V}
    T = Table{K, V}
    Blobs.child_size(fieldtype(T, :entries), length(entries))
end

function Blobs.init(l::Blob{Table{K, V}}, 
                    free::Blob{Nothing}, 
                    entries::Vector{Blob{Entry{K, V}}}) where {K, V}
    free = Blobs.init(l.entries, free, length(entries))
    for i in 1:length(entries)
        l.entries[i].key[] = entries[i][].key
        l.entries[i].val[] = entries[i][].val
        l.entries[i].deleted[] = entries[i][].deleted
    end
    l.size[] = length(entries)
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

function Base.merge(t::Blob{Table{K, V}},
                    v::Blob{BlobVector{Entry{K, V}}},
                    start_index::Int64,
                    end_index::Int64,
                    force_remove) where {K, V}
    result_entries = Vector{Blob{Entry{K, V}}}()
    if length(t[]) == 0 
        for e in v push!(result_entries, e) end
        return Blobs.malloc_and_init(Table{K, V}, result_entries)
    end
    i, j, size = 1, start_index, 0
    while i <= length(t[]) && j <= end_index
        if isequal(t.entries[i][], v[j][])
            if !force_remove || !v[j].deleted[]
                push!(result_entries, v[j])
                size += 1
            end
            i += 1
            j += 1
        elseif t.entries[i][] < v[j][]
            if !force_remove || !t.entries[i].deleted[] 
                push!(result_entries, t.entries[i])
                size += 1
            end
            i += 1
        else
            if !force_remove || !v[j].deleted[] 
                push!(result_entries, v[j])
                size += 1
            end
            j += 1
        end
    end
    while i <= length(t[])
        push!(result_entries, t.entries[i])
        size += 1
        i += 1
    end
    while j <= end_index
        push!(result_entries, v[j])
        size += 1
        j += 1
    end
    return Blobs.malloc_and_init(Table{K, V}, result_entries)
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