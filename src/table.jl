struct Table{K, V}
    id::Int64
    size::Int64
    entries::BlobVector{Entry{K, V}}
end

inmemory_tables = Dict{Int64, Blob}()

Base.length(t::Table) = t.size[]
min(t::Table) = t.entries[1].key[]
max(t::Table) = t.entries[t.size].key[]

function create_id(::Type{Table})
    length(inmemory_tables) == 0 && return 1
    return reverse(sort(collect(keys(inmemory_tables))))[1] + 1
end

function get_table(::Type{Table{K, V}}, id::Int64) where {K, V}
    haskey(inmemory_tables, id) && return inmemory_tables[id]
    path = "blobs/$id.tbl"
    if isfile(path)
        open(path) do f
            size = filesize(f)
            p = Libc.malloc(size)
            b = Blob{Table{K, V}}(p, 0, size)
            unsafe_read(f, p, size)
            inmemory_tables[b.id[]] = b
        end
        return inmemory_tables[id]
    end
    nothing
end

function write(t::Blob{Table{K, V}}) where {K, V}
    open("blobs/$(t.id[]).tbl", "w+") do file
        unsafe_write(file, pointer(t), getfield(t, :limit))
    end
end

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
    l.id[] = create_id(Table)
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
    i, j, size = 1, start_index + 1, 0
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

function split(t::Blob{Table{K, V}}) where {K, V} 
    mid = floor(Int, t.size[] / 2)
    t1_entries = Vector{Blob{Entry{K, V}}}()
    t2_entries = Vector{Blob{Entry{K, V}}}()
    for i in 1:mid 
        push!(t1_entries, t.entries[i]) 
    end
    for i in mid + 1:t.size[]
        push!(t2_entries, t.entries[i]) 
    end
    t1 = Blobs.malloc_and_init(Table{K, V}, t1_entries)
    t2 = Blobs.malloc_and_init(Table{K, V}, t2_entries)
    return (t1, t2)
end