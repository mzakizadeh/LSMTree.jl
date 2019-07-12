struct Table{K, V}
    id::Int64
    size::Int64
    entries::BlobVector{Entry{K, V}}
end

Base.length(t::Table) = t.size[]
Base.min(t::Table) = t.entries[1].key[]
Base.max(t::Table) = t.entries[t.size].key[]

# TODO write a better id generator
function generate_id(::Type{Table}, s::InMemoryData)
    only_tables_pattern = x -> occursin(r"([0-9])+(.tbl)$", x)
    file_names = filter(only_tables_pattern, readdir(s.path))
    length(file_names) == 0 && return 1
    return findmax(map(x -> parse(Int64, replace(x, ".tbl" => "")), 
                         file_names))[1] + 1
end

function get_table(::Type{Table{K, V}}, id::Int64, s::InMemoryData) where {K, V}
    id <= 0 && return nothing
    if in(id, s.tables_queue)
       index = findfirst(x -> x == id, s.tables_queue) 
       deleteat!(s.tables_queue, index)
       pushfirst!(s.tables_queue, id)
       return s.inmemory_tables[id]
    end
    path = "$(s.path)/$id.tbl"
    if isfile(path)
        open(path) do f
            size = filesize(f)
            p = Libc.malloc(size)
            b = Blob{Table{K, V}}(p, 0, size) 
            unsafe_read(f, p, size)
            s.inmemory_tables[b.id[]] = b
        end
        pushfirst!(s.tables_queue, id)
        length(s.tables_queue) > 100 && delete!(s.inmemory_tables, 
                                             pop!(s.tables_queue))
        return s.inmemory_tables[id]
    end
    error("Table does not exist! (path=$path)")
end

function set_table(t::Blob{Table{K, V}}, s::InMemoryData) where {K, V}
    open("$(s.path)/$(t.id[]).tbl", "w+") do file
        unsafe_write(file, pointer(t), getfield(t, :limit))
    end
end

function Blobs.child_size(::Type{Table{K, V}}, 
                          id::Int64,
                          entries::Vector{Entry{K, V}}) where {K, V}
    T = Table{K, V}
    Blobs.child_size(fieldtype(T, :entries), length(entries))
end

function Blobs.init(l::Blob{Table{K, V}}, 
                    free::Blob{Nothing}, 
                    id::Int64,
                    entries::Vector{Entry{K, V}}) where {K, V}
    free = Blobs.init(l.entries, free, length(entries))
    for i in 1:length(entries)
        l.entries[i].key[] = entries[i].key
        l.entries[i].val[] = entries[i].val
        l.entries[i].deleted[] = entries[i].deleted
    end
    l.id[] = id
    l.size[] = length(entries)
    free
end

function Base.get(t::Table{K, V}, key::K) where {K, V} 
    i = bsearch(t.entries, 1, length(t), key)
    if i > 0
        result = t.entries[i]
        return result
    end
    nothing
end

function Base.merge(s::InMemoryData,
                    t::Blob{Table{K, V}},
                    v::BlobVector{Entry{K, V}},
                    start_index::Int64,
                    end_index::Int64,
                    force_remove) where {K, V}
    result_entries = Vector{Entry{K, V}}()
    i, j, size = 1, start_index + 1, 0
    while i <= length(t[]) && j <= end_index
        if isequal(t.entries[i][], v[j])
            if !force_remove || !v[j].deleted
                push!(result_entries, v[j])
                size += 1
            end
            i += 1
            j += 1
        elseif t.entries[i][] < v[j]
            if !force_remove || !t.entries[i][].deleted 
                push!(result_entries, t.entries[i][])
                size += 1
            end
            i += 1
        else
            if !force_remove || !v[j].deleted
                push!(result_entries, v[j])
                size += 1
            end
            j += 1
        end
    end
    while i <= length(t[])
        if !force_remove || !t.entries[i][].deleted 
            push!(result_entries, t.entries[i][])
            size += 1
        end
        i += 1
    end
    while j <= end_index
        if !force_remove || !v[j].deleted
            push!(result_entries, v[j])
            size += 1
        end
        j += 1
    end
    return Blobs.malloc_and_init(Table{K, V}, 
                                 generate_id(Table, s),
                                 result_entries)
end

function split(t::Blob{Table{K, V}}, s::InMemoryData) where {K, V} 
    mid = floor(Int, t.size[] / 2)
    t1_entries = Vector{Entry{K, V}}()
    t2_entries = Vector{Entry{K, V}}()
    for i in 1:mid 
        push!(t1_entries, t.entries[i][]) 
    end
    for i in mid + 1:t.size[]
        push!(t2_entries, t.entries[i][]) 
    end
    t1 = Blobs.malloc_and_init(Table{K, V}, generate_id(Table, s), t1_entries)
    t2 = Blobs.malloc_and_init(Table{K, V}, generate_id(Table, s), t2_entries)
    t2.id[] = t2.id[] + 1
    return (t1, t2)
end