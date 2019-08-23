struct Level{K, V}
    id::Int64
    next_level::Int64
    prev_level::Int64
    size::Int64
    max_size::Int64
    table_threshold_size::Int64
    bounds::BlobVector{K}
    tables::BlobVector{Int64}
end

isfull(l::Level) = l.size >= l.max_size
islast(l::Level) = l.next_level <= 0
isfirst(l::Level) = l.prev_level <= 0

function generate_id(::Type{Level}, inmemory::InMemoryData)
    meta, page = load_meta(inmemory)
    id = meta.next_level_id[]
    meta.next_level_id[] += 1
    save_meta(meta, page, inmemory)
    return id
end

function Blobs.child_size(::Type{Level{K, V}}, 
                          id::Int64,
                          result_tables::Vector{Int64},
                          result_bounds::Vector{K},
                          size::Int64,
                          max_size::Int64,
                          table_threshold_size::Int64) where {K, V}
    T = Level{K, V}
    +(Blobs.child_size(fieldtype(T, :bounds), length(result_bounds)),
      Blobs.child_size(fieldtype(T, :tables), length(result_tables)))
end

function Blobs.init(l::Blob{Level{K, V}},
                    free::Blob{Nothing},
                    id::Int64,
                    result_tables::Vector{Int64},
                    result_bounds::Vector{K},
                    size::Int64,
                    max_size::Int64,
                    table_threshold_size::Int64) where {K, V}
    free = Blobs.init(l.bounds, free, length(result_bounds))
    free = Blobs.init(l.tables, free, length(result_tables))
    for i in 1:length(result_tables)
        l.tables[i][] = result_tables[i]
    end
    for i in 1:length(result_bounds)
        l.bounds[i][] = result_bounds[i]
    end
    l.id[] = id
    l.size[] = size
    l.max_size[] = max_size
    l.table_threshold_size[] = table_threshold_size
    l.next_level[], l.prev_level[] = -1, -1
    free
end

function malloc_and_init(::Type{Level{K, V}}, 
                         inmemory::InMemoryData{PAGE, PAGE_HANDLE}, 
                         args...)::Blob{Level{K, V}} where {K, V, PAGE, PAGE_HANDLE}
    size = Blobs.self_size(Level{K, V}) + Blobs.child_size(Level{K, V}, args...)
    page = malloc_page(PAGE, size)
    id = args[1]
    inmemory.level_pages[id] = page
    push!(inmemory.levels_inuse, id)
    blob = Blob{Level{K, V}}(pointer(page), 0, size)
    used = Blobs.init(blob, args...)
    @assert used - blob == size
    blob
end

function empty(l::Blob{Level{K, V}}, s::InMemoryData) where {K, V}
    res = malloc_and_init(Level{K, V},
                          s, 
                          generate_id(Level, s),
                          Vector{Int64}(), 
                          Vector{K}(), 0, 
                          l.max_size[], 
                          l.table_threshold_size[])
    res.prev_level[] = l.prev_level[]
    res.next_level[] = l.next_level[]
    res
end

function copy(l::Blob{Level{K, V}}, s::InMemoryData) where {K, V}
    tables = Vector{Int64}()
    bounds = Vector{K}()
    for t in l.tables[] push!(tables, t) end
    for b in l.bounds[] push!(bounds, b) end
    res = malloc_and_init(Level{K, V},
                          s,
                          generate_id(Level, s),
                          tables,
                          bounds,
                          l.size[],
                          l.max_size[],
                          l.table_threshold_size[])
    res.prev_level[] = l.prev_level[]
    res.next_level[] = l.next_level[]
    res
end

function get_level(::Type{Level{K, V}}, 
                   id::Int64, 
                   s::InMemoryData{PAGE, PAGE_HANDLE}) where {K, V, PAGE, PAGE_HANDLE}
    id <= 0 && return nothing
    !in(id, s.levels_inuse) && push!(s.levels_inuse, id)
    haskey(s.inmemory_levels, id) && return s.inmemory_levels[id]
    path = "$(s.path)/$id.lvl"
    if isfile_pagehandle(PAGE_HANDLE, path)
        f = open_pagehandle(PAGE_HANDLE, path)
        size = filesize(f.stream)
        page = malloc_page(PAGE, size)
        blob = Blob{Level{K, V}}(pointer(page), 0, size)
        read_pagehandle(f, page, size)
        s.level_pages[id] = page
        s.inmemory_levels[blob.id[]] = blob
        close_pagehandle(f)
        return s.inmemory_levels[id]
    end
    error("Level does not exist! (path=$path)")
end

function set_level(l::Blob{Level{K, V}}, 
                   s::InMemoryData{PAGE, PAGE_HANDLE}) where {K, V, PAGE, PAGE_HANDLE}
    s.inmemory_levels[l.id[]] = l
    id = l.id[]
    path = "$(s.path)/$id.lvl"
    file = open_pagehandle(PAGE_HANDLE, path, truncate=true, read=true)
    write_pagehandle(file, s.level_pages[id], getfield(l, :limit))
    close_pagehandle(file)
end

function Base.length(l::Level)
    len = 0
    for t in l.tables len += length(t) end
    len
end

function Base.get(l::Level{K, V}, key::K, s::InMemoryData) where {K, V} 
    # if isset(l.bf, key)
        for i in 1:length(l.tables)
            i == length(l.tables) && return get(get_table(Table{K, V}, l.tables[i], s)[], key)
            if key <= l.bounds[i]
                return get(get_table(Table{K, V}, l.tables[i], s)[], key)
            end
        end
    # else return nothing end
    nothing
end

function compact(s::InMemoryData,
                 l::Blob{Level{K, V}},
                 t::Blob{Table{K, V}},
                 force_remove) where {K, V}
    indices = partition(l.bounds[], t.entries[])
    return merge(s, l, t.entries[], indices, force_remove)
end

function Base.merge(s::InMemoryData,
                    l::Blob{Level{K, V}},
                    e::BlobVector{Entry{K, V}},
                    indices::Vector{Int64}, 
                    force_remove) where {K, V}
    result_tables, result_bounds = Vector{Int64}(), Vector{K}()
    # If level has no table
    if length(indices) < 3
        if length(l.tables[]) > 0
            table = merge(s, 
                          get_table(Table{K, V}, l.tables[1][], s), 
                          e, 
                          indices[1], 
                          indices[2], 
                          false)
        else
            entries = Vector{Entry{K, V}}()
            for i in 1:length(e)
                push!(entries, e[i])
            end
            table = malloc_and_init(Table{K, V},
                                    s, 
                                    generate_id(Table, s), 
                                    entries)
        end
        if table.size[] > l.table_threshold_size[]
            (t1, t2) = split(table, s)
            set_table(t1, s)
            push!(result_tables, t1.id[])
            push!(result_bounds, max(t1[]))
            set_table(t2, s)
            push!(result_tables, t2.id[])
            push!(result_bounds, max(t2[]))
        else
            set_table(table, s)
            push!(result_tables, table.id[])
            push!(result_bounds, max(table[]))
        end
    else
        for i in 1:length(l.tables[])
            if indices[i + 1] - indices[i] > 0
                table = merge(s, 
                              get_table(Table{K, V}, l.tables[i][], s), 
                              e, 
                              indices[i], 
                              indices[i + 1], 
                              false)
                if table.size[] > l.table_threshold_size[]
                    (t1, t2) = split(table, s)
                    set_table(t1, s)
                    push!(result_tables, t1.id[])
                    push!(result_bounds, max(t1[]))
                    set_table(t2, s)
                    push!(result_tables, t2.id[])
                    push!(result_bounds, max(t2[]))
                else
                    set_table(table, s)
                    push!(result_tables, table.id[])
                    push!(result_bounds, max(table[]))
                end
            else 
                push!(result_tables, l.tables[i][])
                push!(result_bounds, i != length(l.tables[]) ? l.bounds[i][] : i)
            end
        end
    end
    pop!(result_bounds)
    res = malloc_and_init(Level{K, V}, 
                          s,
                          generate_id(Level, s),
                          result_tables, 
                          result_bounds,
                          l.size[] + length(e),
                          l.max_size[],
                          l.table_threshold_size[])
    res.prev_level[], res.next_level[] = l.prev_level[], l.next_level[]
    return res
end

# Returns indices
function partition(bounds::BlobVector{K}, 
                   entries::BlobVector{Entry{K, V}}) where {K, V}
    indices, i, j = Vector{Int64}(), 1, 1
    push!(indices, 0)
    while i <= length(entries) && length(indices) < length(bounds) + 1
        if entries[i].key[] > bounds[j]
            j += 1
            while j <= length(bounds) && entries[i].key[] > bounds[j]
                push!(indices, i - 1)
                j += 1
            end
            push!(indices, i - 1)
        end
        i += 1
    end
    while length(indices) < length(bounds) + 1
        push!(indices, i - 1)
    end
    push!(indices, length(entries))
    return indices
end

function key_table_index(l::Level{K, V}, k::K) where {K, V}
    (length(l.bounds) == 0 || k <= l.bounds[1]) && return 1
    k > l.bounds[length(l.bounds)] && return length(l.tables)
    for i in 1:length(l.bounds) - 1
        k > l.bounds[i] && k < l.bounds[i + 1] && return i + 1
    end
end
