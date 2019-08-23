mutable struct Store{K, V, PAGE, PAGE_HANDLE}
    buffer::Buffer{K, V}
    data::Blob{StoreData{K, V}}
    inmemory::InMemoryData{PAGE, PAGE_HANDLE}
    function Store{K, V, PAGE, PAGE_HANDLE}(
        path::String="./db",
        buffer_max_size::Int=125000, 
        table_threshold_size::Int=125000
    ) where {K, V, PAGE, PAGE_HANDLE} 
        @error !isdir_pagehandle(PAGE_HANDLE, path) "Directory already exists! Try using restore function."
        mkpath_pagehandle(PAGE_HANDLE, path)
        data = Blobs.malloc_and_init(StoreData{K, V}, 
                                     2, buffer_max_size * 2, 
                                     table_threshold_size)
        new{K, V, PAGE, PAGE_HANDLE}(Buffer{K, V}(buffer_max_size), 
                                     data,
                                     InMemoryData{PAGE, PAGE_HANDLE}(path))
    end
    function Store{K, V, PAGE, PAGE_HANDLE}(
        path::String, 
        data::Blob{StoreData{K, V}}
    ) where {K, V, PAGE, PAGE_HANDLE}
        buffer_max_size = floor(Int64, data.first_level_max_size[] / 2)
        new{K, V, PAGE, PAGE_HANDLE}(
            Buffer{K, V}(buffer_max_size), 
            data,
            InMemoryData{PAGE, PAGE_HANDLE}(path)
        )
    end
end

function Store{K, V}(path::String="./db", 
                     buffer_max_size::Int=125000, 
                     table_threshold_size::Int=125000) where {K, V} 
    Store{K, V, MemoryPage, FilePageHandle}(path, 
                                            buffer_max_size, 
                                            table_threshold_size)
end

function Store{K, V}(path::String,
                     data::Blob{StoreData{K, V}}) where {K, V} 
    Store{K, V, MemoryPage, FilePageHandle}(path, data)
end

Base.isempty(s::StoreData{K, V}) where {K, V} = s.first_level <= 0
Base.show(io::IO, s::Store{K, V}) where {K, V} = print(io, "LSMTree.Store{$K, $V} with $(length(s)) entries")

function Base.length(s::Store{K, V}) where {K, V}
    len = length(s.buffer.entries)
    l = get_level(Level{K, V}, s.data.first_level[], s.inmemory)
    while l !== nothing 
        len += l.size[] 
        l = get_level(Level{K, V}, l.next_level[], s.inmemory)
    end
    len
end

function buffer_dump(s::Store{K, V}) where {K, V}
    s.buffer.size <= 0 && return
    compact(s)

    first_level = get_level(Level{K, V}, s.data.first_level[], s.inmemory)
    entries = to_blob(s.buffer)
    indices = partition(first_level.bounds[], entries[])
    l = merge(s.inmemory, first_level, entries[], indices, true)
    set_level(l, s.inmemory)

    current = l
    next = get_level(Level{K, V}, l.next_level[], s.inmemory)
    while next !== nothing 
        next = copy(next, s.inmemory)
        next.prev_level[] = current.id[] 
        current.next_level[] = next.id[]

        set_level(next, s.inmemory)
        set_level(current, s.inmemory)
        current = next
        next = get_level(Level{K, V}, next.next_level[], s.inmemory)
    end

    s.data.first_level[] = l.id[]
    gc(s)
    empty!(s.buffer)
end

function Base.delete!(s::Store)
    rm(s.inmemory.path, recursive=true, force=true)
end

function gc(s::Store{K, V}) where {K, V}
    table_ids = s.inmemory.tables_inuse
    level_ids = s.inmemory.levels_inuse
    store_ids = s.inmemory.stores_inuse
    graph = Vector{Tuple{Int64, Int64}}()
    for id in level_ids
        level = LSMTree.get_level(LSMTree.Level{K, V}, id, s.inmemory)
        level.next_level[] > 0 && push!(graph, (id, level.next_level[]))
    end
    # Mark
    levels = Vector{Int64}()
    tables = Vector{Int64}()
    nodes = in(s.data.first_level[], store_ids) ? store_ids : push!(store_ids, s.data.first_level[])
    while !isempty(nodes)
        n = pop!(nodes)
        push!(levels, n)
        for edge in filter(x -> first(x) == n, graph)
            pushfirst!(nodes, last(edge))
        end
    end
    # Sweep
    for i in levels
        l = LSMTree.get_level(LSMTree.Level{K, V}, i, s.inmemory)[]
        for j in l.tables 
            push!(tables, j)
            deleteat!(table_ids, table_ids .== j)
            # delete!(inmemory_tables, j)
        end
        deleteat!(level_ids, level_ids .== i)
        delete!(s.inmemory.inmemory_levels, i)
    end
    s.inmemory.levels_inuse = levels
    s.inmemory.tables_inuse = tables
    table_files = map(id -> "$id.tbl", table_ids)
    level_files = map(id -> "$id.lvl", level_ids)
    files = vcat(table_files, level_files)
    for f in files rm("$(s.inmemory.path)/$f", force=true) end
end

function compact(s::Store{K, V}) where {K, V}
    # Return if first level has enough empty space
    !isempty(s.data[]) && !isfull(get_level(Level{K, V}, s.data.first_level[], s.inmemory)[]) && return
    # Find first level that has enough empty space
    current = get_level(Level{K, V}, s.data.first_level[], s.inmemory)
    next = current !== nothing ? 
                    get_level(Level{K, V}, current.next_level[], s.inmemory) : nothing
    force_remove = false
    while next !== nothing && !islast(next[])
        if !isfull(next[])
            force_remove = islast(next[])
            break
        end
        current = get_level(Level{K, V}, current.next_level[], s.inmemory) 
        next = get_level(Level{K, V}, next.next_level[], s.inmemory)
    end
    # Create and return new level if tree has no level
    if current === nothing
        new_level = malloc_and_init(Level{K, V}, 
                                    s.inmemory,
                                    generate_id(Level, s.inmemory),
                                    Vector{Int64}(),
                                    Vector{V}(), 
                                    0, 
                                    s.buffer.max_size * s.data.fanout[], 
                                    s.data.table_threshold_size[])
        set_level(new_level, s.inmemory)
        s.data.first_level[] = new_level.id[]
        return
    end
    # Create new level if we didn't find enough space in tree
    if next === nothing || isfull(next[])
        last_level = next === nothing ? copy(current, s.inmemory) : copy(next, s.inmemory)
        new_level = malloc_and_init(Level{K, V},
                                    s.inmemory,
                                    generate_id(Level, s.inmemory),
                                    Vector{Int64}(),
                                    Vector{V}(), 0,
                                    last_level.size[] * s.data.fanout[], 
                                    s.data.table_threshold_size[])
        new_level.prev_level[] = last_level.id[]
        last_level.next_level[] = new_level.id[]
        set_level(last_level, s.inmemory)
        set_level(new_level, s.inmemory)
        current, next = last_level, new_level
        force_remove = true
    end
    # Compact levels and free up space in first level
    after_next = get_level(Level{K, V}, next.next_level[], s.inmemory)
    while !isfirst(next[]) 
        for table in current.tables[]
            next = compact(s.inmemory, next, get_table(Table{K, V}, table, s.inmemory), force_remove)
            set_level(next, s.inmemory)
        end
        if after_next !== nothing 
            next.next_level[] = after_next.id[]
            after_next.prev_level[] = next.id[]
            set_level(after_next, s.inmemory)
        end
        current_isfirst = isfirst(current[])
        current = empty(current, s.inmemory)
        current.next_level[] = next.id[]
        next.prev_level[] = current.id[]
        if current_isfirst s.data.first_level[] = current.id[] end
        set_level(next, s.inmemory)
        set_level(current, s.inmemory)
        after_next = next
        next = current
        current = get_level(Level{K, V}, current.prev_level[], s.inmemory)
        force_remove = false
    end
end

function snapshot(s::Store{K, V}) where {K, V}
    buffer_dump(s)
    # The id of first level is always unique
    # Therefore we also use it as the store id
    path = "$(s.inmemory.path)/$(s.data.first_level[]).str"
    file = open_pagehandle(FilePageHandle, path, truncate=true, read=true)
    unsafe_write(file, pointer(s.data), getfield(s.data, :limit))
    close_pagehandle(file)
    push!(s.inmemory.stores_inuse, s.data.first_level[])
    snapshot = Blobs.malloc_and_init(StoreData{K, V}, s.data.fanout[], 
                                     s.data.first_level_max_size[], 
                                     s.data.table_threshold_size[])
    snapshot.first_level[] = s.data.first_level[]
    snapshot
end

function remove_snapshot(s::Store)
    path = "$(s.inmemory.path)/$(s.data.first_level[]).str"
    delete_pagehandle(FilePageHandle, path, force=true)
    stores = s.inmemory.stores_inuse
    deleteat!(stores, stores .== s.data.first_level[])
    gc(s)
end
