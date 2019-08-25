mutable struct Store{K, V, PAGE, PAGE_HANDLE} <: AbstractStore{K, V, PAGE, PAGE_HANDLE}
    buffer::Buffer{K, V}
    data::Blob{StoreData{K, V}}
    data_page::PAGE
    inmemory::InMemoryData
    path::String
    function Store{K, V, PAGE, PAGE_HANDLE}(
        ;path::String="./db",
        fanout::Int=2,
        buffer_max_size::Int=125000, 
        table_threshold_size::Int=125000
    ) where {K, V, PAGE, PAGE_HANDLE} 
        if isdir_pagehandle(PAGE_HANDLE, path)
            error("Directory already exists")
        end
        mkpath_pagehandle(PAGE_HANDLE, path)
        buffer = Buffer{K, V}(buffer_max_size)
        data, page = malloc_and_init(StoreData{K, V}, 
                                     PAGE,
                                     fanout, 
                                     buffer_max_size, 
                                     table_threshold_size)
        inmemory = InMemoryData(path)
        new{K, V, PAGE, PAGE_HANDLE}(buffer, data, page, inmemory, path)
    end
    function Store{K, V, PAGE, PAGE_HANDLE}(
        path::String, 
        data::Blob{StoreData{K, V}},
        data_page::PAGE
    ) where {K, V, PAGE, PAGE_HANDLE}
        buffer = Buffer{K, V}(data.buffer_max_size[])
        inmemory = InMemoryData(path)
        new{K, V, PAGE, PAGE_HANDLE}(buffer, data, data_page, inmemory, path)
    end
end

function Store{K, V}(;path::String="./db", 
                     buffer_max_size::Int=125000, 
                     table_threshold_size::Int=125000) where {K, V} 
    Store{K, V, MemoryPage, FilePageHandle}(
        path=path, 
        buffer_max_size=buffer_max_size, 
        table_threshold_size=table_threshold_size
    )
end

function Store{K, V}(path::String,
                     data::Blob{StoreData{K, V}},
                     data_page::MemoryPage) where {K, V} 
    Store{K, V, MemoryPage, FilePageHandle}(path, data, data_page)
end

Base.isempty(s::StoreData{K, V}) where {K, V} = s.first_level <= 0
Base.show(io::IO, s::AbstractStore{K, V}) where {K, V} = print(io, "LSMTree.Store{$K, $V} with $(length(s)) entries")

# FIXME find a way to not count the deleted and duplicated entries
function Base.length(store::AbstractStore{K, V}) where {K, V}
    len = length(store.buffer.entries)
    l = get_level(store.data.first_level[], store)
    while l !== nothing 
        len += l.size[] 
        l = get_level(l.next_level[], store)
    end
    len
end

function buffer_dump(store::AbstractStore{K, V}) where {K, V}
    store.buffer.size <= 0 && return
    compact(store)

    first_level = get_level(store.data.first_level[], store)
    entries = to_blob(store.buffer)
    indices = partition(first_level.bounds[], entries[])
    current = merge(store, first_level, entries[], indices, true)
    store.data.first_level[] = current.id[]
    set_level(current, store)
    
    next = get_level(current.next_level[], store)
    while next !== nothing 
        next = copy(next, store)
        next.prev_level[] = current.id[] 
        current.next_level[] = next.id[]
        set_level(next, store)
        set_level(current, store)
        
        current = next
        next = get_level(next.next_level[], store)
    end
    gc(store)
    empty!(store.buffer)
end

function gc(store::AbstractStore{K, V, PAGE, PAGE_HANDLE}) where {K, V, PAGE, PAGE_HANDLE}
    table_ids = store.inmemory.table_ids_inuse
    level_ids = store.inmemory.level_ids_inuse
    store_ids = store.inmemory.store_ids_inuse
    graph = Vector{Tuple{Int64, Int64}}()
    for id in level_ids
        level = LSMTree.get_level(id, store)
        level.next_level[] > 0 && push!(graph, (id, level.next_level[]))
    end
    # Mark
    levels = Vector{Int64}()
    tables = Vector{Int64}()
    nodes = in(store.data.first_level[], store_ids) ? store_ids : push!(store_ids, store.data.first_level[])
    while !isempty(nodes)
        n = pop!(nodes)
        push!(levels, n)
        for edge in filter(x -> first(x) == n, graph)
            pushfirst!(nodes, last(edge))
        end
    end
    # Sweep
    for i in levels
        l = LSMTree.get_level(i, store)[]
        for j in l.tables 
            push!(tables, j)
            deleteat!(table_ids, table_ids .== j)
            # TODO remove the deleted table form memory (maybe?)
        end
        deleteat!(level_ids, level_ids .== i)
    end
    store.inmemory.level_ids_inuse = levels
    store.inmemory.table_ids_inuse = tables
    for deleted_id in table_ids
        # deleted_id_page = store.inmemory.table_pages[deleted_id]
        # deleteat!(store.inmemory.tables_queue, 
        #           store.inmemory.tables_queue .= deleted_id)
        # delete!(store.inmemory.tables, deleted_id)
        # delete!(store.inmemory.table_pages, deleted_id)
        # free_page(deleted_id_page)
        delete_pagehandle(PAGE_HANDLE, "$(store.path)/$deleted_id.tbl", force=true)
    end
    for deleted_id in level_ids
        deleted_id_page = store.inmemory.level_pages[deleted_id]
        delete!(store.inmemory.levels, deleted_id)
        delete!(store.inmemory.level_pages, deleted_id)
        free_page(deleted_id_page)
        delete_pagehandle(PAGE_HANDLE, "$(store.path)/$deleted_id.lvl", force=true)
    end
end

function compact(store::AbstractStore{K, V}) where {K, V}
    # Return if first level has enough empty space
    !isempty(store.data[]) && !isfull(get_level(store.data.first_level[], store)[]) && return
    # Find first level that has enough empty space
    current = get_level(store.data.first_level[], store)
    next = current !== nothing ? get_level(current.next_level[], store) : nothing
    force_remove = false
    while next !== nothing && !islast(next[])
        if !isfull(next[])
            force_remove = islast(next[])
            break
        end
        current = get_level(current.next_level[], store) 
        next = get_level(next.next_level[], store)
    end
    # Create and return new level if tree has no level
    if current === nothing
        new_level = malloc_and_init(Level{K, V}, 
                                    store,
                                    generate_id(Level, store),
                                    Vector{Int64}(),
                                    Vector{V}(), 
                                    0, 
                                    store.buffer.max_size * store.data.fanout[], 
                                    store.data.table_threshold_size[])
        set_level(new_level, store)
        store.data.first_level[] = new_level.id[]
        return
    end
    # Create new level if we didn't find enough space in tree
    if next === nothing || isfull(next[])
        last_level = next === nothing ? copy(current, store) : copy(next, store)
        new_level = malloc_and_init(Level{K, V},
                                    store,
                                    generate_id(Level, store),
                                    Vector{Int64}(),
                                    Vector{V}(), 0,
                                    last_level.size[] * store.data.fanout[], 
                                    store.data.table_threshold_size[])
        new_level.prev_level[] = last_level.id[]
        last_level.next_level[] = new_level.id[]
        set_level(last_level, store)
        set_level(new_level, store)
        current, next = last_level, new_level
        force_remove = true
    end
    # Compact levels and free up space in first level
    after_next = get_level(next.next_level[], store)
    while !isfirst(next[]) 
        for table in current.tables[]
            next = compact(store, next, get_table(table, store), force_remove)
            set_level(next, store)
        end
        if after_next !== nothing 
            next.next_level[] = after_next.id[]
            after_next.prev_level[] = next.id[]
            set_level(after_next, store)
        end
        current_isfirst = isfirst(current[])
        current = empty(current, store)
        current.next_level[] = next.id[]
        next.prev_level[] = current.id[]
        if current_isfirst store.data.first_level[] = current.id[] end
        set_level(next, store)
        set_level(current, store)
        after_next = next
        next = current
        current = get_level(current.prev_level[], store)
        force_remove = false
    end
end

function snapshot(s::AbstractStore{K, V, <:Any, <:Any}) where {K, V}
    buffer_dump(s)
    # The id of first level is always unique
    # Therefore we also use it as the store id
    path = "$(s.path)/$(s.data.first_level[]).str"
    file = open_pagehandle(FilePageHandle, path, truncate=true, read=true)
    write_pagehandle(file, s.data_page, getfield(s.data, :limit))
    close_pagehandle(file)
    push!(s.inmemory.store_ids_inuse, s.data.first_level[])
    snapshot = Blobs.malloc_and_init(StoreData{K, V}, 
                                     s.data.fanout[], 
                                     s.data.buffer_max_size[], 
                                     s.data.table_threshold_size[])
    snapshot.first_level[] = s.data.first_level[]
    snapshot
end

function remove_snapshot(s::AbstractStore)
    path = "$(s.path)/$(s.data.first_level[]).str"
    delete_pagehandle(FilePageHandle, path, force=true)
    stores = s.inmemory.store_ids_inuse
    deleteat!(stores, stores .== s.data.first_level[])
    gc(s)
end
