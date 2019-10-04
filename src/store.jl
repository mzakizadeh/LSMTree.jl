mutable struct Store{K, V, PAGE, PAGE_HANDLE} <: AbstractStore{K, V, PAGE, PAGE_HANDLE}
    buffer::Buffer{K, V}
    data::Blob{StoreData{K, V}}
    data_page::PAGE
    inmemory::InMemoryData
    meta::MetaData{K}
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
        meta = MetaData{K}()
        new{K, V, PAGE, PAGE_HANDLE}(buffer, data, page, inmemory, meta, path)
    end
    function Store{K, V, PAGE, PAGE_HANDLE}(
        path::String, 
        data::Blob{StoreData{K, V}},
        data_page::PAGE
    ) where {K, V, PAGE, PAGE_HANDLE}
        buffer = Buffer{K, V}(data.buffer_max_size[])
        inmemory = InMemoryData(path)
        meta = load_meta(K, PAGE, PAGE_HANDLE, path)
        new{K, V, PAGE, PAGE_HANDLE}(buffer, data, data_page, inmemory, meta, path)
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

function buffer_dump(store::AbstractStore{K, V, PAGE, <:Any}) where {K, V, PAGE}
    store.buffer.size <= 0 && return
    compact(store)

    first_level = get_level(store.data.first_level[], store)[]
    entries, page = to_blob(PAGE, store.buffer)
    indices = partition(first_level.bounds, entries[])
    current = merge(store, first_level, entries[], indices, true)
    free_page(page)
    store.data.first_level[] = current.id[]
    set_level(current, store)
    
    next = get_level(current.next_level[], store)
    while next !== nothing 
        next = copy(next[], store)
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
    data = store.data[]
    first_level = nothing
    # Return if first level has enough empty space
    if !isempty(data) 
        first_level = get_level(data.first_level, store)
        !isfull(first_level[]) && return
    end
    # Find first level that has enough empty space
    blob_current = first_level
    current = blob_current !== nothing ? blob_current[] : nothing
    blob_next = blob_current !== nothing ? get_level(current.next_level, store) : nothing
    next = blob_next !== nothing ? blob_next[] : nothing

    force_remove = false

    while blob_next !== nothing && !islast(next)
        if !isfull(next)
            force_remove = islast(next)
            break
        end
        current = get_level(blob_current.next_level[], store) 
        blob_next = get_level(blob_next.next_level[], store)
        next = blob_next !== nothing ? blob_next[] : nothing
    end
    # Create and return new level if tree has no level
    if blob_current === nothing
        new_id = generate_id(Level, store)
        new_size = data.buffer_max_size * data.fanout
        new_level = malloc_and_init(Level{K, V}, 
                                    store,
                                    new_id,
                                    Vector{Int64}(),
                                    Vector{V}(), 
                                    0, 
                                    new_size, 
                                    store.data.table_threshold_size[])
        store.meta.levels_bf[new_id] = BloomFilter(new_size, 0.001)
        set_level(new_level, store)
        store.data.first_level[] = new_id
        return
    end
    # Create new level if we didn't find enough space in tree
    if blob_next === nothing || isfull(next)
        blob_last_level = blob_next === nothing ? copy(current, store) : copy(next, store)
        last_level = blob_last_level[]

        new_id = generate_id(Level, store)
        new_size = blob_last_level.size[] * store.data.fanout[]
        new_level = malloc_and_init(Level{K, V},
                                    store,
                                    new_id,
                                    Vector{Int64}(),
                                    Vector{V}(), 0,
                                    new_size, 
                                    store.data.table_threshold_size[])
        store.meta.levels_bf[new_id] = BloomFilter(new_size, 0.001)
        new_level.prev_level[] = blob_last_level.id[]
        blob_last_level.next_level[] = new_level.id[]
        set_level(blob_last_level, store)
        set_level(new_level, store)
        blob_current, blob_next = blob_last_level, new_level
        next = blob_next[]
        force_remove = true
    end
    # Compact levels and free up space in first level
    blob_after_next = get_level(next.next_level[], store)
    after_next = blob_after_next !== nothing ? blob_after_next[] : nothing 
    while !isfirst(next) 
        for table in current.tables
            blob_next = compact(store, next, get_table(table, store)[], force_remove)
            next = blob_next[]
            set_level(blob_next, store)
        end
        if blob_after_next !== nothing 
            blob_next.next_level[] = after_next.id
            next = blob_next[]
            blob_after_next.prev_level[] = next.id
            set_level(blob_after_next, store)
        end
        current_isfirst = isfirst(current)
        blob_current = empty(current, store)
        blob_current.next_level[] = next.id
        current = blob_current[]
        blob_next.prev_level[] = current.id
        if current_isfirst store.data.first_level[] = current.id end
        set_level(blob_next, store)
        set_level(blob_current, store)
        blob_after_next = blob_next
        after_next = next
        blob_next = blob_current
        next = current
        blob_current = get_level(current.prev_level[], store)
        current = blob_current !== nothing ? blob_current[] : nothing 
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
