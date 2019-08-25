struct Table{K, V}
    id::Int64
    size::Int64
    entries::BlobVector{Entry{K, V}}
end

Base.length(t::Table) = t.size[]
Base.min(t::Table) = t.entries[1].key[]
Base.max(t::Table) = t.entries[t.size].key[]

function generate_id(::Type{Table}, 
                     s::AbstractStore{<:Any, <:Any, PAGE, <:Any}) where PAGE
    meta, page = load_meta(s)
    id = meta.next_table_id[]
    meta.next_table_id[] += 1
    save_meta(meta, page, s)
    free_page(page)
    return id
end

function Blobs.child_size(::Type{Table{K, V}}, 
                          id::Int64,
                          entries::Vector{Entry{K, V}}) where {K, V}
    T = Table{K, V}
    Blobs.child_size(fieldtype(T, :entries), length(entries))
end

function Blobs.init(blob::Blob{Table{K, V}}, 
                    free::Blob{Nothing}, 
                    id::Int64,
                    entries::Vector{Entry{K, V}}) where {K, V}
    free = Blobs.init(blob.entries, free, length(entries))
    for i in 1:length(entries)
        blob.entries[i].key[] = entries[i].key
        blob.entries[i].val[] = entries[i].val
        blob.entries[i].deleted[] = entries[i].deleted
    end
    blob.id[] = id
    blob.size[] = length(entries)
    free
end

function malloc_and_init(::Type{Table{K, V}}, 
                         s::AbstractStore{K, V, PAGE, <:Any}, 
                         args...)::Blob{Table{K, V}} where {K, V, PAGE}
    T = Table{K, V}
    size = Blobs.self_size(Table{K, V}) + Blobs.child_size(Table{K, V}, args...)
    page = malloc_page(PAGE, size)
    
    id = args[1]
    s.inmemory.table_pages[id] = page
    push!(s.inmemory.table_ids_inuse, id)
    
    blob = Blob{Table{K, V}}(pointer(page), 0, size)
    used = Blobs.init(blob, args...)
    @assert used - blob == size

    blob
end

function get_table(id::Int64, 
                   s::AbstractStore{K, V, PAGE, PAGE_HANDLE}) where {K, V, PAGE, PAGE_HANDLE}
    # Returns nothing if id is not valid
    id <= 0 && return nothing
    # Save ids to check in `gc`
    !in(id, s.inmemory.table_ids_inuse) && push!(s.inmemory.table_ids_inuse, id)
    # Returns the level if it's already loaded in memory
    if in(id, s.inmemory.tables_queue)
       index = findfirst(x -> x == id, s.inmemory.tables_queue) 
       deleteat!(s.inmemory.tables_queue, index)
       pushfirst!(s.inmemory.tables_queue, id)
       return s.inmemory.tables[id]
    end
    # Else load the level to the memory 
    path = "$(s.path)/$id.tbl"
    if isfile_pagehandle(PAGE_HANDLE, path)
        f = open_pagehandle(PAGE_HANDLE, path)
        size = size_pagehandle(f)
        page = malloc_page(PAGE, size)
        b = Blob{Table{K, V}}(pointer(page), 0, size) 
        read_pagehandle(f, page, size)
        s.inmemory.table_pages[id] = page
        s.inmemory.tables[b.id[]] = b
        close_pagehandle(f)
        pushfirst!(s.inmemory.tables_queue, id)
        if length(s.inmemory.tables_queue) > 5
            deleted_id = pop!(s.inmemory.tables_queue)
            deleted_id_page = s.inmemory.table_pages[deleted_id]
            delete!(s.inmemory.tables, deleted_id)
            delete!(s.inmemory.table_pages, deleted_id)
            free_page(deleted_id_page)
        end
        return s.inmemory.tables[id]
    end
    error("Table does not exist! (path=$path)")
end

function set_table(t::Blob{Table{K, V}}, 
                   s::AbstractStore{K, V, <:Any, PAGE_HANDLE}) where {K, V, PAGE_HANDLE}
    id = t.id[]
    path = "$(s.path)/$id.tbl"
    file = open_pagehandle(PAGE_HANDLE, path, truncate=true, read=true)
    write_pagehandle(file, s.inmemory.table_pages[id], getfield(t, :limit))
    close_pagehandle(file)
end

function Base.get(t::Table{K, V}, key::K) where {K, V} 
    i = bsearch(t.entries, 1, length(t), key)
    if i > 0
        result = t.entries[i]
        return result
    end
    nothing
end

function Base.merge(store::AbstractStore{K, V, <:Any, <:Any},
                    table::Blob{Table{K, V}},
                    entries::BlobVector{Entry{K, V}},
                    start_index::Int64,
                    end_index::Int64,
                    force_remove) where {K, V}
    result_entries = Vector{Entry{K, V}}()
    i, j, size = 1, start_index + 1, 0
    while i <= length(table[]) && j <= end_index
        if isequal(table.entries[i][], entries[j])
            if !force_remove || !entries[j].deleted
                push!(result_entries, entries[j])
                size += 1
            end
            i += 1
            j += 1
        elseif table.entries[i][] < entries[j]
            if !force_remove || !table.entries[i][].deleted 
                push!(result_entries, table.entries[i][])
                size += 1
            end
            i += 1
        else
            if !force_remove || !entries[j].deleted
                push!(result_entries, entries[j])
                size += 1
            end
            j += 1
        end
    end
    while i <= length(table[])
        if !force_remove || !table.entries[i][].deleted 
            push!(result_entries, table.entries[i][])
            size += 1
        end
        i += 1
    end
    while j <= end_index
        if !force_remove || !entries[j].deleted
            push!(result_entries, entries[j])
            size += 1
        end
        j += 1
    end
    return malloc_and_init(Table{K, V}, 
                           store,
                           generate_id(Table, store),
                           result_entries)
end

function split(t::Blob{Table{K, V}}, s::AbstractStore{K, V, <:Any, <:Any}) where {K, V} 
    mid = floor(Int, t.size[] / 2)
    t1_entries = Vector{Entry{K, V}}()
    t2_entries = Vector{Entry{K, V}}()
    for i in 1:mid 
        push!(t1_entries, t.entries[i][]) 
    end
    for i in mid + 1:t.size[]
        push!(t2_entries, t.entries[i][]) 
    end
    t1 = malloc_and_init(Table{K, V}, s, generate_id(Table, s), t1_entries)
    t2 = malloc_and_init(Table{K, V}, s, generate_id(Table, s), t2_entries)
    return (t1, t2)
end
