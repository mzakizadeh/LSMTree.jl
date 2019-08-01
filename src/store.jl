struct StoreData{K, V}
    first_level::Int64
    fanout::Int64
    first_level_max_size::Int64
    table_threshold_size::Int64
end

function Blobs.child_size(::Type{StoreData{K, V}}, 
                          fanout::Int64, 
                          first_level_max_size::Int64, 
                          table_threshold_size::Int64) where {K, V}
    +(0)
end

function Blobs.init(bf::Blob{StoreData{K, V}}, 
                    free::Blob{Nothing},
                    fanout::Int64, 
                    first_level_max_size::Int64, 
                    table_threshold_size::Int64) where {K, V}
    bf.first_level[] = -1
    bf.fanout[] = fanout
    bf.first_level_max_size[] = first_level_max_size
    bf.table_threshold_size[] = table_threshold_size
    free
end

mutable struct Store{K, V}
    buffer::Buffer{K, V}
    data::Blob{StoreData{K, V}}
    inmemory::InMemoryData
    function Store{K, V}(path::String="./db",
                         buffer_max_size::Integer=125000, 
                         table_threshold_size::Integer=125000) where {K, V} 
        @assert !isdir(path) "Directory already exists! Try using restore function."
        mkpath(path)
        data = Blobs.malloc_and_init(StoreData{K, V}, 2, 
                                     buffer_max_size * 2, 
                                     table_threshold_size)
        new{K, V}(Buffer{K, V}(buffer_max_size), 
                  data,
                  InMemoryData(path))
    end
    Store{K, V}(path::String, data::Blob{StoreData{K, V}}) where {K, V} =
        new{K, V}(Buffer{K, V}(floor(Int64, data.first_level_max_size[] / 2)), 
                  data,
                  InMemoryData(path))
end

Base.isempty(s::StoreData{K, V}) where {K, V} = s.first_level <= 0
Base.show(io::IO, s::Store{K, V}) where {K, V} = print(io, "LSMTree.Store{$K, $V} with $(length(s)) entries")

function Base.length(s::Store{K, V}) where {K, V}
    len = length(s.buffer.entries)
    l = get_level(Level{K, V}, s.data.first_level[], s.inmemory)
    while !isnothing(l) 
        len += l.size[] 
        l = get_level(Level{K, V}, l.next_level[], s.inmemory)
    end
    len
end

function Base.get(s::Store{K, V}, key) where {K, V}
    key = convert(K, key) 
    result = get(s.buffer, key)
    if !isnothing(result) 
        result.deleted && return nothing
        return result.val
    end
    l = get_level(Level{K, V}, s.data.first_level[], s.inmemory)
    while !isnothing(l)
        result = get(l[], key, s.inmemory)
        if !isnothing(result)
            result.deleted && return nothing
            return result.val
        end
        l = get_level(Level{K, V}, l.next_level[], s.inmemory)
    end
    nothing
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
    while !isnothing(next) 
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

function Base.put!(s::Store{K, V}, key, val, deleted=false) where {K, V}
    key = convert(K, key)
    val = convert(V, val)
    put!(s.buffer, key, val, deleted)
    isfull(s.buffer) && buffer_dump(s)
end

Base.setindex!(s::Store{K, V}, val, key) where {K, V} = put!(s, key, val)
Base.getindex(s::Store{K, V}, key) where {K, V} = get(s, key)

# TODO delete key without getting the value
function Base.delete!(s::Store, key)
    val = get(s, key)
    put!(s, key, val, true)
end

function Base.delete!(s::Store)
    rm(s.inmemory.path, recursive=true, force=true)
end

function Base.close(s::Store{K, V}) where {K, V}
    buffer_dump(s)
    # The id of first level is always unique
    # Therefore we also used it as store id
    open("$(s.inmemory.path)/$(s.data.first_level[]).str", "w+") do file
        unsafe_write(file, pointer(s.data), getfield(s.data, :limit))
    end
    print("LSMTree.Store{$K, $V} with id $(s.data.first_level[]) closed")
    Blobs.free(s.data)
end

function gc(s::Store{K, V}) where {K, V}
    only_levels_pattern = x -> occursin(r"([0-9])+(.lvl)$", x)
    only_stores_pattern = x -> occursin(r"([0-9])+(.str)$", x)
    level_files = filter(only_levels_pattern, readdir(s.inmemory.path))
    level_ids = sort(map(x -> parse(Int64, replace(x, ".lvl" => "")), 
                         level_files))
    store_files = filter(only_stores_pattern, readdir(s.inmemory.path))
    store_ids = sort(map(x -> parse(Int64, replace(x, ".str" => "")), 
                         store_files))
    graph = Vector{Tuple{Int64, Int64}}()
    for id in level_ids
        level = LSMTree.get_level(LSMTree.Level{K, V}, id, s.inmemory)
        level.next_level[] > 0 && push!(graph, (id, level.next_level[]))
    end
    # Mark
    levels = Vector{Int64}()
    nodes = push!(store_ids, s.data.first_level[])
    while !isempty(nodes)
        n = pop!(nodes)
        push!(levels, n)
        for edge in filter(x -> first(x) == n, graph)
            pushfirst!(nodes, last(edge))
        end
    end
    # Sweep
    files = filter(!only_stores_pattern, readdir(s.inmemory.path))
    for i in levels
        l = LSMTree.get_level(LSMTree.Level{K, V}, i, s.inmemory)[]
        for j in l.tables 
            filter!(x -> x != "$j.tbl", files)
            # delete!(inmemory_tables, j)
        end
        filter!(x -> x != "$i.lvl", files)
        delete!(s.inmemory.inmemory_levels, i)
    end
    for f in files rm("$(s.inmemory.path)/$f", force=true) end
end

function compact(s::Store{K, V}) where {K, V}
    # Return if first level has enough empty space
    !isempty(s.data[]) && !isfull(get_level(Level{K, V}, s.data.first_level[], s.inmemory)[]) && return
    # Find first level that has enough empty space
    current = get_level(Level{K, V}, s.data.first_level[], s.inmemory)
    next = !isnothing(current) ? 
                    get_level(Level{K, V}, current.next_level[], s.inmemory) : nothing
    force_remove = false
    while !isnothing(next) && !islast(next[])
        if !isfull(next[])
            force_remove = islast(next[])
            break
        end
        current = get_level(Level{K, V}, current.next_level[], s.inmemory) 
        next = get_level(Level{K, V}, next.next_level[], s.inmemory)
    end
    # Create and return new level if tree has no level
    if isnothing(current)
        new_level = Blobs.malloc_and_init(Level{K, V}, 
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
    if isnothing(next) || isfull(next[])
        last_level = isnothing(next) ? copy(current, s.inmemory) : copy(next, s.inmemory)
        new_level = Blobs.malloc_and_init(Level{K, V},
                                          generate_id(Level, s.inmemory),
                                          Vector{Int64}(),
                                          Vector{V}(), 0,
                                          last_level.size[] * s.data.fanout[], 
                                          s.data.table_threshold_size[])
        new_level.id[] += 1
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
        end
        if !isnothing(after_next) 
            next.next_level[] = after_next.id[]
            after_next.prev_level[] = next.id[]
            set_level(after_next, s.inmemory)
        end
        current_isfirst = isfirst(current[])
        current = empty(current, s.inmemory)
        # TODO problem with generating new id
        current.id[] += 1
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
    # Therefore we also used it as store id
    open("$(s.inmemory.path)/$(s.data.first_level[]).str", "w+") do file
        unsafe_write(file, pointer(s.data), getfield(s.data, :limit))
    end
    snapshot = Blobs.malloc_and_init(StoreData{K, V}, s.data.fanout[], 
                                     s.data.first_level_max_size[], 
                                     s.data.table_threshold_size[])
    snapshot.first_level[] = s.data.first_level[]
    snapshot
end

function restore(::Type{K}, ::Type{V}, path::String, id::Int64) where {K, V}
    file = "$path/$id.str"
    if isfile(file)
        s = missing
        open(file) do f
            size = filesize(f)
            p = Libc.malloc(size)
            b = Blob{StoreData{K, V}}(p, 0, size) 
            unsafe_read(f, p, size)
            s = Store{K, V}(path, b)
        end
        return s
    end
    nothing
end

struct Iterator{K, V}
    levels::Vector{Level{K, V}}
    store::Store{K, V}
    function Iterator(s::Store{K, V}) where {K, V}
        snapshot = LSMTree.snapshot(s)
        levels = Vector{Level{K, V}}()
        l = get_level(Level{K, V}, snapshot.first_level[], s.inmemory)
        while !isnothing(l)
            push!(levels, l[])
            l = get_level(Level{K, V}, l.next_level[], s.inmemory)
        end
        new{K, V}(levels, s)
    end
end

mutable struct LevelIterationState{K, V}
    current_entry::Entry{K, V}
    current_table_index::Int64
    current_entry_index::Int64
    done::Bool
end

mutable struct IterationState{K, V}
    current_levels_state::Vector{LevelIterationState{K, V}}
end

function next_state(s::LevelIterationState{K, V}, l::Level{K, V}, inmemory::InMemoryData) where {K, V}
    s.current_entry_index += 1
    t = get_table(Table{K, V}, l.tables[s.current_table_index], inmemory)[]
    if s.current_entry_index > t.size
        s.current_entry_index = 1
        s.current_table_index += 1
        if s.current_table_index > length(l.tables)
            s.done = true
        end
    end
    s.current_entry = t.entries[s.current_entry_index]
end

function iter_init(iter::Iterator{K, V}) where {K, V}
    levels_state = Vector{LevelIterationState{K, V}}()
    min_index = 0
    min = nothing
    for i in 1:length(iter.levels)
        table_index = 1
        t = get_table(Table{K, V}, iter.levels[i].tables[table_index], iter.store.inmemory)[]
        entry_index = 1
        entry = t.entries[entry_index]
        if isnothing(min) || entry < min 
            min = entry
            min_index = i
        end
        push!(levels_state, LevelIterationState(t.entries[entry_index],
                                                table_index,
                                                entry_index,
                                                length(iter.levels[i].tables) == 0))
    end
    next_state(levels_state[min_index], iter.levels[min_index], iter.store.inmemory) 
    return (min, IterationState{K, V}(levels_state))
end

function iter_next(iter::Iterator, state)
    e, state = state
    next = nothing
    next_index = 0
    for i in 1:length(iter.levels)
        if !state.current_levels_state[i].done && (isnothing(next) || state.current_levels_state[i].current_entry < next)
            next = state.current_levels_state[i].current_entry
            next_index = i
        end
    end
    next_state(state.current_levels_state[next_index], iter.levels[next_index], iter.store.inmemory) 
    return (e, (next, state))
end

# TODO bug fix first element is wrong
function seek_lub_search(iter::Iterator{K, V}, state::IterationState, key) where {K, V}
    key = convert(K, key)
    min_index = 0
    min = nothing
    for i in 1:length(iter.levels)
        table_index = key_table_index(iter.levels[i], key)
        table = get_table(Table{K, V}, iter.levels[i].tables[table_index], iter.store.inmemory)
        entry_index = lub(table.entries[], 1, table.size[], key) + 1
        if entry_index > table.size[] 
            state.current_levels_state[i].done = true
        else 
            state.current_levels_state[i].current_entry = table.entries[entry_index][]
        end
        state.current_levels_state[i].current_table_index = table_index
        state.current_levels_state[i].current_entry_index = entry_index
        if isnothing(min) || state.current_levels_state[i].current_entry < min 
            min = state.current_levels_state[i].current_entry
            min_index = i
        end
    end
    next_state(state.current_levels_state[min_index], iter.levels[min_index], iter.store.inmemory) 
    return (min, state)
end

# TODO bug fix last entry does not print
function iter_done(iter::Iterator, state)
    _, iter_state = state
    level_states = iter_state.current_levels_state
    for i in 1:length(level_states)
        !level_states[i].done && return false
    end
    # TODO remove snapshot and call gc
    true
end