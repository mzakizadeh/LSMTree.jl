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
    function Store{K, V}(buffer_max_size::Integer=120000, 
                         table_threshold_size = 120000) where {K, V} 
        # TODO get path as an input
        mkpath("blobs")
        data = Blobs.malloc_and_init(StoreData{K, V}, 2, 
                                     buffer_max_size * 2, 
                                     table_threshold_size)
        new{K, V}(Buffer{K, V}(buffer_max_size), data)
    end
end

isempty(s::StoreData{K, V}) where {K, V} = s.first_level <= 0

function Base.length(s::Store{K, V}) where {K, V}
    len = length(s.buffer.entries)
    l = get_level(Level{K, V}, s.data.first_level[])
    while !isnothing(l) 
        len += l.size[] 
        l = get_level(Level{K, V}, l.next_level[])
    end
    return len
end

function levels_count(s::Store{K, V}) where {K, V}
    len = 0
    l = get_level(Level{K, V}, s.data.first_level[])
    while !isnothing(l) 
        len += 1 
        l = get_level(Level{K, V}, l.next_level[])
    end
    return len
end

function Base.get(s::Store{K, V}, key) where {K, V}
    key = convert(K, key) 
    result = get(s.buffer, key)
    if !isnothing(result) 
        result.deleted && return nothing
        return result.val
    end
    l = get_level(Level{K, V}, s.data.first_level[])
    while !isnothing(l)
        result = get(l[], key)
        if !isnothing(result)
            result.deleted && return nothing
            return result.val
        end
        l = get_level(Level{K, V}, l.next_level[])
    end
    nothing
end

function Base.put!(s::Store{K, V}, key, val, deleted=false) where {K, V}
    key = convert(K, key)
    val = convert(V, val)
    put!(s.buffer, key, val, deleted)
    if isfull(s.buffer)
        compact(s)
        first_level = get_level(Level{K, V}, s.data.first_level[])
        entries = to_blob(s.buffer)
        indecies = partition(first_level.bounds[], entries[])
        l = merge(first_level, entries[], indecies, true)
        set_level(l)
        s.data.first_level[] = l.id[]
        empty!(s.buffer)
    end
end

Base.setindex!(s::Store{K, V}, val::V, key::K) where {K, V} = put!(s, key, val)
Base.getindex(s::Store{K, V}, key::K) where {K, V} = get(s, key)

# TODO delete key without getting the value
function Base.delete!(s::StoreData, key)
    val = get(s, key)
    @assert !isnothing(val)
    put!(s, key, val, true)
end

function Base.delete!(s::Store)
    rm("blobs", recursive=true, force=true)
    empty!(inmemory_levels)
    empty!(inmemory_tables)
end

function compact(s::Store{K, V}) where {K, V}
    # Return if first level has enough empty space
    !isempty(s.data[]) && !isfull(get_level(Level{K, V}, s.data.first_level[])[]) && return
    # Find first level that has enough empty space
    current = get_level(Level{K, V}, s.data.first_level[])
    next = !isnothing(current) ? 
                    get_level(Level{K, V}, current.next_level[]) : nothing
    force_remove = false
    while !isnothing(next) && !islast(next[])
        if !isfull(next[])
            force_remove = islast(next[])
            break
        end
        current = get_level(Level{K, V}, current.next_level[]) 
        next = get_level(Level{K, V}, next.next_level[])
    end
    # Create new level if tree has no level
    if isnothing(current)
        new_level = Blobs.malloc_and_init(Level{K, V}, 
                                          Vector{Int64}(),
                                          Vector{Int64}(), 
                                          0, 
                                          s.buffer.max_size * s.data.fanout[], 
                                          s.data.table_threshold_size[])
        set_level(new_level)
        s.data.first_level[] = new_level.id[]
        return
    end
    # Create new level if we didn't find enough space in tree
    if isnothing(next) || isfull(next[])
        last_level = isnothing(next) ? current : next
        new_level = Blobs.malloc_and_init(Level{K, V},
                                          Vector{Int64}(),
                                          Vector{Int64}(), 
                                          0,
                                          last_level.size[] * s.data.fanout[], 
                                          s.data.table_threshold_size[])
        new_level.prev_level[] = last_level.id[]
        last_level.next_level[] = new_level.id[]
        set_level(new_level)
        current, next = last_level, new_level
        force_remove = true
    end
    # Compact levels and free up space in first level
    after_next = get_level(Level{K, V}, next.next_level[])
    while !isfirst(next[]) 
        for table in current.tables[]
            next = compact(next, get_table(Table{K, V}, table), force_remove)
        end
        if !isnothing(after_next) 
            next.next_level[] = after_next.id[]
            after_next.prev_level[] = next.id[]
        end
        set_level(next)
        current = empty(current)
        current.next_level[] = next.id[]
        set_level(current)
        after_next = next
        next = current
        current = get_level(Level{K, V}, current.prev_level[])
        force_remove = false
    end
end

struct Iterator{K, V}
    start::K
    buffer_entries::Vector{Entry{K, V}}
    levels::Vector{Level{K, V}}
    function Iterator(s::Store{K, V}) where {K, V}
        levels = Vector{Level{K, V}}()
        l = get_level(Level{K, V}, s.data.first_level[])
        while !isnothing(l)
            push!(levels, l[])
            l = get_level(Level{K, V}, l.next_level[])
        end
        start = nothing
        for level in levels
            for t in level.tables
                start = isnothing(start) ? min(get_table(Table{K, V}, t)[]) : min(start, min(get_table(Table{K, V}, t)[]))
            end
        end
        new{K, V}(start, 
                  [last(p) for p in sort(collect(s.buffer.entries))], 
                  levels)
    end
    function Iterator(s::Store{K, V}, start) where {K, V}
        levels = Vector{Level{K, V}}()
        l = get_level(Level{K, V}, s.data.first_level[])
        while !isnothing(l)
            push!(levels, l[])
            l = get_level(Level{K, V}, l.next_level[])
        end
        new{K, V}(convert(K, start), 
                  [last(p) for p in sort(collect(s.buffer.entries))],  
                  levels)
    end
end

mutable struct LevelIterationState{K, V}
    current_entry::Entry{K, V}
    current_table_index::Int64
    current_entry_index::Int64
    done::Bool
end

mutable struct IterationState{K, V}
    current_buffer_state::Int64
    current_levels_state::Vector{LevelIterationState{K, V}}
end

function next_state(s::LevelIterationState{K, V}, l::Level{K, V}) where {K, V}
    s.current_entry_index += 1
    t = get_table(Table{K, V}, l.tables[s.current_table_index])[]
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
    b = iter.buffer_entries
    buffer_state = length(b) > 0 ? lub(b, 1, length(b), iter.start) : 0
    levels_state = Vector{LevelIterationState{K, V}}()
    for i in 1:length(iter.levels)
        table_index = key_table_index(iter.levels[i], iter.start)
        t = get_table(Table{K, V}, iter.levels[i].tables[table_index])[]
        entry_index = lub(t.entries, 1, t.size, iter.start)
        entry = t.entries[entry_index]
        push!(levels_state, LevelIterationState(t.entries[entry_index],
                                                table_index,
                                                entry_index,
                                                length(iter.levels[i].tables) == 0))
    end
    return (iter.start, IterationState{K, V}(buffer_state, levels_state))
end

function iter_next(iter::Iterator, state)
    element, iter_state = state
    b = iter.buffer_entries
    next = nothing
    next_index = 0
    for i in 1:length(iter.levels)
        if !iter_state.current_levels_state[i].done && (isnothing(next) || iter_state.current_levels_state[i].current_entry < next)
            next = iter_state.current_levels_state[i].current_entry
            next_index = i
        end
    end
    if 0 < iter_state.current_buffer_state <= length(b) && (isnothing(next) || next == b[iter_state.current_buffer_state])
       iter_state.current_buffer_state += 1 
    else 
        next_state(iter_state.current_levels_state[next_index], iter.levels[next_index]) 
    end
    return (element, (next, iter_state))
end

function iter_done(iter::Iterator, state)
    _, iter_state = state
    0 < iter_state.current_buffer_state <= length(iter.buffer_entries) && return false
    for level_state in iter_state.current_levels_state
        !level_state.done && return false
    end
    true
end