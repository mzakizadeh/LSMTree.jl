mutable struct LevelState{K, V}
    entry::Entry{K, V}
    table_index::Int64
    entry_index::Int64
    done::Bool
end

function next_state(s::LevelState{K, V}, 
                    l::Level{K, V}, 
                    inmemory::InMemoryData) where {K, V}
    s.entry_index += 1
    t = get_table(Table{K, V}, l.tables[s.table_index], inmemory)[]
    if s.entry_index > t.size
        s.entry_index = 1
        s.table_index += 1
        s.done = s.table_index > length(l.tables)
        if !s.done 
            t = get_table(Table{K, V}, l.tables[s.table_index], inmemory)[]
        end
    end
    s.entry = t.entries[s.entry_index]
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

mutable struct IteratorState{K, V}
    levels_state::Vector{LevelState{K, V}}
    done::Bool
end

function iter_init(iter::Iterator{K, V}) where {K, V}
    levels_state = Vector{LevelState{K, V}}()
    first_index = 0
    first = nothing
    # Initialize level states
    for i in 1:length(iter.levels)
        table_index = 1
        t = get_table(Table{K, V}, 
                      iter.levels[i].tables[table_index], 
                      iter.store.inmemory)[]
        entry_index = 1
        entry = t.entries[entry_index]
        if isnothing(first) || entry < first 
            first_index = i
            first = entry
        end
        push!(levels_state, LevelState(t.entries[entry_index],
                                       table_index,
                                       entry_index,
                                       length(iter.levels[i].tables) == 0))
    end
    # Find first entry that is not deleted
    while isdeleted(first)
        for i in 1:length(iter.levels)
            if !levels_state[i].done && first.key == levels_state[i].entry.key
                next_state(levels_state[i], iter.levels[i], iter.store.inmemory)
            end
        end
        first_index = 0
        first = nothing
        for i in 1:length(iter.levels)
            if !levels_state[i].done && (isnothing(first) || levels_state[i].entry < first)
                first = levels_state[i].entry
                first_index = i
            end
        end
    end
    for i in 1:length(iter.levels)
        if !levels_state[i].done && first.key == levels_state[i].entry.key
            next_state(levels_state[i], iter.levels[i], iter.store.inmemory)
        end
    end
    return (first, IteratorState{K, V}(levels_state, length(iter.store) == 0))
end

function iter_next(iter::Iterator, state)
    e, state = state
    @assert !state.done
    next = nothing
    next_index = 0
    # Find next entry that is not flaged as deleted
    while isnothing(next) || isdeleted(next)
        next = nothing
        next_index = 0
        for i in 1:length(iter.levels)
            if !state.levels_state[i].done && (isnothing(next) || state.levels_state[i].entry < next)
                next = state.levels_state[i].entry
                next_index = i
            end
        end
        if next_index == 0
            state.done = true
            return (e, (nothing, state))
        end
        # We use for loop so we don't reiterate old duplicated data
        for i in 1:length(iter.levels)
            if !state.levels_state[i].done && next.key == state.levels_state[i].entry.key
                next_state(state.levels_state[i], iter.levels[i], iter.store.inmemory)
            end
        end
    end
    return (e, (next, state))
end

function seek_lub_search(iter::Iterator{K, V}, state::IteratorState, key) where {K, V}
    key = convert(K, key)
    min_index = 0
    min = nothing
    for i in 1:length(iter.levels)
        table_index = key_table_index(iter.levels[i], key)
        table = get_table(Table{K, V}, iter.levels[i].tables[table_index], iter.store.inmemory)
        entry_index = lub(table.entries[], 1, table.size[], key)
        if entry_index > table.size[] 
            state.levels_state[i].done = true
        else 
            state.levels_state[i].entry = table.entries[entry_index][]
        end
        state.levels_state[i].table_index = table_index
        state.levels_state[i].entry_index = entry_index
        if !state.levels_state[i].done && (isnothing(min) || state.levels_state[i].entry < min) 
            min_index = i
            min = state.levels_state[i].entry
        end
    end
    next_state(state.levels_state[min_index], iter.levels[min_index], iter.store.inmemory) 
    return (min, state)
end

function iter_done(iter::Iterator, state)
    _, iter_state = state
    level_states = iter_state.levels_state
    if iter_state.done
        remove_snapshot(iter.store)
        return true
    end
    return false
end

function Base.iterate(iter::Iterator, state=iter_init(iter))
    iter_done(iter, state) && return nothing
    return iter_next(iter, state)
end