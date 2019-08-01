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