abstract type BaseStore{K, V} end

struct Store{K, V}
    node::Blob{Level{K, V}}
    fanout::Integer
    first_level_max_size::Integer
    table_threshold_size::Integer
    function Store{K, V}(first_level_max_size::Integer=10000000, 
                         fanout::Integer=10, 
                         table_threshold_size::Integer=2000000) where {K, V}
        @assert isbitstype(K) 
        @assert isbitstype(V)
        new{K, V}(nothing, fanout, first_level_max_size, table_threshold_size)
    end
end

mutable struct BufferStore{K, V} <: BaseStore{K, V}
    buffer::Buffer{K, V}
    store::Blob{Store{K, V}}
    BufferStore{K, V}(buffer_max_size::Integer=4000000, 
                      store::Store{K, V}) where {K, V} = 
        new{K, V}(Buffer(buffer_max_size), store)
end

# TODO: Update methods based on structures updates!

Base.eltype(s::BaseStore{K, V}) where {K, V} = Tuple{K, V}

function Base.length(s::BufferStore)
    len = length(s.buffer.entries)
    for l in s.store.levels len += length(l) end
    len
end

function Base.get(s::BufferStore{K, V}, key) where {K, V}
    key = convert(K, key) 
    val = get(s.buffer, key)
    !Base.isnothing(val) && val
    for l in s.levels
        val = get(l, key)
        !Base.isnothing(val) && val
    end
    nothing
end

function Base.put!(s::Store{K, V}, key, val, deleted=false) where {K, V}
    key = convert(K, key)
    val = convert(V, val)
    put!(s.buffer, key, val, deleted)
    if isfull(s.buffer)
        compact(s)
        parts = partition_with_bounds(s.levels[1].bounds, s.buffer.entries)
        merge!(s.levels[1], parts)
        empty!(s.buffer)
    end
end

# TODO delete key without getting the value
function Base.delete!(s::Store, key)
    val = get(s, key)
    @assert !Base.isnothing(val)
    put!(s, key, val, true)
end

function compact(s::BaseStore{K, V}) where {K, V}
    # Return if first level has enough empty space
    !Base.isnothing(s.store.levels_head) && !isfull(s.store.levels_head[]) && return
    # Find first level that has enough empty space
    current, next, force_remove = s.store.levels_head[], current.next_level[], false
    while notlast(next.next_level)
        if !isfull(next)
            force_remove = Base.isnothing(next.next_level)
            break
        end
        current, next = current.next_level, next.next_level
    end
    # Create new level if we didn't find enough space in tree
    if isfull(next)
        last_level = next
        new_level = missing # TODO create new level
        last_level.next_level = new_level
        length(s.levels) == 1 && return
        current = s.levels[length(s.levels) - 1]
        next = s.levels[length(s.levels)]
    end
    for table in current.tables
        compact(next, table, force_remove)
    end
    empty!(current)
    while notfirst(current)
        current = current.prev_level
        next = next.prev_level
        for table in current.tables
            compact(next, table)
        end
        empty!(current)
    end
end

struct IterState{K, V}
    store::ImmutableStore{K, V}
    tables_pointer::Vector{Integer}
    entries_pointer::Vector{Integer}
    done::Vector{Bool}
    function IterState{K, V}(s::BaseStore{K, V}) where {K, V}
        store = s isa Store ? ImmutableStore{K, V}(s) : s
        new(
            store, 
            ones(Integer, length(store.levels)), 
            ones(Integer, length(store.levels)), 
            falses(length(store.levels))
        )
    end
end

iter_init(s::BaseStore{K, V}) where {K, V} = IterState{K, V}(s)

function iter_next(state::IterState{K, V})::Tuple{Bool, Pair{K,V}} where {K,V}
    s = state.store
    level_index, table_index, entry_index = missing, missing, missing
    for i in 1:length(s.levels)
        if !state.done[i]
            level_index, table_index, entry_index = i, state.tables_pointer[i], state.entries_pointer[i]
            break
        end
    end
    for i in (level_index + 1):length(s.levels)
        if !state.done[i]
            e = s.levels[i].tables[state.tables_pointer[i]].entries[state.entries_pointer[i]][]
            if e < s.levels[level_index].tables[table_index].entries[entry_index][]
                level_index, table_index, entry_index = i, state.tables_pointer[i], state.entries_pointer[i]
            end
        end
    end
    state.entries_pointer[level_index] += 1
    if state.entries_pointer[level_index] > length(s.levels[level_index].tables[table_index])
        state.entries_pointer[level_index] = 1
        state.tables_pointer[level_index] += 1
        state.done[level_index] = state.tables_pointer[level_index] > length(s.levels[level_index].tables)
    end
    e = s.levels[level_index].tables[table_index].entries[entry_index][]
    return (done(state), Pair(e.key, e.val))
end

function done(state::IterState) 
    s = state.store
    for i in 1:length(s.levels)
        !state.done[i] && return false
    end
    return true
end

function seek_lub_search(hint_state::IterState{K, V}, search_key) where {K, V}
    k = convert(K, search_key)
    s = hint_state.store
    ls = s.levels
    for i in 1:length(ls)
        table_index = key_table_index(ls[i], k)
        table = ls[i].tables[table_index]
        hint_state.tables_pointer[i] = table_index
        hint_state.entries_pointer[i] = lub(table.entries, 1, length(table), k)
    end
end

function Base.dump(s::BaseStore)
    print("b\t")
    for e in s.buffer.entries
        print(e.key[], " ")
    end
    print("\n\n")
    for l in s.levels
        print("l$(l.depth)\t")
        for t in l.tables
            for e in t.entries
                print(e.key[], " ")
            end
            print("\n\t")
        end
        print("\n\n")
    end
end