abstract type BaseStore{K, V} end

mutable struct Store{K, V} <: BaseStore{K, V}
    buffer::Buffer{K, V}
    levels::Vector{Level}
    fanout::Integer
    first_level_max_size::Integer
    table_threshold_size::Integer
    function Store{K, V}(buffer_max_size::Integer=4000000, first_level_max_size::Integer=10000000, fanout::Integer=10, table_threshold_size::Integer=2000000) where {K, V}
        @assert isbitstype(K) && isbitstype(V) "must be isbitstype"
        new{K, V}(Buffer{K, V}(buffer_max_size), Vector{Level}(), fanout, first_level_max_size, table_threshold_size)
    end
end

struct ImmutableStore{K, V} <: BaseStore{K, V}
    buffer::Buffer{K, V}
    levels::Vector{Level}
    fanout::Integer
    first_level_max_size::Integer
    table_threshold_size::Integer
    function ImmutableStore{K, V}(s::Store{K, V}) where {K, V}
        return new{K, V}(deepcopy(s.buffer), deepcopy(s.levels), s.fanout, s.first_level_max_size, s.table_threshold_size)
    end
end

Base.eltype(s::BaseStore{K, V}) where {K, V} = Tuple{K, V}

function Base.length(s::BaseStore)
    len = length(s.buffer.entries)
    for l in s.levels len += length(l) end
    len
end

function Base.get(s::Store{K, V}, key) where {K, V}
    key = convert(K, key) 
    val = get(s.buffer, key)
    if val != nothing return val end
    for l in s.levels
        val = get(l, key)
        if val != nothing return val end
    end
    return nothing
end

function Base.put!(s::Store{K, V}, key, val, deleted=false) where {K, V}
    key = convert(K, key)
    val = convert(V, val)
    put!(s.buffer, key, val, deleted)
    if isfull(s.buffer)
        compact(s)
        partitions = partition_with_bounds(s.levels[1].bounds, s.buffer.entries)
        merge!(s.levels[1], partitions)
        empty!(s.buffer)
    end
end

function put(s::BaseStore{K, V}, key, val, deleted=false) where {K, V}
    key = convert(K, key)
    val = convert(V, val)
    store = ImmutableStore(s)
    put!(store.buffer, key, val)
    if isfull(store.buffer)
        compact(store)
        partitions = partition_with_bounds(store.levels[1].bounds, store.buffer.entries)
        merge!(store.levels[1], partitions)
        empty!(store.buffer)
    end
end

# delete without first getting the value
function Base.delete!(s::Store, key)
    val = get(s, key)
    @assert val != nothing "Can not delete a value that didn't inserted before"
    put!(s, key, val, true)
end

function delete(s::BaseStore, key)
    store = ImmutableStore(s)
    val = get(store, key)
    @assert val != nothing "Can not delete a value that didn't inserted before"
    put!(store, key, val, true)
end

function compact(s::BaseStore{K, V}) where {K, V}
    length(s.levels) > 0 && !isfull(s.levels[1]) && return
    next, current = missing, missing
    force_remove = false
    i = 1
    while i < length(s.levels)
        if !isfull(s.levels[i + 1])
            force_remove = i + 1 == length(s.levels) ? true : false
            current = s.levels[i]
            next = s.levels[i + 1]
            break
        end
        i += 1
    end
    if ismissing(next)
        newsize = s.first_level_max_size * s.fanout ^ length(s.levels)
        push!(s.levels, Level{K, V}(length(s.levels) + 1, newsize, s.table_threshold_size))
        length(s.levels) == 1 && return
        current = s.levels[length(s.levels) - 1]
        next = s.levels[length(s.levels)]
    end
    for table in current.tables
        compact(next, table, force_remove)
    end
    empty!(current)
    while i > 1 
        for table in s.levels[i - 1].tables
            compact(s.levels[i], table)
        end
        empty!(s.levels[i - 1])
        i -= 1 
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