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
    function Store{K, V}(buffer_max_size::Integer=1000) where {K, V} 
        data = Blobs.malloc_and_init(StoreData{K, V}, 2, buffer_max_size * 2, 10)
        new{K, V}(Buffer{K, V}(buffer_max_size), data)
    end
end

isempty(s::StoreData{K, V}) where {K, V} = s.first_level <= 0

function Base.length(s::Store)
    len = length(s.buffer.entries)
    for l in s.data.levels len += length(l) end
    return len
end

function Base.get(s::Store{K, V}, key) where {K, V}
    key = convert(K, key) 
    val = get(s.buffer, key)
    !isnothing(val) && return val
    for l in s.levels
        val = get(l, key)
        !isnothing(val) && return val
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
        inmemory_levels[l.id[]] = l
        s.data.first_level[] = l.id[]
        empty!(s.buffer)
        dump(s)
    end
end

# TODO delete key without getting the value
function Base.delete!(s::StoreData, key)
    val = get(s, key)
    @assert !isnothing(val)
    put!(s, key, val, true)
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
        inmemory_levels[new_level.id[]] = new_level
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
        inmemory_levels[new_level.id[]] = new_level
        current, next = last_level, new_level
    end
    # Compact levels and free up space in first level
    temp = nothing
    while !isfirst(next[]) 
        for table in current.tables[]
            next = compact(next, get_table(Table{K, V}, table), force_remove)
        end
        inmemory_levels[next.id[]] = next
        if !isnothing(temp) 
            temp.prev_level[] = next.id[]
        end
        next.next_level[] = isnothing(temp) ? -1 : temp.id[]
        current.next_level[] = next.id[]
        inmemory_levels[current.id[]] = empty(current)
        temp = next
        next = current
        current = get_level(Level{K, V}, current.prev_level[])
        force_remove = false
    end
end

# struct IterState{K, V}
#     store::ImmutableStore{K, V}
#     tables_pointer::Vector{Integer}
#     entries_pointer::Vector{Integer}
#     done::Vector{Bool}
#     function IterState{K, V}(s::BaseStore{K, V}) where {K, V}
#         store = s isa Store ? ImmutableStore{K, V}(s) : s
#         new(
#             store, 
#             ones(Integer, length(store.levels)), 
#             ones(Integer, length(store.levels)), 
#             falses(length(store.levels))
#         )
#     end
# end

# iter_init(s::BaseStore{K, V}) where {K, V} = IterState{K, V}(s)

# function iter_next(state::IterState{K, V})::Tuple{Bool, Pair{K,V}} where {K,V}
#     s = state.store
#     level_index, table_index, entry_index = missing, missing, missing
#     for i in 1:length(s.levels)
#         if !state.done[i]
#             level_index, table_index, entry_index = i, state.tables_pointer[i], state.entries_pointer[i]
#             break
#         end
#     end
#     for i in (level_index + 1):length(s.levels)
#         if !state.done[i]
#             e = s.levels[i].tables[state.tables_pointer[i]].entries[state.entries_pointer[i]][]
#             if e < s.levels[level_index].tables[table_index].entries[entry_index][]
#                 level_index, table_index, entry_index = i, state.tables_pointer[i], state.entries_pointer[i]
#             end
#         end
#     end
#     state.entries_pointer[level_index] += 1
#     if state.entries_pointer[level_index] > length(s.levels[level_index].tables[table_index])
#         state.entries_pointer[level_index] = 1
#         state.tables_pointer[level_index] += 1
#         state.done[level_index] = state.tables_pointer[level_index] > length(s.levels[level_index].tables)
#     end
#     e = s.levels[level_index].tables[table_index].entries[entry_index][]
#     return (done(state), Pair(e.key, e.val))
# end

# function done(state::IterState) 
#     s = state.store
#     for i in 1:length(s.levels)
#         !state.done[i] && return false
#     end
#     return true
# end

# function seek_lub_search(hint_state::IterState{K, V}, search_key) where {K, V}
#     k = convert(K, search_key)
#     s = hint_state.store
#     ls = s.levels
#     for i in 1:length(ls)
#         table_index = key_table_index(ls[i], k)
#         table = ls[i].tables[table_index]
#         hint_state.tables_pointer[i] = table_index
#         hint_state.entries_pointer[i] = lub(table.entries, 1, length(table), k)
#     end
# end

function Base.dump(s::Store{K, V}) where {K, V}
    print("b\t")
    for e in s.buffer.entries
        print(e.key[], " ")
    end
    print("\n\n")
    depth = 1
    l = get_level(Level{K, V}, s.data.first_level[])
    while !isnothing(l)
        print("l$depth\t")
        for t_id in l.tables[]
            t = get_table(Table{K, V}, t_id)
            for i in 1:length(t.entries[])
                print(t.entries[i][].key, " ")
            end
            print("\n\t")
        end
        print("\n\n")
        l = get_level(Level{K, V}, l.next_level[])
        depth += 1
    end
end
