abstract type BaseStore{K, V} end

struct Store{K, V}
    first_level::Int64
    fanout::Int64
    first_level_max_size::Int64
    table_threshold_size::Int64
end

mutable struct BufferStore{K, V} <: BaseStore{K, V}
    buffer::Buffer{K, V}
    store::Blob{Store{K, V}}
    BufferStore{K, V}(store::Blob{Store{K, V}},
                      buffer_max_size::Integer=1000) where {K, V} = 
        new{K, V}(Buffer{K, V}(buffer_max_size), store)
end

# TODO: Update methods based on structures updates!

Base.eltype(s::BaseStore{K, V}) where {K, V} = Tuple{K, V}
haslevel(s::Store{K, V}) where {K, V} = s.first_level > 0

function Base.length(s::BufferStore)
    len = length(s.buffer.entries)
    for l in s.store.levels len += length(l) end
    return len
end

function Base.get(s::BufferStore{K, V}, key) where {K, V}
    key = convert(K, key) 
    val = get(s.buffer, key)
    !Base.isnothing(val) && return val
    for l in s.levels
        val = get(l, key)
        !Base.isnothing(val) && return val
    end
    nothing
end

function Base.put!(s::Store{K, V}, key, val, deleted=false) where {K, V}
    key = convert(K, key)
    val = convert(V, val)
    put!(s.buffer, key, val, deleted)
    if isfull(s.buffer)
        compact(s)
        parts = partition(s.levels[1].bounds, s.buffer.entries)
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
    haslevel(s.store) && !isfull(get_level(s.store.first_level)[]) && return
    # Find first level that has enough empty space
    current = get_level(s.store.first_level)
    next = get_level(current.next_level)
    force_remove = false
    while !islast(next[])
        if !isfull(next[])
            force_remove = islast(next[])
            break
        end
        current = get_level(current.next_level) 
        next = get_level(next.next_level)
    end
    # Create new level if we didn't find enough space in tree
    if isfull(next[])
        last_level = next
        new_level = level(newlevel_id(), 
                          last_level.max_size[] * s.fanout,
                          s.table_threshold_size)
        last_level.next_level[] = new_level
        # If there is only one level then there's no need to continue 
        isfirst(last_level) && return
        current, next = get_level(last_level.prev_level[]), last_level
    end
    # Compact levels and free up space
    while !isfirst(next) 
        for table in current.tables
            compact(next, table, force_remove)
        end
        empty!(current)
        current, next = get_level(current.prev_level[]), get_level(next.prev_level[])
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