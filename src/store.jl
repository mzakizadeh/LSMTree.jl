mutable struct Store{K, V}
    buffer::Buffer{K, V}
    levels::Vector{Level}
    fanout::Integer
    function Store{K, V}(buffer_max_entries::Integer, fanout::Integer) where {K, V}
        @assert isbitstype(K) && isbitstype(V) "must be isbitstype"
        new{K, V}(Buffer{K, V}(buffer_max_entries), Vector{Level}(), fanout)
    end
end

struct ImmutableStore{K, V}
    buffer::Buffer{K, V}
    levels::Vector{Level}
    fanout::Integer
    function ImmutableStore(store::Store{K, V}) where {K, V}
        new{K, V}() #TODO: ImmutableStore
    end
end

struct IterState
    tables_pointer::Vector{Integer}
    entries_pointer::Vector{Integer}
    done::Vector{Bool}
    IterState(s::Store) = new(ones(Integer, length(s.levels)), ones(Integer, length(s.levels)), falses(length(s.levels)))
end

function Base.put!(s::Store, key, val)
    put!(s.buffer, key, val)
    if isfull(s.buffer)
        compact!(s)
        s.levels[1].size += length(s.buffer.entries)
        merge!(s.levels[1], partition_with_bounds(s.levels[1].bounds, s.buffer.entries))
        empty!(s.buffer)
    end
end

Base.delete!(s::Store, key) = push!(s.buffer, key, missing)
Base.eltype(s::Store{K, V}) where {K, V} = Tuple{K, V}

function Base.length(s::Store)
    result = 0
    for l in s.levels
        result += l.size
    end
    return result
end

# TODO: remove deleted blobs
function compact!(s::Store{K, V}) where {K, V}
    next, current = missing, missing
    i = 1
    while i < length(s.levels)
        if !isfull(s.levels[i + 1])
            current = s.levels[i]
            next = s.levels[i + 1]
            break
        end
        i += 1
    end
    if ismissing(next)
        newsize = length(s.levels) != 0 ? s.levels[length(s.levels)].max_size * s.fanout : s.buffer.max_size * s.fanout
        push!(s.levels, Level{K, V}(length(s.levels) + 1, newsize))
        if length(s.levels) == 1 return end
        current = s.levels[length(s.levels) - 1]
        next = s.levels[length(s.levels)]
    end
    for table in current.tables
        compact!(next, table)
    end
    empty!(current)
    while i > 1 
        for table in s.levels[i - 1].tables
            compact!(s.levels[i], table)
        end
        empty!(s.levels[i - 1])
        i -= 1 
    end
end

function Base.get(s::Store{K, V}, key) where {K, V}
    key = convert(K, key) 
    val = get(s.buffer, key)
    if val != nothing return val end
    for l in s.levels
        val = get(l, key)
        if val != nothing return val end
    end
    print("not found")
end

iter_init(s::Store) = IterState(s)

function iter_next(s::Store{K,V}, state::IterState)::Tuple{Bool, Pair{K,V}} where {K,V}
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
        state.tables_pointer[level_index] > length(s.levels[level_index].tables) ? state.done[level_index] = true : nothing
    end
    e = s.levels[level_index].tables[table_index].entries[entry_index][]
    return (done(s, state), Pair(e.key, e.val))
end

function done(s::Store, state::IterState) 
    for i in 1:length(s.levels)
        !state.done[i] && return false
    end
    return true
end