mutable struct LeveledTree{K, V}
    buffer::Buffer{K, V}
    levels::Vector{Level}
    fanout::Integer
    function LeveledTree{K, V}(buffer_max_entries::Integer, fanout::Integer) where {K, V}
        @assert isbitstype(K) && isbitstype(V) "must be isbitstype"
        new{K, V}(Buffer{K, V}(buffer_max_entries), Vector{Level}(), fanout)
    end
end

function Base.put!(t::LeveledTree, key, val)
    put!(t.buffer, key, val)
    if isfull(t.buffer)
        compact!(t)
        merge!(t.levels[1], partition_with_bounds(t.levels[1].bounds, t.buffer.entries))
        empty!(t.buffer)
    end
end

Base.delete!(t::LeveledTree, key) = push!(t.buffer, key, missing)

function compact!(t::LeveledTree{K, V}) where {K, V}
    next, current = missing, missing
    i = 1
    while i < length(t.levels)
        if !isfull(t.levels[i + 1])
            current = t.levels[i]
            next = t.levels[i + 1]
            break
        end
        i += 1
    end
    if ismissing(next)
        newsize = length(t.levels) != 0 ? t.levels[length(t.levels)].max_size * t.fanout : t.buffer.max_size
        push!(t.levels, Level{K, V}(length(t.levels) + 1, newsize))
        if length(t.levels) == 1 return end
        current = t.levels[length(t.levels) - 1]
        next = t.levels[length(t.levels)]
    end
    for table in current.tables
        compact!(next, table)
    end
    while i > 1 
        for table in t.levels[i - 1].tables
            compact!(t.levels[i], table)
        end
        i -= 1 
    end
end

function Base.get(t::LeveledTree{K, V}, key) where {K, V}
    key = convert(K, key) 
    val = get(t.buffer, key)
    if val != nothing return val end
    for l in t.levels
        val = get(l, key)
        if val != nothing return val end
    end
    print("not found")
end