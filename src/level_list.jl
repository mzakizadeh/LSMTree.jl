mutable struct LevelList{K, V}
    buffer::Buffer{K, V}
    levels::Vector{Level}
    function LSM{K, V}(buffer_max_entries::Integer, fanout::Integer) where {K, V}
        @assert isbitstype(K) && isbitstype(V) "must be isbitstype"
        new{K, V}(Buffer{K, V}(buffer_max_entries), Vector{Level}())
    end
end

Base.delete!(t::LevelList, key) = push!(t.buffer, key, missing)

function compact!(t::LevelList{K, V}) where {K, V}
    next, current = missing
    i = 1
    while i < length(t.levels)
        if !isfull(t.levels[i + 1])
            current = t.levels[i]
            next = t.levels[i + 1]
            break
        end
    end
    if ismissing(next)
        lastsize = t.levels[length(t.levels)].max_size
        push!(t.levels, Level{K, V}(lastsize * t.fanout))
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

function Base.put!(t::LevelList, key, val)
    put!(t.buffer, key, val)
    if isfull(t.buffer)
        compact!(t)
        merge!(t.levels[1], t.buffer)
        empty!(t.buffer)
    end
end

function Base.get(t::LevelList, key) 
    val = get(t.buffer, key)
    if val != nothing return val end
    for l in t.levels
        val = get(l, key)
        if val != nothing return val end
    end
    print("not found")
end