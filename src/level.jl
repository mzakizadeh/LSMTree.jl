const TABLE_THRESHOLD = 2000000

mutable struct Level{K, V}
    depth::Integer
    size::Integer
    max_size::Integer
    tables::Vector{Table{K, V}}
    # bf::BloomFilter
    # min::K
    # max::K
    bounds::Vector{K}
    function Level{K, V}(depth::Integer, max_size::Integer) where {K, V}
        tables = Vector{Table{K, V}}()
        bounds = Vector{K}()
        # bf = BloomFilter{K}(convert(Int, max_size * 0.5))
        new{K, V}(depth, 0, max_size, tables, bounds)
    end
end

isfull(l::Level) = l.size == l.max_size

function Base.get(l::Level{K, V}, key::K) where {K, V} 
    # if isset(l.bf, key)
        for t in l.tables
            if key > t.min && key < t.max return get(t, key) end
        end
    # else return nothing end
end

function compact!(l::Level{K, V}, t::Table{K, V}) where {K, V} 
    splitted_table = split_with_bounds(l.bounds, t.entries)
    for i in 1:length(l.tables)
        if length(splitted_table[i]) != 0
            table = merge(t.tables[i], splitted_table[i])
            if sizeof(table) > TABLE_THRESHOLD
                (p1, p2) = split(table)
                deleteat!(l.tables, i)
                insert!(l.tables, p2)
                insert!(l.tables, p1)
                insert!(l.bounds, i, max(p1))
            end
        end
    end
end

function split_with_bounds(entries::Vector, bounds::Vector)
    if length(bounds) == 0 return [entries] end

    indecies = []
    i, j = 1, 1
    while i <= length(entries) && length(indecies) < length(bounds)
        if entries[i].key > bounds[j]
            j += 1
            while j <= length(bounds) && entries[i].key > bounds[j]
                push!(indecies, i)
                j += 1
            end
            push!(indecies, i)
        end
        i += 1
        if i > length(entries)
            while length(indecies) < length(bounds)
                push!(indecies, i)
            end
        end
    end

    split_result = []
    for k in 1:length(bounds) + 1
        if k == 1
            push!(split_result, entries[1:indecies[k] - 1])
        elseif k == length(bounds) + 1
            push!(split_result, entries[indecies[length(bounds)]:length(entries)])
        else
            push!(split_result, entries[indecies[k - 1]:indecies[k] - 1])
        end
    end
    return split_result
end