const TABLE_THRESHOLD = 2E6

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
        push!(tables, Table{K, V}(Vector{Blob{Entry{K, V}}}()))
        bounds = Vector{K}()
        # bf = BloomFilter{K}(convert(Int, max_size * 0.5))
        new{K, V}(depth, 0, max_size, tables, bounds)
    end
end

isfull(l::Level) = l.size >= l.max_size

function Base.empty!(l::Level{K, V}) where {K, V}
    empty!(l.tables)
    push!(l.tables, Table{K, V}(Vector{Blob{Entry{K, V}}}()))
    l.size = 0
end

function Base.get(l::Level{K, V}, key::K) where {K, V} 
    # if isset(l.bf, key)
        for t in l.tables
            if key > min(t) && key < max(t) return get(t, key) end
        end
    # else return nothing end
end

function compact!(l::Level{K, V}, t::Table{K, V}) where {K, V} 
    l.size += length(t)
    merge!(l, partition_with_bounds(l.bounds, t.entries))
end

function merge!(l::Level, parts::Vector)
    for i in 1:length(l.tables)
        if length(parts[i]) != 0
            table = merge(l.tables[i], parts[i])
            if sizeof(table) > TABLE_THRESHOLD
                (p1, p2) = split(table)
                deleteat!(l.tables, i)
                insert!(l.tables, i, p2)
                insert!(l.tables, i, p1)
                insert!(l.bounds, i, max(p1))
            else 
                l.tables[i] = table 
            end
        end
    end
end

function partition_with_bounds(bounds::Vector, entries::Vector)
    partitioning_result = Vector()
    length(bounds) == 0 && return push!(partitioning_result, entries)

    indecies = []
    i, j = 1, 1
    while i <= length(entries) && length(indecies) < length(bounds)
        if entries[i].key[] > bounds[j]
            j += 1
            while j <= length(bounds) && entries[i].key[] > bounds[j]
                push!(indecies, i)
                j += 1
            end
            push!(indecies, i)
        end
        i += 1
    end
    while length(indecies) < length(bounds)
        push!(indecies, i)
    end

    for k in 1:length(bounds) + 1
        if k == 1
            push!(partitioning_result, entries[1:indecies[k] - 1])
        elseif k == length(bounds) + 1
            push!(partitioning_result, entries[indecies[length(bounds)]:length(entries)])
        else
            push!(partitioning_result, entries[indecies[k - 1]:indecies[k] - 1])
        end
    end
    return partitioning_result
end