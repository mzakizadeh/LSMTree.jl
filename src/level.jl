mutable struct Level{K, V}
    depth::Integer
    size::Integer
    max_size::Integer
    tables::Vector{Table{K, V}}
    table_threshold_size::Integer
    bounds::Vector{K}
    # bf::BloomFilter
    # min::K
    # max::K
    function Level{K, V}(depth::Integer, max_size::Integer, table_threshold::Integer) where {K, V}
        tables = Vector{Table{K, V}}()
        push!(tables, Table{K, V}(Vector{Blob{Entry{K, V}}}(), 0))
        bounds = Vector{K}()
        # bf = BloomFilter{K}(convert(Int, max_size * 0.5))
        new{K, V}(depth, 0, max_size, tables, table_threshold, bounds)
    end
end

isfull(l::Level) = l.size >= l.max_size

function Base.empty!(l::Level{K, V}) where {K, V} 
    l.tables = Vector{Table{K, V}}()
    push!(l.tables, Table{K, V}(Vector{Blob{Entry{K, V}}}(), 0))
    l.bounds = Vector{K}()
    l.size = 0
end

function Base.length(l::Level)
    len = 0
    for t in l.tables len += length(t) end
    len
end

function Base.get(l::Level{K, V}, key::K) where {K, V} 
    # if isset(l.bf, key)
        for t in l.tables
            if key >= min(t) && key <= max(t) return get(t, key) end
        end
    # else return nothing end
end

function compact(l::Level{K, V}, t::Table{K, V}, force_remove=false) where {K, V} 
    merge!(l, partition_with_bounds(l.bounds, t.entries))
end

function merge!(l::Level, parts::Vector, force_remove=false)
    j = 0
    for i in 1:length(l.tables)
        if length(parts[i]) != 0
            table = merge(l.tables[i + j], parts[i])
            l.size += table.size - l.tables[i + j].size
            if table.size > l.table_threshold_size
                (p1, p2) = split(table)
                deleteat!(l.tables, i + j)
                insert!(l.tables, i + j, p2)
                insert!(l.tables, i + j, p1)
                insert!(l.bounds, i + j, max(p1))
                j += 1
            else 
                l.tables[i + j] = table 
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

function key_table_index(l::Level{K, V}, k::K) where {K, V}
    k < l.bounds[1] && return 1
    k > l.bounds[length(l.bounds)] && return length(l.tables)
    for i in 1:length(l.bounds) - 1
        k > l.bounds[i] && k < l.bounds[i + 1] && return i + 1
    end
end