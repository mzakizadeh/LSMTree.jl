struct Level{K, V}
    id::Int64
    next_level::Int64
    prev_level::Int64
    size::Int64
    max_size::Int64
    table_threshold_size::Int64
    bounds::BlobVector{K}
    tables::BlobVector{Int64}
end

inmemory_levels = Dict{Int64, Blob{Level}}()

isfull(l::Level) = l.size >= l.max_size
islast(l::Level) = l.next_level <= 0
isfirst(l::Level) = l.prev_level <= 0
newlevel_id() = reverse(sort(collect(keys(inmemory_levels)))) + 1

function level(id::Int,
               max_size::Int,
               table_threshold_size::Int) where {K, V}
    l = Blobs.malloc_and_init(Level{K, V}, max_size / sizeof(Entry{K, V}))
    l.id[] = id
    l.max_size[] = max_size
    l.table_threshold_size[] = table_threshold_size
    return l
end

function Blobs.child_size(::Type{Level{K, V}}, capacity::Int) where {K, V}
    T = Level{K, V}
    +(Blobs.child_size(fieldtype(T, :bounds), capacity),
      Blobs.child_size(fieldtype(T, :tables), capacity + 1))
end

function Blobs.init(l::Blob{Level{K, V}}, 
                    free::Blob{Nothing}, 
                    capacity::Int) where {K, V}
    free = Blobs.init(l.bounds, free, capacity)
    free = Blobs.init(l.tables, free, capacity + 1)
    l.size[] = 0
    free
end

function get_level(id::Int64) where {K, V}
    haskey(inmemory_levels, id) && return inmemory_levels[id]
    # TODO open level from saved file
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
    merge!(l, t, partition(l.bounds, t.entries), force_remove)
end

function merge!(l::Level{K, V}, 
                t::Table{K, V}, 
                indecies::Vector, 
                force_remove) where {K, V}
    j = 0
    pushfirst!(indecies, 1)
    tables = l.tables[]
    for i in 1:length(l.tables)
        if indecies[i + 1] - indecies[i] > 1
            table = merge(l.tables[i + j], t, indecies[i], indecies[i + 1], force_remove)
            # l.size += table.size - l.tables[i + j].size
            if table.size > l.table_threshold_size
                # TODO Split the table and update level
                # (p1, p2) = split(table)
                # push!(inmemory_levels, p1)
                # push!(inmemory_levels, p2)
                # deleteat!(l.tables, i + j)
                # insert!(l.tables, i + j, p2)
                # insert!(l.tables, i + j, p1)
                # insert!(l.bounds, i + j, max(p1))
                # j += 1
            else 
                # TODO Update level
                l.tables[i + j] = table 
            end
        end
    end
end

# Returns indices
function partition(bounds::BlobVector{K}, 
                   entries::BlobVector{Entry{K, V}}) where {K, V}
    # If there is no bound then we don't need to split vector
    length(bounds) == 0 && return []
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
    return indecies
    # for k in 1:length(bounds) + 1
    #     if k == 1
    #         push!(parts, entries[1:indecies[k] - 1])
    #     elseif k == length(bounds) + 1
    #         push!(parts, entries[indecies[length(bounds)]:length(entries)])
    #     else
    #         push!(parts, entries[indecies[k - 1]:indecies[k] - 1])
    #     end
    # end
    # return parts
end

function key_table_index(l::Level{K, V}, k::K) where {K, V}
    k < l.bounds[1] && return 1
    k > l.bounds[length(l.bounds)] && return length(l.tables)
    for i in 1:length(l.bounds) - 1
        k > l.bounds[i] && k < l.bounds[i + 1] && return i + 1
    end
end