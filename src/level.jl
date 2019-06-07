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

inmemory_levels = Dict{Int64, Blob}()

isfull(l::Level) = l.size >= l.max_size
islast(l::Level) = l.next_level <= 0
isfirst(l::Level) = l.prev_level <= 0

function create_id(::Type{Level}) 
    length(inmemory_levels) == 0 && return 1
    return reverse(sort(collect(keys(inmemory_levels))))[1] + 1
end

function get_level(::Type{Level{K, V}}, id::Int64) where {K, V}
    haskey(inmemory_levels, id) && return inmemory_levels[id]
    path = "blobs/$id.lvl"
    if isfile(path)
        open(path) do f
            size = filesize(f)
            p = Libc.malloc(size)
            b = Blob{Level{K, V}}(p, 0, size)
            unsafe_read(f, p, size)
            inmemory_levels[b.id[]] = b
        end
        return inmemory_levels[id]
    end
    nothing
end

function write(t::Blob{Level{K, V}}) where {K, V}
    open("blobs/$(t.id[]).lvl", "w+") do file
        unsafe_write(file, pointer(t), getfield(t, :limit))
    end
end

function Blobs.child_size(::Type{Level{K, V}}, 
                          result_tables::Vector{Int64},
                          result_bounds::Vector{K},
                          size::Int64,
                          max_size::Int64,
                          table_threshold_size::Int64) where {K, V}
    T = Level{K, V}
    +(Blobs.child_size(fieldtype(T, :bounds), length(result_bounds)),
      Blobs.child_size(fieldtype(T, :tables), length(result_tables)))
end

function Blobs.init(l::Blob{Level{K, V}},
                    free::Blob{Nothing},
                    result_tables::Vector{Int64},
                    result_bounds::Vector{K},
                    size::Int64,
                    max_size::Int64,
                    table_threshold_size::Int64) where {K, V}
    free = Blobs.init(l.bounds, free, length(result_bounds))
    free = Blobs.init(l.tables, free, length(result_tables))
    for i in 1:length(result_tables)
        l.tables[i][] = result_tables[i]
    end
    for i in 1:length(result_bounds)
        l.bounds[i][] = result_bounds[i]
    end
    l.id[] = create_id(Level)
    l.size[] = size
    l.max_size[] = max_size
    l.table_threshold_size[] = table_threshold_size
    l.next_level[], l.prev_level[] = -1, -1
    free
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

function compact(l::Blob{Level{K, V}}, 
                 t::Blob{Table{K, V}}, 
                 force_remove=false) where {K, V} 
    indecies = partition(l.bounds[], t.entries[])
    result_tables, result_bounds = Vector{Int64}(), Vector{K}()
    for i in 1:length(l.tables[])
        if indecies[i + 1] - indecies[i] > 0
            table = merge(get_table(l.tables[i][]), 
                          t.entries, 
                          indecies[i], 
                          indecies[i + 1], 
                          force_remove)
            if table.size[] > l.table_threshold_size[]
                (t1, t2) = split(table)
                inmemory_tables[t1.id[]] = t1
                push!(result_tables, t1.id[])
                push!(result_bounds, max(t1[]))
                inmemory_tables[t2.id[]] = t2
                push!(result_tables, t2.id[])
                push!(result_bounds, max(t2[]))
            else 
                inmemory_tables[table.id[]] = table
                push!(result_tables, table.id[])
                push!(result_bounds, max(table[]))
            end
        end
    end
    pop!(result_bounds)
    return Blobs.malloc_and_init(Level{K, V}, 
                                 result_tables, 
                                 result_bounds,
                                 l.size[] + t.size[],
                                 l.max_size[],
                                 l.table_threshold_size[])
end

# Returns indices
function partition(bounds::BlobVector{K}, 
                   entries::BlobVector{Entry{K, V}}) where {K, V}
    indecies, i, j = Vector{Int64}(), 1, 1
    push!(indecies, 0)
    while i <= length(entries) && length(indecies) < length(bounds) + 1
        if entries[i].key[] > bounds[j]
            j += 1
            while j <= length(bounds) && entries[i].key[] > bounds[j]
                push!(indecies, i - 1)
                j += 1
            end
            push!(indecies, i - 1)
        end
        i += 1
    end
    while length(indecies) < length(bounds)
        push!(indecies, i - 1)
    end
    push!(indecies, length(entries))
    return indecies
end

function key_table_index(l::Level{K, V}, k::K) where {K, V}
    k < l.bounds[1] && return 1
    k > l.bounds[length(l.bounds)] && return length(l.tables)
    for i in 1:length(l.bounds) - 1
        k > l.bounds[i] && k < l.bounds[i + 1] && return i + 1
    end
end