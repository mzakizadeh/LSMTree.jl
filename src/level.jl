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

isfull(l::Level) = l.size >= l.max_size
islast(l::Level) = l.next_level <= 0
isfirst(l::Level) = l.prev_level <= 0

function create_id(::Type{Level}) 
    length(inmemory_levels) == 0 && return 1
    return reverse(sort(collect(keys(inmemory_levels))))[1] + 1
end

function empty(l::Blob{Level{K, V}}) where {K, V}
    res = Blobs.malloc_and_init(Level{K, V}, 
                                Vector{Int64}(), 
                                Vector{K}(), 0, 
                                l.max_size[], 
                                l.table_threshold_size[])
    res.id[] = l.id[]
    res.prev_level[] = l.prev_level[]
    res.next_level[] = l.next_level[]
    return res
end

function get_level(::Type{Level{K, V}}, id::Int64) where {K, V}
    id <= 0 && return nothing
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

function set_level(l::Blob{Level{K, V}}) where {K, V}
    inmemory_levels[l.id[]] = l
    open("blobs/$(l.id[]).lvl", "w+") do file
        unsafe_write(file, pointer(l), getfield(l, :limit))
    end
end

function Base.length(l::Level)
    len = 0
    for t in l.tables len += length(t) end
    len
end

function Base.get(l::Level{K, V}, key::K) where {K, V} 
    # if isset(l.bf, key)
        for i in 1:length(l.tables)
            i == length(l.tables) && return get(get_table(Table{K, V}, l.tables[i])[], key)
            if key <= l.bounds[i]
                return get(get_table(Table{K, V}, l.tables[i])[], key)
            end
        end
    # else return nothing end
    nothing
end

function compact(l::Blob{Level{K, V}},
                 t::Blob{Table{K, V}},
                 force_remove) where {K, V}
    indecies = partition(l.bounds[], t.entries[])
    return merge(l, t.entries[], indecies, force_remove)
end

function Base.merge(l::Blob{Level{K, V}},
                    e::BlobVector{Entry{K, V}},
                    indices::Vector{Int64}, 
                    force_remove) where {K, V}
    result_tables, result_bounds = Vector{Int64}(), Vector{K}()
    # If level has no table
    if length(indices) < 3
        if length(l.tables[]) > 0
            table = merge(get_table(Table{K, V}, l.tables[1][]), 
                          e, 
                          indices[1], 
                          indices[2], 
                          false)
        else
            entries = Vector{Entry{K, V}}()
            for i in 1:length(e)
                push!(entries, e[i])
            end
            table = Blobs.malloc_and_init(Table{K, V}, entries)
        end
        if table.size[] > l.table_threshold_size[]
            (t1, t2) = split(table)
            set_table(t1)
            push!(result_tables, t1.id[])
            push!(result_bounds, max(t1[]))
            set_table(t2)
            push!(result_tables, t2.id[])
            push!(result_bounds, max(t2[]))
        else
            set_table(table)
            push!(result_tables, table.id[])
            push!(result_bounds, max(table[]))
        end
    else
        for i in 1:length(l.tables[])
            if indices[i + 1] - indices[i] > 0
                table = merge(get_table(Table{K, V}, l.tables[i][]), 
                              e, 
                              indices[i], 
                              indices[i + 1], 
                              false)
                if table.size[] > l.table_threshold_size[]
                    (t1, t2) = split(table)
                    set_table(t1)
                    push!(result_tables, t1.id[])
                    push!(result_bounds, max(t1[]))
                    set_table(t2)
                    push!(result_tables, t2.id[])
                    push!(result_bounds, max(t2[]))
                else
                    set_table(table)
                    push!(result_tables, table.id[])
                    push!(result_bounds, max(table[]))
                end
            else 
                push!(result_tables, l.tables[i][])
                push!(result_bounds, i != length(l.tables[]) ? l.bounds[i][] : i)
            end
        end
    end
    pop!(result_bounds)
    res = Blobs.malloc_and_init(Level{K, V}, 
                                result_tables, 
                                result_bounds,
                                l.size[] + length(e),
                                l.max_size[],
                                l.table_threshold_size[])
    res.prev_level[], res.next_level[] = l.prev_level[], l.next_level[]
    return res
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
    while length(indecies) < length(bounds) + 1
        push!(indecies, i - 1)
    end
    push!(indecies, length(entries))
    return indecies
end

function key_table_index(l::Level{K, V}, k::K) where {K, V}
    k <= l.bounds[1] && return 1
    k > l.bounds[length(l.bounds)] && return length(l.tables)
    for i in 1:length(l.bounds) - 1
        k > l.bounds[i] && k < l.bounds[i + 1] && return i + 1
    end
end