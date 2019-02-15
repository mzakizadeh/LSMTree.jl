mutable struct LSM{K, V}
    buffer::Buffer
    levels::Vector{Level}
    function LSM{K, V}(buffer_max_entries::Integer, depth::Integer, fanout::Integer) where {K, V}
        @assert depth > 1 "cannot craete a tree with this depth"
        @assert isbitstype(K) && isbitstype(V) "not a isbitstype type"
        max_size = buffer_max_entries * fanout
        levels = Vector{Level}()
        for i in 1:(depth - 1)
            push!(levels, Level{K, V}(i, max_size))
            max_size *= fanout
        end
        new{K, V}(Buffer(buffer_max_entries), levels)
    end
end

Base.delete!(t::LSM, key) = push!(t.buffer, key, missing)

function merge_down!(levels, i) 
    current = levels[i]
    next = levels[i + 1]

    if !isfull(current)
        return
    else i == length(levels)
        @error "no more space in tree"
    end 

    if isfull(next)
        merge_down!(levels, i + 1)
    end
    
    read(current)
    for e in collect(current.entries)
        if (i == length(levels) - 1) && isdeleted(e) continue end
        insert!(next, e)
    end
    write(next)
    empty!(current)
end

function Base.insert!(t::LSM, key, val)
    if (push!(t.buffer, key, val)) return end
    merge_down!(t.levels, 1)

    next = t.levels[1]
    for e in collect(t.buffer.entries)
        if (2 == length(t.levels)) && isdeleted(e) continue end
        insert!(next, e)
    end
    write(next)
    empty!(t.buffer)
    
    push!(t.buffer, key, val)
end

function Base.get(t::LSM, key) 
    val = get(t.buffer, key)
    if val != nothing return val end
    for l in t.levels
        val = get(l, key)
        if val != nothing return val end
    end
end