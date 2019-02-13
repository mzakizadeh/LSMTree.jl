module LSMTree

using JLD

import DataStructures: SortedDict, findkey
import Random.randstring
export LSM, push!, get, delete!

mutable struct Entry{K<:Unsigned, V}
    key::K
    val::V
end

Base.isless(e1::Entry, e2::Entry) = e1.key < e2.key
Base.isequal(e1::Entry, e2::Entry) = e1.key == e2.key

include("bloom_filter.jl")
include("run.jl")
include("buffer.jl")

mutable struct Level
    max_run::Integer
    max_run_size::Integer
    runs::Vector

    Level(n::Integer, s::Integer) = new(n, s, Vector{Run}())
end

remaining(l::Level) = l.max_run - length(l.runs) != 0

mutable struct LSM{K<:Unsigned, V}
    buffer::Buffer
    levels::Vector{Level}

    function LSM{K, V}(buffer_max_entries::Integer, depth::Integer, fanout::Integer) where {K, V}
        max_run_size = buffer_max_entries
        
        levels = Vector{Level}()
        while depth > 1
            push!(levels, Level(fanout, max_run_size))
            max_run_size *= fanout
            depth = depth - 1
        end

        new{K, V}(Buffer(buffer_max_entries), levels)
    end
end

function merge_down!(levels, i) 
    if remaining(levels[i]) > 0
        return
    else i == length(levels)
        error("No more space in tree.")
    end 

    if remaining(levels[i + 1]) == 0
        merge_down!(levels, i + 1)
    end

    current = levels[i]
    next = levels[i + 1]
    
    r = Run(next.max_run_size)
    push!(next.runs, r)

    entries = SortedSet{Entry}()
    for run in current.runs
        run_entries = read(run)
        push!(entries, read(run)...)
    end

    for e in entries
        if (i == length(levels) - 1) && ismissing(e.val) continue end
        insert!(r, e)
    end
    write!(r)

    current.runs = Vector{Run}()

end

function Base.push!(t::LSM, key, val)
    if (push!(t.buffer, key, val)) 
        return 
    end
    
    merge_down!(t.levels, 1)

    r = Run(t.levels[1].max_run_size)
    push!(t.levels[1].runs, r)
    for e in t.buffer.entries
        if (2 == length(t.levels)) && ismissing(e.val) continue end
        insert!(r, e)
    end
    write!(r)

    empty!(t.buffer)
    push!(t.buffer, key, val)

end

function Base.get(t::LSM, key) 
    key = unsigned(key)

    val = get(t.buffer, key)
    if val == missing 
        return nothing
    else val != nothing 
        return val
    end

    for l in t.levels
        for r in l.runs
            val = get(r, key)
            if val == missing
                return nothing
            else val != nothing 
                return val
            end
        end
    end

end

delete!(t::LSM, key) = push!(t, key, missing)

end