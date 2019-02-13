mutable struct Run
    # bloom_filter::BloomFilter
    file_name::String
    max_size::Integer
    memmory::Vector

    Run(max_size::Integer) = new(
        # BloomFilter(Integer(max_size * bf_bits_per_entry)),
        joinpath(@__DIR__, "..", "tmp", randstring()),
        max_size,
        Vector{Entry}()
    )
end

function write!(r::Run)
    @save r.file_name r.memmory
    r.memmory = []
end

function read(r::Run)
    result = Vector{Entry}()
    jldopen(r.file_name, "r") do f
        result = read(f, "r.memmory")
    end
    return result
end

function Base.insert!(r::Run, e::Entry) 
    # set(r.bloom_filter, e.key)
    push!(r.memmory, e)
end

function Base.get(r::Run, k::K) where K 
    # if !isset(r.bloom_filter, k) return end

    jldopen(r.file_name, "r") do f
        r.memmory = read(f, "r.memmory")
    end

    for e in r.memmory
        if e.key == k
            return e.val
        end
    end

    return nothing
    
end