module LSMTree
using Blobs

function bsearch(v::Vector, l::Integer, r::Integer, k::K) where K
    while l <= r
        mid = floor(Int, l + (r - l) / 2)
        if v[mid].key[] == k
            return mid
        elseif v[mid].key[] < k 
            l = mid + 1
        else
            r = mid - 1
        end
    end
    return 0
end

function lub(v::Vector, lo::Integer, hi::Integer, k::K) where K
    p = i -> k < v[i][].key
    while lo < hi
        mid::Integer = floor(lo + (hi-lo) / 2)
        if p(mid)
            hi = mid
        else
            lo = mid + 1
        end
    end
    !p(lo) && return length(v)
    lo
end

struct Entry{K, V}
    key::K
    val::V
    deleted::Bool
end

isdeleted(e::Entry) = e.deleted
Base.sizeof(e::Blob{Entry{K, V}}) where {K, V} = sizeof(K) + sizeof(V)
Base.isless(e1::Entry, e2::Entry) = e1.key < e2.key
Base.isequal(e1::Entry, e2::Entry) = e1.key == e2.key

function Base.sizeof(v::Vector{Blob{Entry{K, V}}}) where {K, V}
    size = 0
    for i in v size += Blobs.sizeof(i) end
    size
end

include("bloom_filter.jl")
include("table.jl")
include("buffer.jl")
include("level.jl")
include("store.jl")

export BaseStore, 
    Store, 
    get, 
    put!,
    delete!
    # iter_init, 
    # iter_next, 
    # seek_lub_search
    
end