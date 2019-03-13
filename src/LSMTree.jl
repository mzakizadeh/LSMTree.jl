module LSMTree

using Blobs

function bsearch(bv::Vector, l::Integer, r::Integer, k::K) where K
    while l <= r
        mid = floor(Int, l + (r - l) / 2)
        if bv[mid].key[] == k
            return mid
        elseif bv[mid].key[] < k 
            l = mid + 1
        else
            r = mid - 1
        end
    end
    return 0
end

struct Entry{K, V}
    key::K
    val::V
    deleted::Bool
    Entry{K, V}(k, v, deleted=false) where {K, V} = new{K, V}(k, v, deleted)
end

isdeleted(e::Entry) = e.deleted
Base.isless(e1::Entry, e2::Entry) = e1.key < e2.key
Base.isequal(e1::Entry, e2::Entry) = e1.key == e2.key

include("bloom_filter.jl")
include("table.jl")
include("buffer.jl")
include("level.jl")
include("leveled_tree.jl")

export LeveledTree, insert!, get, delete!

end