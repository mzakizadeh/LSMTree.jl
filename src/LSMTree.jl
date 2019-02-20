module LSMTree

using Blobs

function bsearch(bv::BlobVector, l::Integer, r::Integer, k::K) where K
    while l <= r
        mid = convert(Int, floor(l + (r - l) / 2))
        if bv[mid].key == k
            return mid
        elseif bv[mid].key < k 
            l = mid + 1
        else
            r = mid - 1
        end
    end
    return 0
end

include("entry.jl")
include("buffer.jl")
include("bloom_filter.jl")
include("level.jl")
include("lsm_tree.jl")

export LSM, insert!, get, delete!

end