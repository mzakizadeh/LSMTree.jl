function bsearch(v::Union{Vector, BlobVector}, 
                 l::Integer, 
                 r::Integer, 
                 k::K) where K
    while l <= r
        mid = floor(Int, l + (r - l) / 2)
        if v[mid].key == k
            return mid
        elseif v[mid].key < k 
            l = mid + 1
        else
            r = mid - 1
        end
    end
    return 0
end

function lub(v::Union{Vector, BlobVector}, 
             lo::Integer, 
             hi::Integer, 
             k::K) where K
    p = i -> k < v[i].key
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

isnothing(::Any) = false
isnothing(::Nothing) = true
