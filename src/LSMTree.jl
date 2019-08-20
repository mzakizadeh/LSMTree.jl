module LSMTree
using Blobs

include("interface.jl")
include("utils.jl")
include("entry.jl")
include("inmemory_data.jl")
include("bloom_filter.jl")
include("table.jl")
include("buffer.jl")
include("level.jl")
include("store_data.jl")
include("store.jl")
include("iterator.jl")

function Base.get(s::Store{K, V}, key) where {K, V}
    key = convert(K, key) 
    result = get(s.buffer, key)
    if !isnothing(result) 
        result.deleted && return nothing
        return result.val
    end
    l = get_level(Level{K, V}, s.data.first_level[], s.inmemory)
    while !isnothing(l)
        result = get(l[], key, s.inmemory)
        if !isnothing(result)
            result.deleted && return nothing
            return result.val
        end
        l = get_level(Level{K, V}, l.next_level[], s.inmemory)
    end
    nothing
end

function Base.put!(s::Store{K, V}, key, val, deleted=false) where {K, V}
    key = convert(K, key)
    val = convert(V, val)
    put!(s.buffer, key, val, deleted)
    isfull(s.buffer) && buffer_dump(s)
end

Base.setindex!(s::Store{K, V}, val, key) where {K, V} = put!(s, key, val)
Base.getindex(s::Store{K, V}, key) where {K, V} = get(s, key)

# TODO delete key without getting the value
function Base.delete!(s::Store, key)
    val = get(s, key)
    put!(s, key, val, true)
end

function Base.close(s::Store{K, V}) where {K, V}
    buffer_dump(s)
    # The id of first level is always unique
    # Therefore we also used it as store id
    path = "$(s.inmemory.path)/$(s.data.first_level[]).str"
    file = open_pagehandle(FilePageHandle, 
                           path, 
                           pointer(s.data), 
                           truncate=true, 
                           read=true)
    write_pagehandle(file, file.page, getfield(s.data, :limit))
    close_pagehandle(file)
    print("LSMTree.Store{$K, $V} with id $(s.data.first_level[]) closed")
    Blobs.free(s.data)
end

function restore(::Type{K}, 
                 ::Type{V}, 
                 ::Type{PAGE},
                 ::Type{PAGE_HANDLE},
                 path::String, 
                 id::Int64) where {K, V, PAGE, PAGE_HANDLE}
    file = "$path/$id.str"
    if isfile_pagehandle(PAGE_HANDLE, file)
        s::Union{Nothing, Store} = nothing
        f = open_pagehandle(PAGE_HANDLE, file)
        size = filesize(f)
        page = malloc_page(PAGE, size)
        blob = Blob{Level{K, V}}(pointer(page), 0, size)
        read_pagehandle(PAGE_HANDLE, page, size)
        s = Store{K, V}(path, blob)
        close_pagehandle(f)
        return s
    end
    nothing
end

export get, 
       put!,
       delete!,
       snapshot,
       restore,
       iter_init, 
       iter_next 
    
end