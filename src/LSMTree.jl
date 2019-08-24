module LSMTree
using Blobs

abstract type AbstractStore{K, V, PAGE, PAGE_HANDLE} end

include("interface.jl")
include("utils.jl")
include("entry.jl")
include("inmemory_data.jl")
include("meta_data.jl")
include("bloom_filter.jl")
include("table.jl")
include("buffer.jl")
include("level.jl")
include("store_data.jl")
include("store.jl")
include("iterator.jl")

function Base.get(s::AbstractStore{K, V}, key) where {K, V}
    key = convert(K, key) 
    result = get(s.buffer, key)
    if result !== nothing 
        result.deleted && return nothing
        return result.val
    end
    l = get_level(s.data.first_level[], s)
    while l !== nothing
        result = get(l[], key, s)
        if result !== nothing
            result.deleted && return nothing
            return result.val
        end
        l = get_level(l.next_level[], s)
    end
    nothing
end

function Base.put!(s::AbstractStore{K, V}, key, val, deleted=false) where {K, V}
    key = convert(K, key)
    val = convert(V, val)
    put!(s.buffer, key, val, deleted)
    isfull(s.buffer) && buffer_dump(s)
end

Base.setindex!(s::AbstractStore{K, V}, val, key) where {K, V} = put!(s, key, val)
Base.getindex(s::AbstractStore{K, V}, key) where {K, V} = get(s, key)

# TODO delete key without getting the value
function Base.delete!(s::AbstractStore, key)
    val = get(s, key)
    put!(s, key, val, true)
end

function Base.close(s::AbstractStore{K, V}) where {K, V}
    buffer_dump(s)
    # The id of first level is always unique
    # Therefore we also used it as store id
    path = "$(s.path)/$(s.data.first_level[]).str"
    file = open_pagehandle(FilePageHandle, 
                           path, 
                           truncate=true, 
                           read=true)
    write_pagehandle(file, s.data_page, getfield(s.data, :limit))
    close_pagehandle(file)
    @info "LSMTree.Store{$K, $V} with id $(s.data.first_level[]) closed"
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
        f = open_pagehandle(PAGE_HANDLE, file)
        size = filesize(f)
        page = malloc_page(PAGE, size)
        blob = Blob{Level{K, V}}(pointer(page), 0, size)
        read_pagehandle(PAGE_HANDLE, page, size)
        s = Store{K, V}(path, blob, page)
        push!(s.inmemory.store_ids_inuse, id)
        close_pagehandle(f)
        return s
    end
    nothing
end

function restore(::Type{K}, 
                 ::Type{V}, 
                 path::String, 
                 id::Int64) where {K, V}
    file = "$path/$id.str"
    if isfile_pagehandle(FilePageHandle, file)
        f = open_pagehandle(FilePageHandle, file)
        size = filesize(f.stream)
        page = malloc_page(MemoryPage, size)
        blob = Blob{StoreData{K, V}}(pointer(page), 0, size)
        read_pagehandle(f, page, size)
        s = Store{K, V}(path, blob, page)
        push!(s.inmemory.store_ids_inuse, id)
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
