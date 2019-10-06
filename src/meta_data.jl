abstract type AbstractMetaData{K} end

struct BlobMetaData{K}
    next_table_id::Int64
    next_level_id::Int64
    levels_min::BlobVector{Tuple{Int64, K}}
    levels_max::BlobVector{Tuple{Int64, K}}
end

function Blobs.child_size(::Type{BlobMetaData{K}}, meta::AbstractMetaData{K}) where K
    T = BlobMetaData{K}
    len = length(meta.levels_min)
    +(Blobs.child_size(fieldtype(T, :levels_min), len),
      Blobs.child_size(fieldtype(T, :levels_max), len))
end

function Blobs.init(blob::Blob{BlobMetaData{K}}, 
                    free::Blob{Nothing}, 
                    meta::AbstractMetaData{K}) where {K}
    blob.next_table_id[] = 1
    blob.next_level_id[] = 1
    levels_min = collect(meta.levels_min)
    levels_max = collect(meta.levels_max)
    @assert length(levels_min) == length(levels_max)
    len = length(levels_min)
    free = Blobs.init(blob.levels_min, free, len)
    free = Blobs.init(blob.levels_max, free, len)
    for i in 1:len
        blob.levels_min[i][] = (first(levels_min[i]), last(levels_min[i]))
        blob.levels_max[i][] = (first(levels_max[i]), last(levels_max[i]))
    end
    free
end

function malloc_and_init(::Type{BlobMetaData{K}}, 
                         s::AbstractStore{K, <:Any, PAGE, <:Any}, 
                         args...) where {K, PAGE}
    T = BlobMetaData{K}
    size = Blobs.self_size(T) + Blobs.child_size(T, args...)
    page = malloc_page(PAGE, size)
    blob = Blob{T}(pointer(page), 0, size)
    used = Blobs.init(blob, args...)
    @assert used - blob == size
    blob, page
end

mutable struct MetaData{K} <: AbstractMetaData{K}
    next_table_id::Int64
    next_level_id::Int64
    levels_min::Dict{Int64, K}
    levels_max::Dict{Int64, K}
    levels_bf::Dict{Int64, BloomFilter}
    function MetaData{K}(::Type{PAGE},
                         ::Type{PAGE_HANDLE},
                         meta::BlobMetaData{K},
                         path::String) where {K, PAGE, PAGE_HANDLE}
        levels_bf = Dict{Int64, BloomFilter}()
        levels_min = Dict{Int64, K}()
        levels_max = Dict{Int64, K}()
        levels_id = Vector{Int64}()
        for (id, min) in meta.levels_min 
            levels_min[id] = min
            push!(levels_id, id)
        end
        for (id, max) in meta.levels_max levels_max[id] = max end
        # read BloomFilters 
        for id in levels_id
            fpath = "$path/$id.bf"
            if isfile_pagehandle(PAGE_HANDLE, fpath)
                f = open_pagehandle(PAGE_HANDLE, fpath)
                size = size_pagehandle(f)
                page = malloc_page(PAGE, size)
                blob = Blob{BlobBloomFilter}(pointer(page), 0, size)
                read_pagehandle(f, page, size)
                close_pagehandle(f)
                levels_bf[id] = BloomFilter(blob[])
                free_page(page)
            else error("cannot find bloom filter with path:$fpath") end
        end
        # create MetaData
        return new{K}(meta.next_table_id, 
                      meta.next_level_id, 
                      levels_min, 
                      levels_max,
                      levels_bf)
    end
    MetaData{K}() where K = new{K}(1, 1, 
                                   Dict{Int64, K}(), 
                                   Dict{Int64, K}(),
                                   Dict{Int64, BloomFilter}())
end

function save_meta(s::AbstractStore{K, <:Any, PAGE, PAGE_HANDLE}) where {K, PAGE, PAGE_HANDLE}
    # save bloom filters 
    levels_id = collect(keys(s.meta.levels_bf))
    for id in levels_id
        path = "$(s.path)/$id.bf"
        bf, page = malloc_and_init(BlobBloomFilter, s, s.meta.levels_bf[id])
        file = open_pagehandle(PAGE_HANDLE, path, truncate=true, read=true)
        write_pagehandle(file, page, getfield(bf, :limit))
        close_pagehandle(file)
        free_page(page)
    end
    # save actual meta data
    path = "$(s.path)/.meta"
    meta, page = malloc_and_init(BlobMetaData{K}, s, s.meta)
    file = open_pagehandle(PAGE_HANDLE, path, truncate=true, read=true)
    write_pagehandle(file, page, getfield(meta, :limit))
    close_pagehandle(file)
    free_page(page)
end

function load_meta(::Type{K}, 
                   ::Type{PAGE}, 
                   ::Type{PAGE_HANDLE}, 
                   path::String) where {K, PAGE, PAGE_HANDLE}
    fpath = "$path/.meta"
    if isfile_pagehandle(PAGE_HANDLE, fpath)
        f = open_pagehandle(PAGE_HANDLE, fpath)
        size = size_pagehandle(f)
        page = malloc_page(PAGE, size)
        blob = Blob{BlobMetaData{K}}(pointer(page), 0, size)
        read_pagehandle(f, page, size)
        close_pagehandle(f)
        meta = MetaData{K}(PAGE, PAGE_HANDLE, blob[], path)
        free_page(page)
        return meta
    end
    nothing
end
