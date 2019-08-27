struct MetaData
    next_table_id::Int64
    next_level_id::Int64
end

Blobs.child_size(::Type{MetaData}) = 0

function Blobs.init(blob::Blob{MetaData}, free::Blob{Nothing})
    blob.next_table_id[] = 1
    blob.next_level_id[] = 1
    free
end

function malloc_and_init(::Type{MetaData}, 
                         s::AbstractStore{<:Any, <:Any, PAGE, <:Any}, 
                         args...) where PAGE
    size = Blobs.self_size(MetaData) + Blobs.child_size(MetaData, args...)
    page = malloc_page(PAGE, size)
    blob = Blob{MetaData}(pointer(page), 0, size)
    used = Blobs.init(blob, args...)
    @assert used - blob == size
    blob, page
end

function save_meta(meta::Blob{MetaData},
                   page:: PAGE,
                   s::AbstractStore{<:Any, <:Any, PAGE, PAGE_HANDLE}) where {PAGE, PAGE_HANDLE}
    path = "$(s.path)/.meta"
    file = open_pagehandle(PAGE_HANDLE, path, truncate=true, read=true)
    write_pagehandle(file, page, getfield(meta, :limit))
    close_pagehandle(file)
end

function load_meta(s::AbstractStore{<:Any, <:Any, PAGE, PAGE_HANDLE}) where {PAGE, PAGE_HANDLE}
    path = "$(s.path)/.meta"
    if isfile_pagehandle(PAGE_HANDLE, path)
        f = open_pagehandle(PAGE_HANDLE, path)
        size = size_pagehandle(f)
        page = malloc_page(PAGE, size)
        blob = Blob{MetaData}(pointer(page), 0, size)
        read_pagehandle(f, page, size)
        close_pagehandle(f)
        return blob, page
    end
    return malloc_and_init(MetaData, s)
end
