abstract type Page end

struct MemoryPage <: Page
   _ptr::Ptr
end

Base.pointer(page::MemoryPage) = page._ptr

function malloc_page(::Type{T}, size::Int)::T where {T <: Page}
    error("Not implemented")
end

function malloc_page(::Type{MemoryPage}, size::Int)::MemoryPage
    ptr = Libc.malloc(size)
    page = MemoryPage(ptr)
    page
end

function free_page(page::T) where {T <: Page}
    error("Not implemented")
end

free_page(page::MemoryPage) = Libc.free(page._ptr)

abstract type PageHandle end

struct FilePageHandle
    id::String
    stream::IOStream
end

Base.size(f::FilePageHandle) = filesize(f.stream)

function open_pagehandle(::Type{T}, 
                         id::AbstractString; 
                         keywords...)::T where {T <: PageHandle}
    error("Not implemented")
end

function open_pagehandle(::Type{FilePageHandle}, 
                         id::AbstractString;
                         keywords...)::FilePageHandle
    stream = open(id; keywords...)
    pagehandle = FilePageHandle(id, stream)
    pagehandle
end

function close_pagehandle(phandle::T) where {T <: PageHandle}
    error("Not implemented")
end

close_pagehandle(phandle::FilePageHandle) = close(phandle.stream)

function write_pagehandle(phandle::T, 
                          page::P, 
                          nbytes::Int) where {T <: PageHandle, P <: Page}
    error("Not implemented")
end

write_pagehandle(phandle::FilePageHandle, 
                 page::MemoryPage, 
                 nbytes::Int) = unsafe_write(phandle.stream, page._ptr, nbytes)

function read_pagehandle(phandle::T,
                         page::P,
                         nbytes::Int) where {T <: PageHandle, P <: Page}
    error("Not implemented")
end

read_pagehandle(phandle::FilePageHandle,
                page::MemoryPage,
                nbytes::Int) = unsafe_read(phandle.stream, page._ptr, nbytes)

function delete_pagehandle(::Type{T}, 
                           id::AbstractString; 
                           keywords...)::T where {T <: PageHandle}
    error("Not implemented")
end

delete_pagehandle(::Type{FilePageHandle}, 
                  id::AbstractString; 
                  force::Bool=false, 
                  recursive::Bool=false) = rm(id, force=force, recursive=recursive)

function mkpath_pagehandle(::Type{T},
                           path::AbstractString,
                           mode::Unsigned=0o777) where {T <: PageHandle}
    error("Not implemented")
end

mkpath_pagehandle(::Type{FilePageHandle}, 
                  path::AbstractString;
                  mode::Unsigned=0o777) = Base.Filesystem.mkpath(path, mode=mode)

function isdir_pagehandle(::Type{T}, path)::Bool where {T <: PageHandle}
    error("Not implemented")
end

isdir_pagehandle(::Type{FilePageHandle}, path)::Bool = Base.Filesystem.isdir(path)

function isfile_pagehandle(::Type{T}, path)::Bool where {T <: PageHandle}
    error("Not implemented")
end

isfile_pagehandle(::Type{FilePageHandle}, path)::Bool = Base.Filesystem.isfile(path)
