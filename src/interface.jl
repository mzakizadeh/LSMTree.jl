abstract type Page end

struct MemoryPage <: Page
   _ptr::Ptr
end

function malloc_page(::Type{T}, size::Int)::T where {T <: Page}
    error("Not implemented")
end

malloc_page(::Type{MemoryPage}, size::Int)::MemoryPage = Libc.malloc(size)

function free_page(page::T) where {T <: Page}
    error("Not implemented")
end

free_page(page::MemoryPage) = Libc.free(page._ptr)

abstract type PageHandle end

struct FilePageHandle <: PageHandle
    id:String
    stream::IOStream
end

function open_pagehandle(::Type{T}, 
                         id::AbstractString; 
                         keywords...)::T where {T <: PageHandle}
    error("Not implemented")
end

open_pagehandle(::Type{FilePageHandle}, 
                id::AbstractString; 
                keywords...)::FilePageHandle = open(id, keywords...)

function close_pagehandle(phandle::T) where {T <: PageHandle}
    error("Not implemented")
end

close_pagehandle(phandle::FilePageHandle) = close(phandle)

function write_pagehandle(phandle::T, 
                          page::P, 
                          nbytes::UInt) where {T <: PageHandle, P <: Page}
    error("Not implemented")
end

write_pagehandle(phandle::FilePageHandle, 
                 page::MemoryPage, 
                 nbytes::UInt) = unsafe_write(phandle.stream, page._ptr, nbytes)

function read_pagehandle(phandle::T,
                         page::P,
                         nbytes::UInt) where {T <: PageHandle, P <: Page}
    error("Not implemented")
end

read_pagehandle(phandle::FilePageHandle,
                page::MemoryPage,
                nbytes::UInt) = unsafe_read(phandle.stream, page._ptr, nbytes)

function delete_pagehandle(::Type{T}, 
                           id::AbstractString; 
                           keywords...)::T where {T <: PageHandle}
    error("Not implemented")
end

delete_pagehandle(::Type{FilePageHandle}, 
                  id::AbstractString; 
                  force::Bool=false, 
                  recursive::Bool=false) = rm(id, force=force, recursive=recursive)