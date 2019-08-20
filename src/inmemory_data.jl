struct InMemoryData{PAGE, PAGE_HANDLE}
    path::String
    level_pages::Dict{Int64, PAGE} # Level pages
    table_pages::Dict{Int64, PAGE}
    tables_queue::Vector{Int64}
    inmemory_tables::Dict{Int64, Blob}
    inmemory_levels::Dict{Int64, Blob}
    InMemoryData{PAGE, PAGE_HANDLE}(path) where {PAGE, PAGE_HANDLE} = 
        new{PAGE, PAGE_HANDLE}(path, 
                               Dict{Int64, PAGE}(),
                               Dict{Int64, PAGE}(),
                               Vector{Int64}(), 
                               Dict{Int64, Blob}(), 
                               Dict{Int64, Blob}())
end
