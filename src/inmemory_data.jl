struct InMemoryData{PAGE, PAGE_HANDLE}
    path::String
    table_pages::Dict{Int64, PAGE}
    level_pages::Dict{Int64, PAGE}
    store_pages::Dict{Int64, PAGE}
    tables_queue::Vector{Int64}
    inmemory_tables::Dict{Int64, Blob}
    inmemory_levels::Dict{Int64, Blob}
    tables_inuse::Vector{Int64}
    levels_inuse::Vector{Int64}
    stores_inuse::Vector{Int64}
    InMemoryData{PAGE, PAGE_HANDLE}(path) where {PAGE, PAGE_HANDLE} = 
        new{PAGE, PAGE_HANDLE}(path,
                               Dict{Int64, PAGE}(),
                               Dict{Int64, PAGE}(),
                               Dict{Int64, PAGE}(),
                               Vector{Int64}(), 
                               Dict{Int64, Blob}(), 
                               Dict{Int64, Blob}(),
                               Vector{Int64}(),
                               Vector{Int64}(),
                               Vector{Int64}())
end
