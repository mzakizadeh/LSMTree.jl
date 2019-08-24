mutable struct InMemoryData
    tables::Dict{Int64, Blob}
    levels::Dict{Int64, Blob}
    table_pages::Dict{Int64, Page}
    level_pages::Dict{Int64, Page}
    store_pages::Dict{Int64, Page}
    tables_queue::Vector{Int64}
    table_ids_inuse::Vector{Int64}
    level_ids_inuse::Vector{Int64}
    store_ids_inuse::Vector{Int64}
    InMemoryData(path) = new(Dict{Int64, Blob}(), 
                             Dict{Int64, Blob}(),
                             Dict{Int64, Page}(),
                             Dict{Int64, Page}(),
                             Dict{Int64, Page}(),
                             Vector{Int64}(), 
                             Vector{Int64}(),
                             Vector{Int64}(),
                             Vector{Int64}())
end
