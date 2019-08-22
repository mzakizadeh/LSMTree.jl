struct InMemoryData
    path::String
    tables_queue::Vector{Int64}
    inmemory_tables::Dict{Int64, Blob}
    inmemory_levels::Dict{Int64, Blob}
    InMemoryData(path) = new(path, 
                             Vector{Int64}(), 
                             Dict{Int64, Blob}(), 
                             Dict{Int64, Blob}())
end
