struct StoreData{K, V}
    first_level::Int64
    fanout::Int64
    first_level_max_size::Int64
    table_threshold_size::Int64
end

Blobs.child_size(::Type{StoreData{K, V}}, 
                 fanout::Int64, 
                 first_level_max_size::Int64, 
                 table_threshold_size::Int64) where {K, V} = 0

function Blobs.init(bf::Blob{StoreData{K, V}}, 
                    free::Blob{Nothing},
                    fanout::Int64, 
                    first_level_max_size::Int64, 
                    table_threshold_size::Int64) where {K, V}
    bf.first_level[] = -1
    bf.fanout[] = fanout
    bf.first_level_max_size[] = first_level_max_size
    bf.table_threshold_size[] = table_threshold_size
    free
end

function Blobs.malloc_and_init(::Type{StoreData}, args...)::Blob{StoreData}
    size = Blobs.self_size(StoreData) + child_size(StoreData, args...)
    page = malloc_page(MemoryPage, size)
    blob = Blob{StoreData}(page.ptr, 0, size)
    used = init(blob, args...)
    @assert used - blob == size
    blob
end
