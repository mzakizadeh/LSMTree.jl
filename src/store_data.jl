struct StoreData{K, V}
    first_level::Int64
    fanout::Int64
    buffer_max_size::Int64
    table_threshold_size::Int64
end

Blobs.child_size(::Type{StoreData{K, V}}, 
                 fanout::Int64, 
                 buffer_max_size::Int64, 
                 table_threshold_size::Int64) where {K, V} = 0

function Blobs.init(blob::Blob{StoreData{K, V}}, 
                    free::Blob{Nothing},
                    fanout::Int64, 
                    buffer_max_size::Int64, 
                    table_threshold_size::Int64) where {K, V}
    blob.first_level[] = -1
    blob.fanout[] = fanout
    blob.buffer_max_size[] = buffer_max_size
    blob.table_threshold_size[] = table_threshold_size
    free
end

function malloc_and_init(::Type{StoreData{K, V}}, 
                         ::Type{PAGE},
                         args...) where {K, V, PAGE <: Page}
    size = Blobs.self_size(StoreData{K, V}) + Blobs.child_size(StoreData{K, V}, args...)
    page = malloc_page(PAGE, size)
    blob = Blob{StoreData{K, V}}(pointer(page), 0, size)
    used = Blobs.init(blob, args...)
    @assert used - blob == size
    blob, page
end
