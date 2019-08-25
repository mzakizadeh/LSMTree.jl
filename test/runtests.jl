module TestLSMTree
using LSMTree, Test, Random, Blobs

include("bench.jl")

randoms = Vector{Tuple{Int32, Int32}}()
for i in 1:number 
    push!(randoms, (rand(Int32), i))
end

# println("Write sequential")
# s = LSMTree.Store{Int32, Int32}()
# @time write_seq(s, randoms)
# delete!(s)

println("Write random")
s = LSMTree.Store{Int32, Int32}()
@time write_random(s, randoms)

# println("Read sequential")
# @time read_seq(s)

println("Read random")
randoms2 = shuffle(randoms)
@time read_random(s, randoms)
rm("./db", force=true, recursive=true)

# # Table

# # test merge function 

# # testcase 1
# ids = [1, 2, 3, 4]
# tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
# for i in ids 
#     x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
#     x.key[] = i
#     x.val[] = 2 * i
#     push!(tmp, x)
# end
# t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
# ids = [5, 6, 7, 8]
# e = Blobs.malloc_and_init(BlobVector{LSMTree.Entry{Int64, Int64}}, 5)
# for i in 1:length(ids)
#     e[i].key[] = ids[i]
#     e[i].val[] = ids[i]
# end

# t2 = merge(t, e, 0, 4, false)
# @test length(t2[]) == 8
# @test t2.entries[1].val[] == 2
# @test t2.entries[5].val[] == 5


# # testcase 2
# ids = [5, 6, 7, 8]
# tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
# for i in ids 
#     x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
#     x.key[] = i
#     x.val[] = 2 * i
#     push!(tmp, x)
# end
# t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
# ids = [1, 2, 3, 4]
# e = Blobs.malloc_and_init(BlobVector{LSMTree.Entry{Int64, Int64}}, 5)
# for i in 1:length(ids)
#     e[i].key[] = ids[i]
#     e[i].val[] = ids[i]
# end

# t2 = merge(t, e, 0, 4, false)
# @test length(t2[]) == 8
# @test t2.entries[1].val[] == 1
# @test t2.entries[5].val[] == 10


# # testcase 3
# ids = [1, 2 ,3, 4]
# tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
# for i in ids 
#     x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
#     x.key[] = i
#     x.val[] = 2 * i
#     push!(tmp, x)
# end
# t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
# ids = [3, 4, 5, 6]
# e = Blobs.malloc_and_init(BlobVector{LSMTree.Entry{Int64, Int64}}, 5)
# for i in 1:length(ids)
#     e[i].key[] = ids[i]
#     e[i].val[] = ids[i]
# end

# t2 = merge(t, e, 0, 4, false)
# @test length(t2[]) == 6
# @test t2.entries[1].val[] == 2
# @test t2.entries[3].val[] == 3


# # testcase 4
# ids = [1, 2 ,3, 4]
# tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
# for i in ids 
#     x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
#     x.key[] = i
#     x.val[] = 2 * i
#     x.deleted[] = false
#     push!(tmp, x)
# end
# t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
# ids = [3, 4, 5, 6]
# e = Blobs.malloc_and_init(BlobVector{LSMTree.Entry{Int64, Int64}}, 5)
# for i in 1:length(ids)
#     e[i].key[] = ids[i]
#     e[i].val[] = ids[i]
#     e[i].deleted[] = false
# end
# e[1].deleted[] = true

# t2 = merge(t, e, 0, 4, true)
# @test length(t2[]) == 5
# @test t2.entries[1].val[] == 2
# @test t2.entries[3].val[] == 4


# # test split function
# ids = [1, 2, 3, 4, 5, 6, 7]
# tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
# for i in ids 
#     x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
#     x.key[] = i
#     x.val[] = 2 * i
#     push!(tmp, x)
# end
# t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)

# r = LSMTree.split(t)
# @test length(r[1][]) == 3
# @test length(r[2][]) == 4


# # Level

# # test partition function 
# ids = [1, 1, 3, 3, 5, 10]
# e = Blobs.malloc_and_init(BlobVector{LSMTree.Entry{Int64, Int64}}, 6)
# for i in 1:length(ids)
#     e[i].key[] = ids[i]
#     e[i].val[] = ids[i]
# end
# ids = [2, 5, 7]
# b = Blobs.malloc_and_init(BlobVector{Int64}, 3)
# for i in 1:length(ids)
#     b[i][] = ids[i]
# end

# t = LSMTree.partition(b[], e[])
# @test t == [0, 2, 5, 5, 6]


# # test compact function
# ids = [1, 2, 3, 4]
# tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
# for i in ids 
#     x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
#     x.key[] = i
#     x.val[] = 2 * i
#     push!(tmp, x)
# end
# t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
# LSMTree.inmemory_tables[t.id[]] = t
# ids = [7, 8, 9, 10]
# tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
# for i in ids 
#     x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
#     x.key[] = i
#     x.val[] = 2 * i
#     push!(tmp, x)
# end
# t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
# LSMTree.inmemory_tables[t.id[]] = t

end