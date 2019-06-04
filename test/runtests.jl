module TestLSMTree
using LSMTree, Test, Random, Blobs

# Table

# test merge function 

# testcase 1
ids = [1, 2, 3, 4]
tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
for i in ids 
    x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
    x.key[] = i
    x.val[] = 2 * i
    push!(tmp, x)
end
t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
ids = [5, 6, 7, 8]
e = Blobs.malloc_and_init(BlobVector{LSMTree.Entry{Int64, Int64}}, 5)
for i in 1:length(ids)
    e[i].key[] = ids[i]
    e[i].val[] = ids[i]
end

t2 = merge(t, e, 1, 4, false)
@test length(t2[]) == 8
@test t2.entries[1].val[] == 2
@test t2.entries[5].val[] == 5


# testcase 2
ids = [5, 6, 7, 8]
tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
for i in ids 
    x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
    x.key[] = i
    x.val[] = 2 * i
    push!(tmp, x)
end
t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
ids = [1, 2, 3, 4]
e = Blobs.malloc_and_init(BlobVector{LSMTree.Entry{Int64, Int64}}, 5)
for i in 1:length(ids)
    e[i].key[] = ids[i]
    e[i].val[] = ids[i]
end

t2 = merge(t, e, 1, 4, false)
@test length(t2[]) == 8
@test t2.entries[1].val[] == 1
@test t2.entries[5].val[] == 10


# testcase 3
ids = [1, 2 ,3, 4]
tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
for i in ids 
    x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
    x.key[] = i
    x.val[] = 2 * i
    push!(tmp, x)
end
t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
ids = [3, 4, 5, 6]
e = Blobs.malloc_and_init(BlobVector{LSMTree.Entry{Int64, Int64}}, 5)
for i in 1:length(ids)
    e[i].key[] = ids[i]
    e[i].val[] = ids[i]
end

t2 = merge(t, e, 1, 4, false)
@test length(t2[]) == 6
@test t2.entries[1].val[] == 2
@test t2.entries[3].val[] == 3


# testcase 4
ids = [1, 2 ,3, 4]
tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
for i in ids 
    x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
    x.key[] = i
    x.val[] = 2 * i
    x.deleted[] = false
    push!(tmp, x)
end
t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)
ids = [3, 4, 5, 6]
e = Blobs.malloc_and_init(BlobVector{LSMTree.Entry{Int64, Int64}}, 5)
for i in 1:length(ids)
    e[i].key[] = ids[i]
    e[i].val[] = ids[i]
    e[i].deleted[] = false
end
e[1].deleted[] = true

t2 = merge(t, e, 1, 4, true)
@test length(t2[]) == 5
@test t2.entries[1].val[] == 2
@test t2.entries[3].val[] == 4


# test split function

ids = [1, 2, 3, 4, 5, 6, 7]
tmp = Vector{Blob{LSMTree.Entry{Int64, Int64}}}()
for i in ids 
    x = Blobs.malloc_and_init(LSMTree.Entry{Int64, Int64})
    x.key[] = i
    x.val[] = 2 * i
    push!(tmp, x)
end
t = Blobs.malloc_and_init(LSMTree.Table{Int64, Int64}, tmp)

r = LSMTree.split(t)
@test length(r[1][]) == 3
@test length(r[2][]) == 4

end