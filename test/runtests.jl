module TestLSMTree
using LSMTree, Test, Random, Blobs

# Clear the directory
function clear()
    rm("./db", force=true, recursive=true)
end

clear()

# include("bench.jl")

# Simple Put & Get
s = LSMTree.Store{Int32, Int32}(
    buffer_max_size=10, 
    table_threshold_size=10
)
for i in 1:100 s[i] = 2 * i + 3 end
# 10 + 20 + 40 + 80 = 150
# has 3 levels
# buffer is empty
# last level is not full (has 40 entires)
@test s.buffer.size == 0
level1st = LSMTree.get_level(s.data.first_level[], s)[]
@test level1st.max_size == level1st.size == 20
level2nd = LSMTree.get_level(level1st.next_level, s)[]
@test level2nd.max_size == level2nd.size == 40
level3rd = LSMTree.get_level(level2nd.next_level, s)[]
@test level3rd.max_size == 80
@test level3rd.size == 40
@test LSMTree.get_level(level3rd.next_level, s) === nothing

for i in 1:100 @test s[i] == 2 * i + 3 end # check data correctness

clear()

# Update & Override
s = LSMTree.Store{Int32, Int32}(
    buffer_max_size=10, 
    table_threshold_size=10
)
for i in 1:10 s[i] = i end
for i in 1:20 s[i] = 2 * i end
for i in 1:40 s[i] = 3 * i end
for i in 1:60 s[i] = 4 * i end

for i in 1:60 @test s[i] == 4 * i end
# length is not accurate in this state
# because the overriden data is not removed compeletely
@test length(s) != 60 

for i in 61:80 s[i] = 4 * i end

for i in 1:80 @test s[i] == 4 * i end
# data is now completely overriden
@test length(s) == 80

clear()

# Delete
s = LSMTree.Store{Int32, Int32}(
    buffer_max_size=10, 
    table_threshold_size=10
)
for i in 1:60 s[i] = i end
for i in 1:10 delete!(s, i) end

@test s.buffer.size == 0
@test length(s) != 60
for i in 1:10 @test s[i] === nothing end

for i in 61:100 s[i] = i end
@test length(s) == 90
for i in 1:10 @test s[i] === nothing end
for i in 11:100 @test s[i] == i end

clear()

# Iterate
randoms = Vector{Tuple{Int32, Int32}}()
for i in 1:1000 push!(randoms, (rand(Int32), i)) end

s = LSMTree.Store{Int32, Int32}(
    buffer_max_size=10, 
    table_threshold_size=10
)
for i in 1:1000 s[i] = first(randoms[i]) end

iter = LSMTree.Iterator(s)
let prev = nothing, c = 0
    for e in iter 
        c += 1
        @test prev === nothing || prev < e
        prev = e
    end
    @test c == length(randoms)
end

# Snapshot & Restore
close(s)
s = nothing 
s2 = LSMTree.restore(Int32, Int32, "./db", 1009)
for (v, k) in randoms @test s2[k] == v end

clear()

# Complex Types
struct Foo
    a::Int
    b::Float32
    c::Tuple{Int, Int}
end
Base.isless(f1::Foo, f2::Foo) = f1.a + first(f1.c) < f2.a + first(f2.c)

K = Int32
V = Foo
s = LSMTree.Store{K, V}(
    buffer_max_size=10, 
    table_threshold_size=10
)
for i in 1:100 
    s[i] = Foo(rand(Int), rand(Float32), (rand(Int), rand(Int)))
end

end