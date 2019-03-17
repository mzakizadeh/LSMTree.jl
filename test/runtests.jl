module TestLSMTree
using LSMTree, Test, Random, Blobs

# Test different types of key & value
s1 = Store{Int64, Int64}(2, 2)
put!(s1, rand(Int), rand(Int))

# AssertionError: These blobs do not share the same allocation
# s2 = Store{Int64, BlobString}(2, 2)
# bs = Blobs.malloc_and_init(BlobString, randstring())
# put!(s2, 1, bs[])

s3 = Store{Int64, Tuple{Int64, Int64}}(2, 2)
k, v = rand(Int), (rand(Int), rand(Int))
put!(s3, k, v)
@test get(s3, k) == v

# AssertionError: must be isbitstype
# s4 = Store{Int64, Tuple{Int64, String, String}}(2, 2)
# put!(s4, 1, (rand(Int), randstring(), randstring()))

s5 = Store{Int8, Int16}(2, 2)
k, v = rand(Int8), rand(Int16)
put!(s5, k, v)
@test get(s5, k) == v

# test custom key type
struct Foo
    x::Int64
    y::Int64
end
Base.isless(f1::Foo, f2::Foo) = f1.y < f2.y
s6 = Store{Foo, Int64}(2, 2)
k, v = Foo(rand(Int64), rand(Int64)), rand(Int64)
put!(s6, k, v)
@test get(s6, k) == v

# Test some behaviors of tree
# transfrerring data to next level
s7 = Store{Int64, Int64}(5, 2)
for i in 1:5
    put!(s7, i, i)
end
@test length(s7.buffer.entries) == 0
@test s7.levels[1].size == 5
for i in 6:10
    put!(s7, i , i)
end
@test length(s7.buffer.entries) == 0
@test s7.levels[1].size == 10
for i in 11:20
    put!(s7, i, i)
end
@test length(s7.buffer.entries) == 0
@test s7.levels[1].size == 10
@test s7.levels[2].size == 10
for i in 21:35
    put!(s7, i, i)
end
@test length(s7.buffer.entries) == 0
@test s7.levels[1].size == 5
@test s7.levels[2].size == 10
@test s7.levels[3].size == 20

# partitioning tables
# s8 = Store{Int64, String}(20000, 2)
# @assert 
# for i in 1:40000
#     put!(s8, i, randstring('a':'z', 92))
# end
# @test length(s8.levels[1].tables) == 2

# isbittype
# duplicate entries
# delete old data

# Test methods
# put (and update)
s9 = Store{Int64, Int64}(2, 2)
put!(s9, 1, 1)
put!(s9, 1, 2)
# test entries replacement in buffer
@test length(s9.buffer.entries) == 1
@test get(s9, 1) == 2
put!(s9, 2, 2)
put!(s9, 1, 3)
# test the hierarchy of get function
@test get(s9, 1) == 3
put!(s9, 2, 3)
# test entries replacement in levels 
@test s9.levels[1].size == 2

# delete
s10 = Store{Int64, Int64}(2, 2)
put!(s10, 1, 1)
delete!(s10, 1)
@test get(s10, 1) == nothing
@test_throws AssertionError delete!(s10, 2)
# test force remove in last level
s11 = Store{Int64, Int64}(2, 2)
put!(s11, 1, 1)
put!(s11, 2, 2)
put!(s11, 3, 3)
delete!(s11, 1)
@test s11.levels[1].size == 3
@test get(s11, 1) == nothing

# iteration
# s12 = Store{Int128, Int128}(1000000, 10)
# ints = []
# for i in 1:1000000
#     x = rand(Int128)
#     # x = i
#     push!(ints, x)
#     put!(s12, x, i)
# end
# sort!(ints)
# state = iter_init(s12)
# for i in 1:1000000
#     done, p = iter_next(s12, state)
#     @test ints[i] == p[1]
#     @test i == 1000000 && done
# end

end