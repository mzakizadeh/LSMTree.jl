module TestLSMTree
using LSMTree, Test, Random, Blobs

# Test different types of key & value
s1 = Store{Int64, Int64}()
put!(s1, rand(Int), rand(Int))

# AssertionError: These blobs do not share the same allocation
# s2 = Store{Int64, BlobString}()
# bs = Blobs.malloc_and_init(BlobString, randstring())
# put!(s2, 1, bs[])

s3 = Store{Int64, Tuple{Int64, Int64}}()
k, v = rand(Int), (rand(Int), rand(Int))
put!(s3, k, v)
@test get(s3, k) == v

# AssertionError: must be isbitstype
# s4 = Store{Int64, Tuple{Int64, String, String}}()
# put!(s4, 1, (rand(Int), randstring(), randstring()))

s5 = Store{Int8, Int16}()
k, v = rand(Int8), rand(Int16)
put!(s5, k, v)
@test get(s5, k) == v

# test custom key type
struct Foo
    x::Int64
    y::Int64
end
Base.isless(f1::Foo, f2::Foo) = f1.y < f2.y
s6 = Store{Foo, Int64}()
k, v = Foo(rand(Int64), rand(Int64)), rand(Int64)
put!(s6, k, v)
@test get(s6, k) == v

# Test some behaviors of tree
# transfrerring data to next level
s7 = Store{Int64, Int64}(80, 160, 2)
for i in 1:5
    put!(s7, i, i)
end
@test length(s7.buffer.entries) == 0
@test length(s7.levels[1]) == 5
for i in 6:10
    put!(s7, i , i)
end
@test length(s7.buffer.entries) == 0
@test length(s7.levels[1]) == 10
for i in 11:20
    put!(s7, i, i)
end
@test length(s7.buffer.entries) == 0
@test length(s7.levels[1]) == 10
@test length(s7.levels[2]) == 10
for i in 21:35
    put!(s7, i, i)
end
@test length(s7.buffer.entries) == 0
@test length(s7.levels[1]) == 5
@test length(s7.levels[2]) == 10
@test length(s7.levels[3]) == 20

# partitioning levels
s8 = Store{Int8, Int8}(20, 40, 2, 20)
for i in 1:20
    put!(s8, i, i)
end
@test length(s8.levels[1].tables) == 2
for i in 21:30
    put!(s8, i, i)
end
# test if tables can compact correcly in next level
@test length(s8.levels[1].tables) == 1
@test length(s8.levels[2].tables) == 2

# Test methods
# put (and update)
s9 = Store{Int64, Int64}(32, 64, 2)
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
@test s9.levels[1].size == 2 * 16

# delete
s10 = Store{Int64, Int64}(32, 64, 2)
put!(s10, 1, 1)
delete!(s10, 1)
@test get(s10, 1) == nothing
@test_throws AssertionError delete!(s10, 2)
# test force remove in last level
s11 = Store{Int64, Int64}(32, 64, 2)
put!(s11, 1, 1)
put!(s11, 2, 2)
put!(s11, 3, 3)
delete!(s11, 1)
@test s11.levels[1].size == 3 * 16
@test get(s11, 1) == nothing

# iteration
s12 = Store{Int128, Int128}(400000, 1000000, 4, 100000)
ints = []
for i in 1:100000
    x = rand(Int128)
    # x = i
    push!(ints, x)
    put!(s12, x, i)
end
sort!(ints)
state = iter_init(s12)
for i in 1:100000
    done, p = iter_next(s12, state)
    @test ints[i] == p[1]
    # @test i == 100000 && done
end

end