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

# test using FixedSizeStrings in tree
# first you should get and use FixedSizeStrings.jl package
# using FixedSizeStrings
# s7 = Store{Int64, FixedSizeString{96}}()
# k, v = rand(Int64), randstring(96)
# put!(s7, k, v)
# @test get(s7, k) == v

# Test some behaviors of tree
# transfrerring data to next level
s8 = Store{Int64, Int64}(80, 160, 2)
for i in 1:5
    put!(s8, i, i)
end
@test length(s8.buffer.entries) == 0
@test length(s8.levels[1]) == 5
for i in 6:10
    put!(s8, i , i)
end
@test length(s8.buffer.entries) == 0
@test length(s8.levels[1]) == 10
for i in 11:20
    put!(s8, i, i)
end
@test length(s8.buffer.entries) == 0
@test length(s8.levels[1]) == 10
@test length(s8.levels[2]) == 10
for i in 21:35
    put!(s8, i, i)
end
@test length(s8.buffer.entries) == 0
@test length(s8.levels[1]) == 5
@test length(s8.levels[2]) == 10
@test length(s8.levels[3]) == 20

# partitioning levels
s9 = Store{Int8, Int8}(20, 40, 2, 20)
for i in 1:20
    put!(s9, i, i)
end
@test length(s9.levels[1].tables) == 2
for i in 21:30
    put!(s9, i, i)
end
# test if tables can compact correcly in next level
@test length(s9.levels[1].tables) == 1
@test length(s9.levels[2].tables) == 2

# Test methods
# put (and update)
s10 = Store{Int64, Int64}(32, 64, 2)
put!(s10, 1, 1)
put!(s10, 1, 2)
# test entries replacement in buffer
@test length(s10.buffer.entries) == 1
@test get(s10, 1) == 2
put!(s10, 2, 2)
put!(s10, 1, 3)
# test the hierarchy of get function
@test get(s10, 1) == 3
put!(s10, 2, 3)
# test entries replacement in levels 
@test s10.levels[1].size == 2 * 16

# delete
s11 = Store{Int64, Int64}(32, 64, 2)
put!(s11, 1, 1)
delete!(s11, 1)
@test get(s11, 1) == nothing
@test_throws AssertionError delete!(s11, 2)
# test force remove in last level
s12 = Store{Int64, Int64}(32, 64, 2)
put!(s12, 1, 1)
put!(s12, 2, 2)
put!(s12, 3, 3)
delete!(s12, 1)
@test s12.levels[1].size == 3 * 16
@test get(s12, 1) == nothing

# iteration
s13 = Store{Int32, Int32}(400, 1000, 4, 100)
for i in 1:100
    x = rand(Int32)
    put!(s13, x, i)
end
state = iter_init(s13)
@test length(s13) == 100
done, p = iter_next(state)
last = p
while true
    done, p = iter_next(state)
    @test last < p
    global last = p
    done && break
end

# seek_lub
s14 = Store{Int128, Int128}(40000, 100000, 400, 10000)
ints = []
for i in 1:100000
    x = rand(Int128)
    # x = i
    push!(ints, x)
    put!(s14, x, i)
end
@test length(s14) == 100000
sort!(ints)
state = iter_init(s14)
seek_lub_search(state, ints[50000])
done, p = iter_next(state)
last = p
while true
    done, p = iter_next(state)
    @test last < p
    global last = p
    done && break
end

end