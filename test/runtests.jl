module TestLSMTree

using LSMTree
using Test

t = LeveledTree{Int64, Int64}(2, 2)
insert!(t, 1, 1) 
insert!(t, 2, 2) 
insert!(t, 3, 3)
@test t.buffer.size == 1

# t = LevelList{Int16, Int16}(2, 10)
# for i in 1:100
#     put!(t, i, i)
# end
# @test get(t, 1) == 1

end