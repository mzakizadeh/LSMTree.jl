module TestLSMTree

using LSMTree
using Test

# t = LSM{Int16, Int16}(2, 3, 3)
# insert!(t, 1, 1) 
# insert!(t, 2, 2) 
# insert!(t, 3, 3)
# @test t.buffer.size == 1

t = LSM{Int16, Int16}(2, 10, 2)
for i in 1:100
    insert!(t, i, i)
end
@test get(t, 1) == 1

end