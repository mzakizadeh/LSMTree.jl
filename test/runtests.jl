module TestLSMTree

using LSMTree
using Test

t = LSM{Int16, Int16}(2, 3, 3)
insert!(t, 1, 1) 
insert!(t, 2, 2) 
insert!(t, 3, 3)
@test length(t.buffer.entries) == 1

end