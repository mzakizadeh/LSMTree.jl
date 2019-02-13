include("LSMTree.jl")

using .LSMTree

t = LSM{UInt16, Integer}(2, 3, 2)

push!(t, 1, 0)
push!(t, 2, 0)
push!(t, 3, 0)

Base.get(t, 3)