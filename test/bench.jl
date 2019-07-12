number = 10000000

function write_seq(s::LSMTree.Store{Int32, Int32}, vals::Vector{Tuple{Int32, Int32}})
    for i in 1:number
        s[i] = first(vals[i])
    end
    println("$number record added")
end

function write_random(s::LSMTree.Store{Int32, Int32}, vals::Vector{Tuple{Int32, Int32}}) 
    for i in 1:number
        val = vals[i]
        s[first(val)] = last(val)
    end
    println("$number record added")
end

function read_seq(s::LSMTree.Store{Int32, Int32})
    iter = LSMTree.Iterator(s)
    state = LSMTree.iter_init(iter)
    i = 0
    while !LSMTree.iter_done(iter, state)
        (element, state) = LSMTree.iter_next(iter, state)
        i += 1
    end
    println("$i record checked")
end

function read_random(s::LSMTree.Store{Int32, Int32}, randoms::Vector{Tuple{Int32, Int32}})
    for i in 1:length(randoms)
        s[first(randoms[i])]
    end
    println("$(length(randoms)) record checked")
end