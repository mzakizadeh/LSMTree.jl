mutable struct BloomFilter{K}
    table::Array
    BloomFilter{K}(length::Int) where K = new{K}(fill(false, length))
end

hash_1(b::BloomFilter{K}, k::K) where K = convert(Int128, hash(k, UInt(1))) % length(b.table) + 1
hash_2(b::BloomFilter{K}, k::K) where K = convert(Int128, hash(k, UInt(2))) % length(b.table) + 1
hash_3(b::BloomFilter{K}, k::K) where K = convert(Int128, hash(k, UInt(3))) % length(b.table) + 1

function set(b::BloomFilter{K}, key::K) where K
    b.table[hash_2(b, key)] = true;
    b.table[hash_3(b, key)] = true;
    b.table[hash_1(b, key)] = true;
end

function isset(b::BloomFilter{K}, key::K) where K
    return (b.table[hash_1(b, key)]
         && b.table[hash_2(b, key)]
         && b.table[hash_3(b, key)])
end