mutable struct BloomFilter
    table::Array

    BloomFilter(length::Int) = new(fill(false, length))
end

function hash_1(b::BloomFilter, k::UInt64)
    key = k
    key = ~key + (key<<15)
    key = key ^ (key>>12)
    key = key + (key<<2)
    key = key ^ (key>>4)
    key = key * 2057
    key = key ^ (key>>16)
    return key % length(b.table) + 1
end

function hash_2(b::BloomFilter, k::UInt64)
    key = k
    key = (key+0x7ed55d16) + (key<<12)
    key = (key^0xc761c23c) ^ (key>>19)
    key = (key+0x165667b1) + (key<<5)
    key = (key+0xd3a2646c) ^ (key<<9)
    key = (key+0xfd7046c5) + (key<<3)
    key = (key^0xb55a4f09) ^ (key>>16)
    return key % length(b.table) + 1
end

function hash_3(b::BloomFilter, k::UInt64)
    key = k
    key = (key^61) ^ (key>>16)
    key = key + (key<<3)
    key = key ^ (key>>4)
    key = key * 0x27d4eb2d
    key = key ^ (key>>15)
    return key % length(b.table) + 1
end

function set(b::BloomFilter, key::UInt64)
    b.table[hash_2(b, key)] = true;
    b.table[hash_3(b, key)] = true;
    b.table[hash_1(b, key)] = true;
end

function isset(b::BloomFilter, key::UInt64)
    return (b.table[hash_1(b, key)]
         && b.table[hash_2(b, key)]
         && b.table[hash_3(b, key)])
end