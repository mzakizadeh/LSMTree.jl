struct Entry{K, V}
    key::K
    val::V
    deleted::Bool
end

isdeleted(e::Entry) = e.deleted
Base.isless(e1::Entry, e2::Entry) = e1.key < e2.key
Base.isequal(e1::Entry, e2::Entry) = e1.key == e2.key
