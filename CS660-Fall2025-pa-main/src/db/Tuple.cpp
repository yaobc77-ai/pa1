#include <db/Tuple.hpp>
#include <cstring>
#include <stdexcept>
#include <cstdint>
#include <cstddef>
#include <unordered_set>

using namespace db;

// ---------------- Tuple ----------------
Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
    const field_t &f = fields.at(i);
    if (std::holds_alternative<int>(f))    return type_t::INT;
    if (std::holds_alternative<double>(f)) return type_t::DOUBLE;
    if (std::holds_alternative<std::string>(f)) return type_t::CHAR;
    throw std::logic_error("Tuple: unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

// ---------------- TupleDesc ----------------
TupleDesc::TupleDesc(const std::vector<type_t> &types,
                     const std::vector<std::string> &names)
{
    if (types.size() != names.size()) {
        throw std::logic_error("TupleDesc: types and names must have same length");
    }

    // 检查重名
    std::unordered_set<std::string> seen;
    for (const auto &n : names) {
        if (!seen.insert(n).second) {
            throw std::logic_error("TupleDesc: duplicate field name: " + n);
        }
    }

    types_   = types;
    names_   = names;
    offsets_.resize(types_.size());

    auto size_of = [](type_t t) -> size_t {
        switch (t) {
            case type_t::INT:    return INT_SIZE;
            case type_t::DOUBLE: return DOUBLE_SIZE;
            case type_t::CHAR:   return CHAR_SIZE; // 固定 64
        }
        throw std::logic_error("TupleDesc: unknown type");
    };

    size_t off = 0;
    for (size_t i = 0; i < types_.size(); ++i) {
        offsets_[i] = off;
        off += size_of(types_[i]);
        name2idx_.emplace(names_[i], i);
    }
    length_ = off;
}

bool TupleDesc::compatible(const Tuple &tuple) const {
    if (tuple.size() != types_.size()) return false;
    for (size_t i = 0; i < types_.size(); ++i) {
        if (tuple.field_type(i) != types_[i]) return false;
    }
    return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
    auto it = name2idx_.find(name);
    if (it == name2idx_.end()) {
        throw std::out_of_range("TupleDesc::index_of: field not found: " + name);
    }
    return it->second;
}

size_t TupleDesc::offset_of(const size_t &index) const {
    if (index >= offsets_.size()) {
        throw std::out_of_range("TupleDesc::offset_of: index out of range");
    }
    return offsets_[index];
}

size_t TupleDesc::length() const { return length_; }

size_t TupleDesc::size() const { return types_.size(); }

Tuple TupleDesc::deserialize(const uint8_t *data) const {
    std::vector<field_t> out;
    out.reserve(types_.size());

    for (size_t i = 0; i < types_.size(); ++i) {
        const uint8_t *ptr = data + offsets_[i];
        switch (types_[i]) {
            case type_t::INT: {
                int v;
                std::memcpy(&v, ptr, INT_SIZE);
                out.emplace_back(v);
                break;
            }
            case type_t::DOUBLE: {
                double v;
                std::memcpy(&v, ptr, DOUBLE_SIZE);
                out.emplace_back(v);
                break;
            }
            case type_t::CHAR: {
                const char *csrc = reinterpret_cast<const char *>(ptr);
                size_t len = 0;
                while (len < CHAR_SIZE && csrc[len] != '\0') ++len;
                out.emplace_back(std::string(csrc, len));
                break;
            }
        }
    }
    return Tuple(out);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
    if (!compatible(t)) {
        throw std::logic_error("TupleDesc::serialize: tuple incompatible with schema");
    }

    for (size_t i = 0; i < types_.size(); ++i) {
        uint8_t *ptr = data + offsets_[i];
        switch (types_[i]) {
            case type_t::INT: {
                const int &v = std::get<int>(t.get_field(i));
                std::memcpy(ptr, &v, INT_SIZE);
                break;
            }
            case type_t::DOUBLE: {
                const double &v = std::get<double>(t.get_field(i));
                std::memcpy(ptr, &v, DOUBLE_SIZE);
                break;
            }
            case type_t::CHAR: {
                const std::string &s = std::get<std::string>(t.get_field(i));
                size_t n = s.size();
                if (n > CHAR_SIZE) n = CHAR_SIZE;          // 超长截断
                std::memcpy(ptr, s.data(), n);
                if (n < CHAR_SIZE) {
                    std::memset(ptr + n, 0, CHAR_SIZE - n); // 不足补 0
                }
                break;
            }
        }
    }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
    std::vector<type_t> types;
    std::vector<std::string> names;
    types.reserve(td1.size() + td2.size());
    names.reserve(td1.size() + td2.size());

    for (size_t i = 0; i < td1.size(); ++i) {
        types.push_back(td1.types_[i]);
        names.push_back(td1.names_[i]);
    }
    for (size_t i = 0; i < td2.size(); ++i) {
        types.push_back(td2.types_[i]);
        names.push_back(td2.names_[i]);
    }
    return TupleDesc(types, names);
}
