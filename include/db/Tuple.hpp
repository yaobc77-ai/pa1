#pragma once

#include <db/types.hpp>
#include <string>
#include <vector>
#include <unordered_map>
#include <variant>
#include <cstdint>

namespace db {

    class Tuple {
        std::vector<field_t> fields_;

    public:
        // 允许用花括号/向量隐式构造（配合测试用例）
        Tuple(const std::vector<field_t>& fields);

        type_t         field_type(size_t i) const;
        size_t         size() const;
        const field_t& get_field(size_t i) const;
    };

    // ---------------- TupleDesc ----------------
    class TupleDesc {
    private:
        std::vector<type_t>      types_;
        std::vector<std::string> names_;
        std::vector<size_t>      offsets_;
        size_t                   length_{0};
        std::unordered_map<std::string, size_t> name2idx_;

    public:
        TupleDesc() = default;

        /**
         * @brief Construct a new Tuple Desc object
         * @details Construct a new TupleDesc object with the provided types and names
         * @param types  the types of the fields
         * @param names  the names of the fields
         * @throws std::logic_error if types and names have different lengths
         * @throws std::logic_error if names are not unique
         */
        TupleDesc(const std::vector<type_t>& types,
                  const std::vector<std::string>& names);

        /// A Tuple is compatible if it has the same number of fields and matching types.
        bool   compatible(const Tuple& tuple) const;

        /// Byte offset of a field from the start of a serialized tuple.
        size_t offset_of(const size_t& index) const;

        /// Index of a field by name.
        size_t index_of(const std::string& name) const;

        /// Number of fields.
        size_t size() const;

        /// Total serialized byte length.
        size_t length() const;

        /// Serialize/deserialize a Tuple.
        void   serialize(uint8_t* data, const Tuple& t) const;
        Tuple  deserialize(const uint8_t* data) const;

        /// Merge two TupleDescs (td1 fields first, then td2).
        static TupleDesc merge(const TupleDesc& td1, const TupleDesc& td2);
    };

} // namespace db
