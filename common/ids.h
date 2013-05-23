// Copyright (c) 2013, Sean Ogden
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of WTF nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#ifndef wtf_common_ids_h_
#define wtf_common_ids_h_

// C
#include <stdint.h>

// C++
#include <iostream>

// e
#include <e/buffer.h>

// An ID is a simple wrapper around uint64_t in order to prevent devs from
// accidently using one type of ID as another.

#define OPERATOR(TYPE, OP) \
    inline bool \
    operator OP (const TYPE ## _id& lhs, const TYPE ## _id& rhs) \
    { \
        return lhs.get() OP rhs.get(); \
    }
#define CREATE_ID(TYPE) \
    class TYPE ## _id \
    { \
        public: \
            TYPE ## _id() : m_id(0) {} \
            explicit TYPE ## _id(uint64_t id) : m_id(id) {} \
        public: \
            uint64_t get() const { return m_id; } \
        private: \
            uint64_t m_id; \
    }; \
    inline std::ostream& \
    operator << (std::ostream& lhs, const TYPE ## _id& rhs) \
    { \
        return lhs << #TYPE "(" << rhs.get() << ")"; \
    } \
    inline e::buffer::packer \
    operator << (e::buffer::packer pa, const TYPE ## _id& rhs) \
    { \
        return pa << rhs.get(); \
    } \
    inline e::unpacker \
    operator >> (e::unpacker up, TYPE ## _id& rhs) \
    { \
        uint64_t id; \
        up = up >> id; \
        rhs = TYPE ## _id(id); \
        return up; \
    } \
    OPERATOR(TYPE, <) \
    OPERATOR(TYPE, <=) \
    OPERATOR(TYPE, ==) \
    OPERATOR(TYPE, !=) \
    OPERATOR(TYPE, >=) \
    OPERATOR(TYPE, >)

namespace wtf
{

CREATE_ID(server)

} // namespace wtf

#undef OPERATOR
#undef CREATE_ID
#endif // wtf_common_ids_h_
