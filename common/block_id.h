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

#ifndef wtf_common_block_id_h_
#define wtf_common_block_id_h_

// C
#include <stdint.h>

// C++
#include <iostream>

// e
#include <e/intrusive_ptr.h>
#include <e/buffer.h>
#include <e/endian.h>

namespace wtf
{

#define OPERATORDECL(OP) \
    bool \
    operator OP (const block_id& lhs, const block_id& rhs)
#define OPERATOR(OP) \
    inline OPERATORDECL(OP) \
    { \
        return lhs.m_sid OP rhs.m_sid \
                || (lhs.m_sid == rhs.m_sid \
                && lhs.m_bid OP rhs.m_bid); \
    }

    class block_id
    { 
        public:
            block_id();
            explicit block_id(uint64_t sid, uint64_t bid);
            ~block_id() throw();

        public:
            static uint64_t pack_size() { return 2 * sizeof(uint64_t); }
            void pack(char* buf) const;
        private:
            friend std::ostream&
                operator << (std::ostream& lhs, const block_id& rhs);
            friend e::buffer::packer
                operator << (e::buffer::packer pa, const block_id& rhs);
            friend e::unpacker
                operator >> (e::unpacker up, block_id& rhs);
            friend OPERATORDECL(<);
            friend OPERATORDECL(<=);
            friend OPERATORDECL(==); 
            friend OPERATORDECL(!=); 
            friend OPERATORDECL(>=); 
            friend OPERATORDECL(>);

        private: 
            uint64_t m_sid; 
            uint64_t m_bid; 
    }; 

    inline std::ostream& 
    operator << (std::ostream& lhs, const block_id& rhs) 
    { 
        return lhs << "block_id(server=" << rhs.m_sid << ", chunk=" << rhs.m_bid << ")"; 
    } 

    inline e::buffer::packer 
    operator << (e::buffer::packer pa, const block_id& rhs) 
    { 
        return pa << rhs.m_sid << rhs.m_bid; 
    } 

    inline e::unpacker 
    operator >> (e::unpacker up, block_id& rhs) 
    { 
        up = up >> rhs.m_sid >> rhs.m_bid; 
        e::unpack64be((uint8_t*)&rhs.m_sid, &rhs.m_sid);
        e::unpack64be((uint8_t*)&rhs.m_bid, &rhs.m_bid);
        return up; 
    } 

    OPERATOR(<) 
    OPERATOR(<=) 
    OPERATOR(==) 
    OPERATOR(!=) 
    OPERATOR(>=) 
    OPERATOR(>)
}
#endif /* wtf_common_block_id_h_ */
