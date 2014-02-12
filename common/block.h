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

#ifndef wtf_block_h_
#define wtf_block_h_

// e
#include <e/intrusive_ptr.h>

// STL
#include <vector>

//WTF
#include <client/client.h>
#include <common/block_location.h>

namespace wtf __attribute__ ((visibility("hidden")))
{

class block
{
    public:
        block(size_t block_length, size_t file_offset, size_t reps);
        ~block() throw ();

    public:
        void add_replica(const wtf::block_location& bid);
        uint64_t size() { return m_block_list.size(); }
        uint64_t offset() { return m_offset; }
        void set_length(uint64_t len) { m_length = len; }
        void set_offset(uint64_t offset) { m_offset = offset; }
        uint64_t pack_size();
        uint64_t length() { return m_length; }
        block_location first_location();
        std::vector<wtf::block_location>::iterator blocks_begin() { return m_block_list.begin(); }
        std::vector<wtf::block_location>::iterator blocks_end() { return m_block_list.end(); }
        bool is_hole() { return first_location() == block_location(); }

    private:
        friend class file;
        friend class e::intrusive_ptr<block>;
        friend std::ostream& 
            operator << (std::ostream& lhs, const block& rhs);
        friend e::buffer::packer
            operator << (e::buffer::packer pa, e::intrusive_ptr<block>& rhs);
        friend e::buffer::packer
            operator << (e::buffer::packer pa, const block& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, block& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, e::intrusive_ptr<block>& rhs);


    private:
        block(const block&);

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        block& operator = (const block&);
        typedef std::vector<wtf::block_location> block_list;

    private:
        size_t m_ref;
        block_list m_block_list;
        uint64_t m_offset;
        uint64_t m_length;
        bool m_is_hole;
};

template <typename T>
    std::ostream&
operator << (std::ostream& lhs, const std::vector<T>& rhs)
{
    for (size_t i = 0; i < rhs.size(); ++i)
    {
        if (i > 0)
        {
            lhs << " " << rhs;
        }
        else
        {
            lhs << rhs;
        }
    }

    return lhs;
}

inline std::ostream& 
operator << (std::ostream& lhs, const block& rhs) 
{ 
    lhs << "\t\t[" << rhs.m_offset << "," << rhs.m_offset+rhs.m_length << ")" << std::endl;

    for (block::block_list::const_iterator it = rhs.m_block_list.begin();
            it < rhs.m_block_list.end(); ++it)
    {
        lhs << "\t\t\t" << *it << std::endl;
    }

    return lhs;
} 

inline e::buffer::packer 
operator << (e::buffer::packer pa, e::intrusive_ptr<block>& rhs) 
{ 
    uint64_t replicas = rhs->m_block_list.size();
    uint64_t offset = rhs->m_offset;
    uint64_t length = rhs->m_length;

    pa = pa << offset << length << replicas; 

    for (block::block_list::const_iterator it = rhs->m_block_list.begin();
            it < rhs->m_block_list.end(); ++it)
    {
        pa = pa << *it;
    }

    return pa;
} 

inline e::buffer::packer 
operator << (e::buffer::packer pa, const block& rhs) 
{ 
    uint64_t replicas = rhs.m_block_list.size();
    uint64_t offset = rhs.m_offset;
    uint64_t length = rhs.m_length;

    pa = pa << offset << length << replicas; 

    for (block::block_list::const_iterator it = rhs.m_block_list.begin();
            it < rhs.m_block_list.end(); ++it)
    {
        pa = pa << *it;
    }

    return pa;
} 

inline e::unpacker 
operator >> (e::unpacker up, block& rhs) 
{ 
    uint64_t replicas;
    uint64_t len;
    uint64_t offset;

    up = up >> offset >> len >> replicas; 

    rhs.m_length = len;
    rhs.m_offset = offset;

    for (uint64_t i = 0; i < replicas; ++i)
    {
        block_location bl;
        up = up >> bl;
        rhs.add_replica(bl);
    }

    return up; 
} 

inline e::unpacker 
operator >> (e::unpacker up, e::intrusive_ptr<block>& rhs) 
{ 
    uint64_t replicas;
    uint64_t len;
    uint64_t offset;

    up = up >> offset >> len >> replicas; 

    rhs->m_length = len;
    rhs->m_offset = offset;

    for (uint64_t i = 0; i < replicas; ++i)
    {
        block_location bl;
        up = up >> bl;
        rhs->add_replica(bl);
    }

    return up; 
}
}
#endif // wtf_block_h_
