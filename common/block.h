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
#include <client/wtf.h>
#include <common/block_id.h>

namespace wtf
{

class block
{
    public:
        block();
        ~block() throw ();

    public:
        void update(uint64_t version, uint64_t len, 
                    const wtf::block_id& bid, bool dirty);
        uint64_t size() { return m_block_list.size(); }
        uint64_t version() { return m_version; }
        uint64_t pack_size();
        void pack(char* buf) const;
        uint64_t resize(uint64_t sz) { m_length = sz; }
        uint64_t length() { return m_length; }
        block_id get_first_location() { return m_block_list[0]; }
        bool dirty() { return m_dirty; }

    private:
        friend class e::intrusive_ptr<block>;
        friend std::ostream& 
            operator << (std::ostream& lhs, const block& rhs);
        friend e::buffer::packer
            operator << (e::buffer::packer pa, const block& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, block& rhs);

    private:
        block(const block&);

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        block& operator = (const block&);
        typedef std::vector<wtf::block_id> block_list;

    private:
        size_t m_ref;
        block_list m_block_list;
        uint64_t m_version;
        uint64_t m_length;
        bool m_dirty;
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
    lhs << "block(len=" << rhs.m_length << ", chunks=[";

    bool first = true;

    for (block::block_list::const_iterator it = rhs.m_block_list.begin();
            it < rhs.m_block_list.end(); ++it)
    {
        if(!first)
        {
            lhs << ",";
            first = false;
        }

        lhs << *it;
    }

    lhs << "])";

    return lhs;
} 

inline e::buffer::packer 
operator << (e::buffer::packer pa, const block& rhs) 
{ 
    uint64_t sz = rhs.m_block_list.size();
    pa = pa << rhs.m_length << sz; 

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
    uint64_t size;
    uint64_t len;

    up = up >> len >> size; 

    e::unpack64be((uint8_t *)&len, &len);
    e::unpack64be((uint8_t *)&size, &size);

    std::cout << "Unpacking block, len: " << len << " size:" << size << std::endl;

    for (uint64_t i = 0; i < size; ++i)
    {
        block_id bid;
        up = up >> bid;
        rhs.update(0, len, bid, false);
    }

    return up; 
} 

}
#endif // wtf_block_h_
