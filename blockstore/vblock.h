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

#ifndef wtf_vblock_h_
#define wtf_vblock_h_

// e
#include <e/intrusive_ptr.h>

// STL
#include <vector>

namespace wtf
{

class vblock
{
    public:
        vblock();
        ~vblock() throw ();

    public:
        void update(uint64_t offset, uint64_t len);
        uint64_t size() { return m_block_list.size(); }
        uint64_t pack_size();
        uint64_t resize(uint64_t sz) { m_length = sz; }
        uint64_t length() { return m_length; }
        std::vector<wtf::block_id>::iterator blocks_begin() { return m_block_list.begin(); }
        std::vector<wtf::block_id>::iterator blocks_end() { return m_block_list.end(); }

    private:
        friend class e::intrusive_ptr<vblock>;
        friend std::ostream& 
            operator << (std::ostream& lhs, const vblock& rhs);
        friend e::buffer::packer
            operator << (e::buffer::packer pa, const vblock& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, vblock& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, e::intrusive_ptr<vblock>& rhs);


    private:
        class slice;
        vblock(const vblock&);
        void update(slice& s);

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        vblock& operator = (const vblock&);
        typedef std::vector<wtf::vblock::slice> slice_list;

    private:
        size_t m_ref;
        slice_list m_slice_list;
        uint64_t m_length;
};

class vblock::slice
{
    public:
        slice();
        slice(uint64_t offset, uint64_t length);
        ~slice() throw();

     private:
        friend class e::intrusive_ptr<slice>;
        friend std::ostream& 
            operator << (std::ostream& lhs, const slice& rhs);
        friend e::buffer::packer
            operator << (e::buffer::packer pa, const slice& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, slice& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, e::intrusive_ptr<slice>& rhs);
     private:
        uint64_t m_offset;
        uint64_t m_length;
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
operator << (std::ostream& lhs, const vblock& rhs) 
{ 
    lhs << "vblock(len=" << rhs.m_length << ", slices=[";

    bool first = true;

    for (vblock::slice_list::const_iterator it = rhs.m_slice_list.begin();
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

//VBLOCK SERIALIZATION
inline e::buffer::packer 
operator << (e::buffer::packer pa, const vblock& rhs) 
{ 
    uint64_t sz = rhs.m_slice_list.size();
    pa = pa << rhs.m_length << sz; 

    for (vblock::slice_list::const_iterator it = rhs.m_slice_list.begin();
            it < rhs.m_slice_list.end(); ++it)
    {
        pa = pa << *it;
    }

    return pa;
} 

inline e::unpacker 
operator >> (e::unpacker up, vblock& rhs) 
{ 
    uint64_t size;
    uint64_t len;

    up = up >> len >> size; 

    for (uint64_t i = 0; i < size; ++i)
    {
        vblock::slice s;
        up = up >> s;
        rhs.m_slice_list.push_back(s);
    }

    return up; 
} 

inline e::unpacker 
operator >> (e::unpacker up, e::intrusive_ptr<vblock>& rhs) 
{ 
    uint64_t size;
    uint64_t len;

    up = up >> len >> size; 

    for (uint64_t i = 0; i < size; ++i)
    {
        vblock::slice s;
        up = up >> s;
        rhs->update(s);
    }

    return up; 
}


//SLICE SERIALIZATION
inline e::buffer::packer 
operator << (e::buffer::packer pa, const vblock::slice& rhs) 
{ 
    pa = pa << rhs.m_offset << rhs.m_length; 
    return pa;
} 

inline e::unpacker 
operator >> (e::unpacker up, vblock::slice& rhs) 
{ 
    up = up >> rhs.m_offset >> rhs.m_length; 
    return up; 
} 

inline e::unpacker 
operator >> (e::unpacker up, e::intrusive_ptr<vblock::slice>& rhs) 
{ 
    up >> rhs.m_offset >> rhs.m_length;
    return up; 
}


}
#endif // wtf_vblock_h_
