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
#include <e/buffer.h>

// STL
#include <map>

namespace wtf
{

class vblock
{
    public:
        vblock();
        ~vblock() throw ();

    public:
        void update(size_t offset, size_t len, size_t disk_offset);
        uint64_t size() { return m_slice_map.size(); }

    public:
        class slice;
        typedef std::map<size_t, e::intrusive_ptr<slice> > slice_map;

    private:
        friend class e::intrusive_ptr<vblock>;
        friend std::ostream& 
            operator << (std::ostream& lhs, const vblock& rhs);
        friend std::ostream&
            operator << (std::ostream& lhs, const e::intrusive_ptr<vblock>& rhs);
        friend e::buffer::packer
            operator << (e::buffer::packer pa, const vblock& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, vblock& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, e::intrusive_ptr<vblock>& rhs);
        friend e::buffer::packer 
            operator << (e::buffer::packer pa, const e::intrusive_ptr<vblock>& rhs);

    private:
        vblock(const vblock&);
        void update(slice& s);

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        vblock& operator = (const vblock&);

    private:
        size_t m_ref;
        slice_map m_slice_map;
};

class vblock::slice
{
    public:
        slice();
        slice(size_t offset, size_t length, size_t disk_offset);
        ~slice() throw();

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

     private:
        friend class e::intrusive_ptr<slice>;
        friend class vblock;
        friend std::ostream& 
            operator << (std::ostream& lhs, const vblock::slice& rhs);
        friend e::buffer::packer
            operator << (e::buffer::packer pa, const slice& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, slice& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, e::intrusive_ptr<slice>& rhs);
        friend e::unpacker 
            operator >> (e::unpacker up, vblock& rhs);
        friend e::unpacker 
            operator >> (e::unpacker up, e::intrusive_ptr<vblock>& rhs);
        friend std::ostream& 
            operator << (std::ostream& lhs, const e::intrusive_ptr<vblock::slice>& rhs);
        friend e::buffer::packer 
            operator << (e::buffer::packer pa, const e::intrusive_ptr<vblock>& rhs);
        friend e::buffer::packer 
            operator << (e::buffer::packer pa, const e::intrusive_ptr<vblock::slice>& rhs);
     private:
        size_t m_ref;
        size_t m_offset;
        size_t m_length;
        uint64_t m_disk_offset;
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
operator << (std::ostream& lhs, const wtf::vblock& rhs) 
{ 
    lhs << "vblock(slices=[";

    bool first = true;

    for (vblock::slice_map::const_iterator it = rhs.m_slice_map.begin();
            it != rhs.m_slice_map.end(); ++it)
    {
        if(!first)
        {
            lhs << ",";
            first = false;
        }

        lhs << it->second;
    }

    lhs << "])";

    return lhs;
} 

inline std::ostream& 
operator << (std::ostream& lhs, const e::intrusive_ptr<vblock>& rhs) 
{ 
    lhs << "vblock(slices=[";

    bool first = true;

    for (vblock::slice_map::const_iterator it = rhs->m_slice_map.begin();
            it != rhs->m_slice_map.end(); ++it)
    {
        if(!first)
        {
            lhs << ",";
            first = false;
        }

        lhs << it->second;
    }

    lhs << "])";

    return lhs;
}

//SLICE SERIALIZATION
inline e::buffer::packer 
operator << (e::buffer::packer pa, const e::intrusive_ptr<vblock::slice>& rhs) 
{ 
    pa = pa << rhs->m_offset << rhs->m_length << rhs->m_disk_offset;
    return pa;
} 

inline e::buffer::packer 
operator << (e::buffer::packer pa, const vblock::slice& rhs) 
{ 
    pa = pa << rhs.m_offset << rhs.m_length << rhs.m_disk_offset;
    return pa;
} 

inline e::unpacker 
operator >> (e::unpacker up, vblock::slice& rhs) 
{ 
    up = up >> rhs.m_offset >> rhs.m_length >> rhs.m_disk_offset; 
    return up; 
} 

inline e::unpacker 
operator >> (e::unpacker up, e::intrusive_ptr<vblock::slice>& rhs) 
{ 
    up >> rhs->m_offset >> rhs->m_length >> rhs->m_disk_offset;
    return up; 
}

inline std::ostream& 
operator << (std::ostream& lhs, const vblock::slice& rhs) 
{ 
    lhs << "slice(" << rhs.m_offset << "," << rhs.m_length << "," << rhs.m_disk_offset << ")";
    return lhs;
} 

inline std::ostream& 
operator << (std::ostream& lhs, const e::intrusive_ptr<vblock::slice>& rhs) 
{ 
    lhs << "slice(" << rhs->m_offset << "," << rhs->m_length << "," << rhs->m_disk_offset << ")";
    return lhs;
}

//VBLOCK SERIALIZATION
inline e::buffer::packer 
operator << (e::buffer::packer pa, const e::intrusive_ptr<vblock>& rhs) 
{ 
    uint64_t size = rhs->m_slice_map.size();
    pa = pa << size; 

    for (vblock::slice_map::const_iterator it = rhs->m_slice_map.begin();
            it != rhs->m_slice_map.end(); ++it)
    {
        pa = pa << it->second;
    }

    return pa;
} 

inline e::buffer::packer 
operator << (e::buffer::packer pa, const vblock& rhs) 
{ 
    uint64_t size = rhs.m_slice_map.size();
    pa = pa << size; 

    for (vblock::slice_map::const_iterator it = rhs.m_slice_map.begin();
            it != rhs.m_slice_map.end(); ++it)
    {
        pa = pa << it->second;
    }

    return pa;
} 

inline e::unpacker 
operator >> (e::unpacker up, vblock& rhs) 
{ 
    uint64_t size;
    up = up >> size; 

    for (uint64_t i = 0; i < size; ++i)
    {
        e::intrusive_ptr<vblock::slice> s(new vblock::slice());
        up = up >> s;
        rhs.m_slice_map[s->m_offset] = s;
    }

    return up; 
} 

inline e::unpacker 
operator >> (e::unpacker up, e::intrusive_ptr<vblock>& rhs) 
{ 
    uint64_t size;

    up = up >> size; 

    for (uint64_t i = 0; i < size; ++i)
    {
        e::intrusive_ptr<vblock::slice> s(new vblock::slice());
        up = up >> s;
        rhs->m_slice_map[s->m_disk_offset] = s;
    }

    return up; 
}

}
#endif // wtf_vblock_h_
