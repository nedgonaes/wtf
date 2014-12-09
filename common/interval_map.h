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

#ifndef wtf_interval_map_h_
#define wtf_interval_map_h_

// e
#include <e/intrusive_ptr.h>

// STL
#include <map>

namespace wtf __attribute__ ((visibility("hidden")))
{

class interval_map
{
    public:
        interval_map();
        ~interval_map() throw ();

    private:
        friend class file;
        friend class e::intrusive_ptr<interval_map>;
        friend std::ostream& 
            operator << (std::ostream& lhs, const interval_map& rhs);
        friend e::buffer::packer
            operator << (e::buffer::packer pa, e::intrusive_ptr<interval_map>& rhs);
        friend e::buffer::packer
            operator << (e::buffer::packer pa, const interval_map& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, interval_map& rhs);
        friend e::unpacker
            operator >> (e::unpacker up, e::intrusive_ptr<interval_map>& rhs);


    private:
        interval_map(const interval_map&);

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        interval_map& operator = (const interval_map&);

    private:
        size_t m_ref;
};

template <typename T>
    std::ostream&
operator << (std::ostream& lhs, const std::vector<T>& rhs)
{
    return lhs;
}

inline std::ostream& 
operator << (std::ostream& lhs, const interval_map& rhs) 
{ 
    return lhs;
} 


}
#endif // wtf_interval_map_h_
