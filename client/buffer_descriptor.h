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

#ifndef wtf_buffer_descriptor_h_
#define wtf_buffer_descriptor_h_

// e
#include <e/intrusive_ptr.h>
#include <assert.h>

namespace wtf __attribute__ ((visibility("hidden")))
{

class buffer_descriptor 
{

    public:
        buffer_descriptor(const char* buffer, int count);
        ~buffer_descriptor() throw ();

    private:
        buffer_descriptor(const buffer_descriptor&);

    private:
        friend class e::intrusive_ptr<buffer_descriptor>;
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        buffer_descriptor& operator = (const buffer_descriptor&);

    public:
        void add_op() { ++m_count; }
        void remove_op() { --m_count; assert(m_count >= 0); }
        bool done() { return m_count == 0; }
        void print() { std::cout << "buffer descriptor is " << m_count << std::endl; }

    private:
        size_t m_ref;
        const char* m_buffer;
        int m_count;
};
}

#endif // wtf_buffer_descriptor_h_
