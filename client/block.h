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

class wtf_client::block
{
    public:
        block();
        ~block() throw ();

    public:
        void update(uint64_t version, uint64_t length,
                    uint64_t sid, uint64_t bid);

    private:
        friend class e::intrusive_ptr<block>;

    private:
        block(const block&);

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        block& operator = (const block&);
        typedef std::vector<e::intrusive_ptr<wtf::block_id> > block_list;

    private:
        size_t m_ref;
        block_list m_block_list;
        uint64_t m_length;
        uint64_t m_version;
};

#endif // wtf_block_h_