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

#define __STDC_LIMIT_MACROS

// e
#include <e/endian.h>

// WTF
#include "client/command.h"

using wtf::wtf_node;

wtf_client :: command :: command(wtf_returncode* st,
                                       uint64_t n,
                                       std::auto_ptr<e::buffer> m,
                                       const char** output,
                                       size_t* output_sz)
    : m_ref(0)
    , m_nonce(n)
    , m_clientid(n)
    , m_request(m)
    , m_status(st)
    , m_output(output)
    , m_output_sz(output_sz)
    , m_sent_to()
    , m_last_error_desc()
    , m_last_error_file()
    , m_last_error_line()
{
    *st = WTF_GARBAGE;
}

wtf_client :: command :: ~command() throw ()
{
}

void
wtf_client :: command :: set_nonce(uint64_t n)
{
    m_nonce = n;
}

void
wtf_client :: command :: set_sent_to(const wtf_node& s)
{
    m_sent_to = s;
}

void
wtf_client :: command :: fail(wtf_returncode status)
{
    *m_status = status;
}

void
wtf_client :: command :: succeed(std::auto_ptr<e::buffer> backing,
                                       const e::slice& resp,
                                       wtf_returncode status)
{
    if (m_output)
    {
        char* base = reinterpret_cast<char*>(backing.get());
        const char* data = reinterpret_cast<const char*>(resp.data());
        *m_output = data;
        *m_output_sz = resp.size();
        backing.release();
    }

    *m_status = status;
}
