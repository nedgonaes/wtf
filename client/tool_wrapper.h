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

#ifndef wtf_client_tool_wrapper_h_
#define wtf_client_tool_wrapper_h_

// WTF
#include "client/wtf.h"

namespace wtf
{

class tool_wrapper
{
    public:
        tool_wrapper(wtf_client* h) : m_h(h) {}
        tool_wrapper(const tool_wrapper& other) : m_h(other.m_h) {}
        ~tool_wrapper() throw () {}

    public:
        wtf_returncode initialize_cluster(uint64_t cluster, const char* path)
        { return m_h->initialize_cluster(cluster, path); }
        wtf_returncode show_config(std::ostream& out)
        { return m_h->show_config(out); }
        wtf_returncode kill(uint64_t server_id)
        { return m_h->kill(server_id); }

    public:
        tool_wrapper& operator = (const tool_wrapper& rhs)
        { m_h = rhs.m_h; return *this; }

    private:
        wtf_client* m_h;
};

} // namespace wtf

#endif // wtf_client_tool_wrapper_h_
