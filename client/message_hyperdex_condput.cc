// Copyright (c) 2011-2013, Cornell University
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

#include "common/macros.h"
#include "client/message_hyperdex_condput.h"

using wtf::message_hyperdex_condput;

message_hyperdex_condput :: message_hyperdex_condput(client* cl,
                                             const char* space,
                                             const char* key,
                                             const struct hyperdex_ds_arena* checks_arena,
                                             const hyperdex_client_attribute_check* checks,
                                             size_t checks_sz,
                                             const struct hyperdex_ds_arena* attrs_arena,
                                             const hyperdex_client_attribute* attrs,
                                             size_t attrs_sz)
    : message(cl, OPCODE_HYPERDEX_PUT) 
    , m_space(space)
    , m_key(key)
    , m_checks_arena(checks_arena)
    , m_status(HYPERDEX_CLIENT_GARBAGE)
    , m_checks(checks)
    , m_checks_sz(checks_sz)
    , m_attrs_arena(attrs_arena)
    , m_attrs(attrs)
    , m_attrs_size(attrs_sz)
{
    TRACE;
}

message_hyperdex_condput :: ~message_hyperdex_condput() throw()
{
    TRACE;
}

int64_t
message_hyperdex_condput :: send()
{
    TRACE;
    hyperdex::Client* hc = &m_cl->m_hyperdex_client;
    m_reqid = hc->cond_put(m_space.c_str(), m_key.data(), m_key.size(),
            m_checks, m_checks_sz, m_attrs, m_attrs_size, &m_status);
    return m_reqid;
}
