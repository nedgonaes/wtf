// Copyright (c) 2013, Cornell University
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

// WTF
#include "client/pending_aggregation.h"
#include "common/macros.h"

using wtf::pending_aggregation;

pending_aggregation :: pending_aggregation(uint64_t id,
                                           wtf_client_returncode* status)
    : m_ref(0)
    , m_outstanding_wtf()
    , m_outstanding_hyperdex()
    , m_client_visible_id(id)
    , m_status(status)
    , m_error()
{
    TRACE;
}

pending_aggregation :: ~pending_aggregation() throw ()
{
    TRACE;
}

bool
pending_aggregation :: aggregation_done()
{
    TRACE;
    return m_outstanding_wtf.empty() && m_outstanding_hyperdex.empty();
}

void
pending_aggregation :: handle_sent_to_wtf(const server_id& si)
{
    TRACE;
    m_outstanding_wtf.push_back(si);
}

void
pending_aggregation :: handle_sent_to_hyperdex(e::intrusive_ptr<message>& msg)
{
    TRACE;
    m_outstanding_hyperdex.push_back(msg);
}

void
pending_aggregation :: handle_hyperdex_failure(int64_t reqid)
{
    TRACE;
    remove_hyperdex_message(reqid);
}

void
pending_aggregation :: handle_wtf_failure(const server_id& si)
{
    TRACE;
    remove_wtf_message(si);
}

bool
pending_aggregation :: handle_wtf_message(client* cl,
                                    const server_id& si,
                                    std::auto_ptr<e::buffer>,
                                    e::unpacker up,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    TRACE;
    remove_wtf_message(si);
    return true;
}

bool
pending_aggregation :: handle_hyperdex_message(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    TRACE;
    remove_hyperdex_message(reqid);
    return true;
}

void
pending_aggregation :: remove_wtf_message(const server_id& si)
{
    TRACE;
    for (size_t i = 0; i < m_outstanding_wtf.size(); ++i)
    {
        if (m_outstanding_wtf[i] != si)
        {
            continue;
        }

        for (size_t j = i; j + 1 < m_outstanding_wtf.size(); ++j)
        {
            m_outstanding_wtf[j] = m_outstanding_wtf[j + 1];
        }

        m_outstanding_wtf.pop_back();
        return;
    }
}

void
pending_aggregation :: remove_hyperdex_message(int64_t reqid)
{
    TRACE;
    for (size_t i = 0; i < m_outstanding_hyperdex.size(); ++i)
    {
        if (m_outstanding_hyperdex[i]->reqid() != reqid)
        {
            TRACE;
            continue;
        }

        for (size_t j = i; j + 1 < m_outstanding_hyperdex.size(); ++j)
        {
            TRACE;
            m_outstanding_hyperdex[j] = m_outstanding_hyperdex[j + 1];
        }

        TRACE;
        m_outstanding_hyperdex.pop_back();
        return;
    }
}

std::ostream&
pending_aggregation :: error(const char* file, size_t line)
{
    TRACE;
    m_error.set_loc(file, line);
    return m_error.set_msg();
}

void
pending_aggregation :: set_error(const e::error& err)
{
    TRACE;
    m_error = err;
}

bool
pending_aggregation :: try_op()
{
    TRACE;
    return true;
}
