// Copyright (c) 2012-2013, Cornell University
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
#include "client/pending_read.h"

using wtf::pending_read;
using wtf::pending_aggregation;

pending_read :: pending_read(uint64_t id,
                             wtf_client_returncode* status,
                             char* buf,
                             size_t* buf_sz)
    : m_buf(buf)
    , m_buf_sz(buf_sz)
    , pending_aggregation(id, status)
    , m_done(false)
{
    set_status(WTF_CLIENT_SUCCESS);
    set_error(e::error());
}

pending_read :: ~pending_read() throw ()
{
}

bool
pending_read :: can_yield()
{
    return this->aggregation_done() && !m_done;
}

bool
pending_read :: yield(wtf_client_returncode* status, e::error* err)
{
    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();
    assert(this->can_yield());
    m_done = true;
    return true;
}

void
pending_read :: handle_failure(const server_id& si)
{
    PENDING_ERROR(RECONFIGURE) << "reconfiguration affecting "
                               << "/" << si;
    return pending_aggregation::handle_failure(si);
}

bool
pending_read :: handle_message(client* cl,
                                    const server_id& si,
                                    wtf_network_msgtype mt,
                                    std::auto_ptr<e::buffer>,
                                    e::unpacker up,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    bool handled = pending_aggregation::handle_message(cl, si, mt, std::auto_ptr<e::buffer>(), up, status, err);
    assert(handled);

    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();

    if (mt != RESP_GET)
    {
        PENDING_ERROR(SERVERERROR) << "server " <<  si << " responded to GET with " << mt;
        return true;
    }

    //XXX: put the data in the buffer.

    return true;
}
