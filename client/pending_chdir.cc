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

//hyperdex
#include <hyperdex/client.hpp>

// WTF
#include "common/macros.h"
#include "client/constants.h"
#include "client/client.h"
#include "common/block.h"
#include "client/pending_chdir.h"
#include "common/response_returncode.h"
#include "client/message_hyperdex_get.h"
#include "client/message_hyperdex_put.h"

using wtf::pending_chdir;

pending_chdir :: pending_chdir(client* cl, uint64_t id, e::intrusive_ptr<file> f,
                               const char* buf, size_t* buf_sz, 
                               wtf_client_returncode* status)
    : pending_aggregation(id, status)
    , m_cl(cl)
    , m_buf(buf)
    , m_buf_sz(buf_sz)
    , m_old_blockmap(f->serialize_blockmap())
    , m_file(f)
    , m_done(false)
    , m_state(0)
{
    TRACE;
    set_status(WTF_CLIENT_SUCCESS);
    set_error(e::error());
}

pending_chdir :: ~pending_chdir() throw ()
{
    TRACE;
}

bool
pending_chdir :: can_yield()
{
    TRACE;
    return this->aggregation_done() && !m_done;
}

bool
pending_chdir :: yield(wtf_client_returncode* status, e::error* err)
{
    TRACE;
    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();
    assert(this->can_yield());
    m_done = true;
    return true;
}

bool
pending_chdir :: try_op()
{
    TRACE;
    /* Get the file metadata from HyperDex */
    e::intrusive_ptr<message_hyperdex_get> msg =
        new message_hyperdex_get(m_cl, "wtf", m_path.c_str()); 
       
    if (msg->send() < 0)
    {
        PENDING_ERROR(IO) << "Couldn't put to HyperDex: " << msg->status();
    }
    else
    {
        m_cl->add_hyperdex_op(msg->reqid(), this);
        e::intrusive_ptr<message> m = msg.get();
        handle_sent_to_hyperdex(m);
    }

    return true;
}

bool
pending_chdir :: handle_hyperdex_message(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    TRACE;
    //response from initial get

    if (res == HYPERDEX_CLIENT_NOTFOUND)
    {
        ERROR(NOTFOUND) << "path " << abspath << " not found in HyperDex.";
        return -1;
    }
    else
    {
        for (size_t i = 0; i < attrs_sz; ++i)
        {
            if (strcmp(attrs[i].attr, "directory") == 0)
            {
                uint64_t is_dir;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> is_dir;

                if (is_dir == 0)
                {
                    errno = ENOTDIR;
                    return -1;
                }
            }
            else if (strcmp(attrs[i].attr, "mode") == 0)
            {
                uint64_t mode;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> mode;
                //XXX: implement owner, group, etc and deny if no
                //     read permissions.
            }
        }
    }

    m_cl->m_cwd = m_path;
}

