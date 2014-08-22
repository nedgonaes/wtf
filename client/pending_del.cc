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
#include "client/message_hyperdex_search.h"
#include "client/message_hyperdex_del.h"
#include "client/pending_del.h"
#include "common/response_returncode.h"

using wtf::pending_del;

pending_del :: pending_del(client* cl, uint64_t client_visible_id, 
                           std::string path, wtf_client_returncode* status)
    : pending_aggregation(client_visible_id, status) 
    , m_path(path)
    , m_cl(cl)
    , m_done(false)
    , m_search_id(0)
{
    set_status(WTF_CLIENT_SUCCESS);
    set_error(e::error());
}

pending_del :: ~pending_del() throw ()
{
}

bool
pending_del :: can_yield()
{
    return this->aggregation_done() && !m_done;
}

bool
pending_del :: yield(wtf_client_returncode* status, e::error* err) {
    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();
    assert(this->can_yield());
    m_done = true;
    return true;
}

void
pending_del :: handle_hyperdex_failure(int64_t reqid)
{
    return pending_aggregation::handle_hyperdex_failure(reqid);
}

bool
pending_del :: handle_hyperdex_message(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    if (reqid == m_search_id)
    {
        return handle_search(cl, reqid, rc, status, err);
    }

    else
    {
        return handle_delete(cl, reqid, rc, status, err);
    }
}

bool
pending_del :: try_op()
{
    std::string regex("^");
    regex += m_path;


    e::intrusive_ptr<message_hyperdex_search> msg = new message_hyperdex_search(m_cl, "wtf", "path", regex.c_str());
   
    if (msg->send() < 0)
    {
        PENDING_ERROR(IO) << "Couldn't put to HyperDex: " << msg->status();
    }
    else
    {
        m_cl->add_hyperdex_op(msg->reqid(), this);
        e::intrusive_ptr<message> m = msg.get();
        pending_aggregation::handle_sent_to_hyperdex(m);
    }

    return true;
}

bool
pending_del :: send_del(std::string path)
{
    e::intrusive_ptr<message_hyperdex_del> msg = new message_hyperdex_del(m_cl, "wtf", path.c_str());

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
pending_del :: handle_search(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    std::cout << "HYPERDEX RETURNED " << rc << std::endl;
    e::intrusive_ptr<message_hyperdex_search> msg = 
        dynamic_cast<message_hyperdex_search* >(m_outstanding_hyperdex[0].get());

    if (rc < 0)
    {
        PENDING_ERROR(IO) << "Couldn't get from HyperDex";
    }
    else if (msg->status() == HYPERDEX_CLIENT_SEARCHDONE)
    {
        return pending_aggregation::handle_hyperdex_message(cl, reqid, rc, status, err);
    }
    else
    {
        const hyperdex_client_attribute* attrs = msg->attrs();
        size_t sz = msg->attrs_sz();

        for (int i = 0; i < sz; ++i)
        {
            if (strcmp(attrs[i].attr, "path") == 0)
            {
                //send_del adds an op for the delete.
                send_del(std::string(attrs[i].value));
                break;
            }
        }

        //need to add for the pending search which is still valid.
        m_cl->add_hyperdex_op(m_outstanding_hyperdex[0]->reqid(), this);
    }

    return true;
}

bool
pending_del :: handle_delete(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    if (rc != HYPERDEX_CLIENT_SUCCESS)
    {
        PENDING_ERROR(IO) << "Delete failed.";
    }

    if (this->aggregation_done())
    {
        //XXX: commit transaction here
        //XXX: retry if commit failed.
    }

    return pending_aggregation::handle_hyperdex_message(cl, reqid, rc, status, err);
}

