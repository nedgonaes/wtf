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
#include "client/pending_readdir.h"
#include "common/response_returncode.h"

using wtf::pending_readdir;

pending_readdir :: pending_readdir(client* cl, uint64_t client_visible_id, 
                           wtf_client_returncode* status, char* path,
                           char** entry)
    : pending_aggregation(client_visible_id, status) 
    , m_cl(cl)
    , m_entry(entry)
    , m_results()
    , m_path(path)
{
    set_status(WTF_CLIENT_SUCCESS);
    set_error(e::error());
}

pending_readdir :: ~pending_readdir() throw ()
{
}

bool
pending_readdir :: can_yield()
{
    return this->aggregation_done() && !m_results.empty();
}

bool
pending_readdir :: yield(wtf_client_returncode* status, e::error* err)
{
    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();
    assert(this->can_yield());
    std::string* res = &m_results[m_results.size()-1];
    *m_entry = (char*)malloc(res->size()+1);
    strcpy(*m_entry, res->c_str());
    m_results.pop_back();
    return true;
}

bool
pending_readdir :: handle_hyperdex_message(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    if (rc < 0)
    {
        PENDING_ERROR(IO) << "Couldn't get from HyperDex";
    }
    else if (rc == HYPERDEX_CLIENT_SEARCHDONE)
    {
        return pending_aggregation::handle_hyperdex_message(cl, reqid, rc, status, err);
    }
    else
    {
        e::intrusive_ptr<message_hyperdex_search> msg = 
            dynamic_cast<message_hyperdex_search* >(m_outstanding_hyperdex[0].get());
        hyperdex_client_attribute* attrs = msg->attrs();
        size_t sz = msg->attrs_sz();

        for (int i = 0; i < sz; ++i)
        {
            if (strcmp(attrs[i].attr, "path") == 0)
            {
                m_results.push_back(std::string(attrs[i].value));
                break;
            }
        }
    }

    return true;
}

bool
pending_readdir :: try_op()
{
    std::string regex("^");
    regex += m_path;


    e::intrusive_ptr<message_hyperdex_search> msg = new message_hyperdex_search("wtf", "path", regex.c_str());
   
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
