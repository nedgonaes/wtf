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
#include <hyperdex/datastructures.h>
#include <hyperdex/client.hpp>

// WTF
#include "client/pending_rename.h"
#include "common/response_returncode.h"
#include "client/message_hyperdex_search.h"
#include "client/message_hyperdex_put.h"
#include "client/message_hyperdex_del.h"

using wtf::pending_rename;
using wtf::message_hyperdex_put;

pending_rename :: pending_rename(client* cl, uint64_t client_visible_id, 
                           wtf_client_returncode* status, const char* src,
                           const char* dst)
    : pending_aggregation(client_visible_id, status) 
    , m_cl(cl)
    , m_src(src)
    , m_dst(dst)
    , m_search_id(0)
    , m_search_status(HYPERDEX_CLIENT_GARBAGE)
    , m_search_attrs(NULL)
    , m_done(false)
{
    set_status(WTF_CLIENT_SUCCESS);
    set_error(e::error());
}

pending_rename :: ~pending_rename() throw ()
{
}

bool
pending_rename :: can_yield()
{
    return this->aggregation_done() && !m_done;
}

bool
pending_rename :: yield(wtf_client_returncode* status, e::error* err)
{
    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();
    assert(this->can_yield());
    m_done = true;
    return true;
}

bool
pending_rename :: handle_hyperdex_message(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    //XXX: search for it in m_outstanding_hyperdex
    if (reqid == m_search_id)
    {
        return handle_search(cl, reqid, rc, status, err);
    }

    else
    {
        return handle_put_and_delete(cl, reqid, rc, status, err);
    }
}

typedef struct hyperdex_ds_arena* arena_t;
typedef const struct hyperdex_client_attribute* attr_t;

attr_t 
change_name(arena_t arena, attr_t attrs, size_t sz, std::string& dst)
{
    //XXX
    return NULL;
}

bool
pending_rename :: send_put(std::string& dst, const hyperdex_client_attribute* attrs, size_t attrs_sz)
{
    e::intrusive_ptr<message_hyperdex_put> msg = 
        new message_hyperdex_put(m_cl, "wtf", dst.c_str(), attrs, attrs_sz);

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
pending_rename :: handle_search(client* cl,
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
        const hyperdex_client_attribute* search_attrs = msg->attrs();
        size_t sz = msg->attrs_sz();


        arena_t arena = hyperdex_ds_arena_create();
        attr_t attrs = change_name(arena, search_attrs, sz, m_dst);
        bool ret = send_put(m_dst, attrs, sz) && send_del(m_dst);
        hyperdex_ds_arena_destroy(arena);
        return ret;
    }

    return true;
}

bool
pending_rename :: handle_put_and_delete(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    if (rc != HYPERDEX_CLIENT_SUCCESS)
    {
        /*
         * XXX: retry if necessary.
         * Need to figure out what hyperdex statuses
         * are "transient failures" that should be retried
         * in every case, and what indicates a problem with
         * the op that we might want to just pass up to the
         * user, like if the thing we're renaming to already
         * exists.
         * 
         * When transactions are added, we should make this
         * whole op a nested transaction so that there is
         * never a chance for a client that is not using
         * transactions to delete the metadata for a file
         * and fail to create a new file, or create duplicates.
         * For clients that are using transactions, this will
         * just end up being a sub-transaction that pretends to
         * commit, but may fail when the client calls commit on
         * the user's transaction. 
         */
        PENDING_ERROR(IO) << "Delete failed.";
    }

    if (this->aggregation_done())
    {
        //XXX: commit transaction here
        //XXX: retry if commit failed.
    }

    return pending_aggregation::handle_hyperdex_message(cl, reqid, rc, status, err);
}
bool
pending_rename :: send_del(std::string& src)
{
    e::intrusive_ptr<message_hyperdex_del> msg = new message_hyperdex_del(m_cl, "wtf", src.c_str());

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
pending_rename :: try_op()
{
    std::string regex("^");
    regex += m_src;


    e::intrusive_ptr<message_hyperdex_search> msg = 
        new message_hyperdex_search(m_cl, "wtf", "path", regex.c_str());
   
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
