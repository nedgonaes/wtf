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
#include "common/macros.h"
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
    TRACE;
    set_status(WTF_CLIENT_SUCCESS);
    set_error(e::error());
}

pending_rename :: ~pending_rename() throw ()
{
    TRACE;
}

bool
pending_rename :: can_yield()
{
    TRACE;
    return this->aggregation_done() && !m_done;
}

bool
pending_rename :: yield(wtf_client_returncode* status, e::error* err)
{
    TRACE;
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
    TRACE;
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

bool
pending_rename :: handle_wtf_message(client* cl,
                                    const server_id& si,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up,
                                    wtf_client_returncode* status,
                                    e::error* error)
{
    TRACE;
}
 
typedef struct hyperdex_ds_arena* arena_t;
typedef const struct hyperdex_client_attribute* attr_t;

attr_t 
pending_rename :: change_name(arena_t arena, attr_t attrs, size_t sz, std::string& dst, std::string& src)
{
    TRACE;
    hyperdex_ds_returncode status;
    struct hyperdex_client_attribute* attrs_new;
    attrs_new = hyperdex_ds_allocate_attribute(arena, sz-1);

    int j = 0;
    for (int i = 0; i < sz; ++i)
    {
        if (strcmp(attrs[i].attr, "path") == 0)
        {
            //send_del adds an op for the delete.
            src = std::string(attrs[i].value, attrs[i].value_sz);
            dst = src;
            dst.replace(dst.begin(), dst.begin() + m_src.size(), m_dst);
        }
        else
        {
            size_t size;

            std::cout << "COPYING " << attrs[i].attr << std::endl;
            attrs_new[j].datatype = attrs[i].datatype;
            hyperdex_ds_copy_string(arena, attrs[i].attr,
                strlen(attrs[i].attr) + 1,
                &status, &attrs_new[j].attr, &size);
            hyperdex_ds_copy_string(arena, attrs[i].value,
                attrs[i].value_sz,
                &status, &attrs_new[j].value, &attrs_new[j].value_sz);
            ++j;
        }
    }

    return attrs_new;
}

bool
pending_rename :: send_put(std::string& dst, arena_t arena,
                            const hyperdex_client_attribute* attrs, size_t attrs_sz)
{
    TRACE;
    std::cout << "===== HERE ======" << std::endl;
    std::cout << dst << std::endl;
    e::intrusive_ptr<message_hyperdex_put> msg = 
        new message_hyperdex_put(m_cl, "wtf", dst.c_str(), arena, attrs, attrs_sz);

    std::cout << attrs_sz << std::endl;
    for (int i = 0; i < attrs_sz; ++i)
    {
        std::cout << attrs[i].attr << std::endl;
        std::cout << attrs[i].value_sz << std::endl;
    }

    if (msg->send() < 0)
    {
        TRACE;
        PENDING_ERROR(IO) << "Couldn't put to HyperDex: " << msg->status();
    }
    else
    {
        TRACE;
        m_cl->add_hyperdex_op(msg->reqid(), this);
        e::intrusive_ptr<message> m = msg.get();
        pending_aggregation::handle_sent_to_hyperdex(m);
    }

    TRACE;
    return true;
}


bool
pending_rename :: handle_search(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    TRACE;
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
        std::string src;
        std::string dst;
        //needs to point src to the path from the search_attrs
        attr_t attrs = change_name(arena, search_attrs, sz, dst, src);
        bool ret = send_put(dst, arena, attrs, sz-1) && send_del(src);
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
    TRACE;
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
    TRACE;
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

void
pending_rename :: handle_sent_to_hyperdex(e::intrusive_ptr<message> msg)
{
    TRACE;
    pending_aggregation::handle_sent_to_hyperdex(msg);
}

void
pending_rename :: handle_sent_to_wtf(const server_id& sid)
{
    TRACE;
    pending_aggregation::handle_sent_to_wtf(sid);
}

void
pending_rename :: handle_hyperdex_failure(int64_t reqid)
{
    TRACE;
    return pending_aggregation::handle_hyperdex_failure(reqid);
}

void
pending_rename :: handle_wtf_failure(const server_id& sid)
{
    TRACE;
    pending_aggregation::handle_wtf_failure(sid);
}

bool
pending_rename :: try_op()
{
    TRACE;
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
        m_search_id = msg->reqid();
        m_cl->add_hyperdex_op(msg->reqid(), this);
        e::intrusive_ptr<message> m = msg.get();
        handle_sent_to_hyperdex(m);
    }

    return true;
}
