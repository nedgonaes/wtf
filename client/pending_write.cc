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
#include "client/pending_write.h"
#include "common/response_returncode.h"
#include "client/message_hyperdex_get.h"
#include "client/message_hyperdex_put.h"

using wtf::pending_write;

pending_write :: pending_write(client* cl, uint64_t id, e::intrusive_ptr<file> f,
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

pending_write :: ~pending_write() throw ()
{
    TRACE;
}

bool
pending_write :: can_yield()
{
    TRACE;
    return this->aggregation_done() && !m_done;
}

bool
pending_write :: yield(wtf_client_returncode* status, e::error* err)
{
    TRACE;
    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();
    assert(this->can_yield());
    m_done = true;
    return true;
}

void
pending_write :: handle_wtf_failure(const server_id& si)
{
    TRACE;
    PENDING_ERROR(RECONFIGURE) << "reconfiguration affecting "
                               << si;
    return pending_aggregation::handle_wtf_failure(si);
}

bool
pending_write :: handle_wtf_message(client* cl,
                                    const server_id& si,
                                    std::auto_ptr<e::buffer>,
                                    e::unpacker up,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    bool handled = pending_aggregation::handle_wtf_message(cl, si, std::auto_ptr<e::buffer>(), up, status, err);
    assert(handled);

    /* 
     * We take these messages until the last one is received, then request to update
     * the metadata from hyperdex.
     */

    uint64_t bi;
    uint64_t file_offset;
    uint32_t block_capacity;
    uint64_t block_length;
    e::intrusive_ptr<block> bl;
    response_returncode rc;
    up = up >> rc >> bi >> file_offset >> block_capacity >> block_length;

    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();

    changeset_t::iterator it = m_changeset.find(file_offset);

    if (it == m_changeset.end())
    {
        bl = new block(block_capacity, file_offset, 0);
        bl->set_length(block_length);
        bl->set_offset(file_offset);
        m_changeset[file_offset] = bl;
    }
    else
    {
        bl = it->second;
    }

    bl->add_replica(block_location(si.get(), bi));

    if (this->aggregation_done())
    {
        /* This applying change set and putting file metadata
           should be done atomically so that no other op
           can modify the metadata after we apply our changes
           but before we update hyperdex */ 
          send_metadata_update(); 
   }

    return true;
}

bool
pending_write :: try_op()
{
    TRACE;
    /* Get the file metadata from HyperDex */
    const char* path = m_file->path().get();
    e::intrusive_ptr<message_hyperdex_get> msg =
        new message_hyperdex_get(m_cl, "wtf", path); 
       
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
pending_write :: handle_hyperdex_message(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    TRACE;
    //response from initial get
    if (m_state == 0)
    {

        size_t rem = *m_buf_sz;
        size_t next_buf_offset = 0;

        while(rem > 0)
        {
            std::vector<block_location> bl;
            size_t buf_offset = next_buf_offset;
            uint32_t block_offset;
            uint32_t block_capacity;
            uint64_t file_offset;
            size_t slice_len;
            m_cl->prepare_write_op(m_file, rem, bl, next_buf_offset, block_offset, block_capacity, file_offset, slice_len);
            e::slice data = e::slice(m_buf + buf_offset, slice_len);

            for (size_t i = 0; i < bl.size(); ++i)
            {
                size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
                    + sizeof(uint64_t) // bl.bi (remote block number) 
                    + sizeof(uint32_t) // block_offset (remote block offset) 
                    + sizeof(uint32_t) // block_capacity 
                    + sizeof(uint64_t) // file_offset 
                    + data.size();     // user data 
                std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
                e::buffer::packer pa = msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ);
                pa = pa << bl[i].bi << block_offset << block_capacity << file_offset;
                pa.copy(data);

                std::vector<server_id> servers;
                servers.push_back(server_id(bl[i].si));

                //SEND
                m_cl->perform_aggregation(servers, this, REQ_UPDATE, msg, status);
            }
        }

        pending_aggregation::handle_hyperdex_message(cl, reqid, rc, status, err);
        m_state = 1;
        return true;
    }
    //response from final metadata update
    else
    {
        e::intrusive_ptr<message_hyperdex_put> msg = dynamic_cast<message_hyperdex_put*>(m_outstanding_hyperdex[0].get());
        pending_aggregation::handle_hyperdex_message(cl, reqid, rc, status, err);
        return true;
    }
}

 
typedef struct hyperdex_ds_arena* arena_t;
typedef struct hyperdex_client_attribute* attr_t;


void
pending_write :: send_metadata_update()
{
    TRACE;
    m_file->apply_changeset(m_changeset);
    //XXX set attrs and attrs_sz to file metadata with new changes
    // see update_file_metadata in client.cc

    size_t sz;

    std::auto_ptr<e::buffer> blockmap_update = m_file->serialize_blockmap();
    uint64_t mode = m_file->mode;
    uint64_t directory = m_file->is_directory;

    hyperdex_ds_returncode status;
    arena_t arena = hyperdex_ds_arena_create();
    attr_t attrs = hyperdex_ds_allocate_attribute(arena, 3);

    attrs[0].datatype = HYPERDATATYPE_INT64;
    hyperdex_ds_copy_string(arena, "mode", 5,
                            &status, &attrs[0].attr, &sz);
    hyperdex_ds_copy_int(arena, mode, 
                            &status, &attrs[0].value, &attrs[0].value_sz);

    attrs[1].datatype = HYPERDATATYPE_INT64;
    hyperdex_ds_copy_string(arena, "directory", 10,
                            &status, &attrs[1].attr, &sz);
    hyperdex_ds_copy_int(arena, directory, 
                            &status, &attrs[1].value, &attrs[1].value_sz);

    attrs[2].datatype = HYPERDATATYPE_STRING;
    hyperdex_ds_copy_string(arena, "blockmap", 9,
                            &status, &attrs[2].attr, &sz);
    hyperdex_ds_copy_string(arena, 
                            reinterpret_cast<const char*>(blockmap_update->data()), 
                            blockmap_update->size(),
                            &status, &attrs[2].value, &attrs[2].value_sz);

    e::intrusive_ptr<message_hyperdex_put> msg = 
        new message_hyperdex_put(m_cl, "wtf", m_file->path().get(), arena, attrs, 3);

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
}
