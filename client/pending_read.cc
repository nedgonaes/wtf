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
#include "client/constants.h"
#include "client/client.h"
#include "common/block.h"
#include "client/pending_read.h"
#include "common/response_returncode.h"
#include "client/message_hyperdex_get.h"
#include "client/message_hyperdex_put.h"

using wtf::pending_read;

pending_read :: pending_read(client* cl, uint64_t id, e::intrusive_ptr<file> f,
                               char* buf, size_t* buf_sz, 
                               wtf_client_returncode* status)
    : pending_aggregation(id, status)
    , m_cl(cl)
    , m_buf(buf)
    , m_buf_sz(buf_sz)
    , m_max_buf_sz(*buf_sz)
    , m_file(f)
    , m_done(false)
    , m_state(0)
    , m_offset_map()
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
pending_read :: handle_wtf_failure(const server_id& si)
{
    PENDING_ERROR(RECONFIGURE) << "reconfiguration affecting "
                               << si;
    return pending_aggregation::handle_wtf_failure(si);
}

bool
pending_read :: handle_wtf_message(client* cl,
                                    const server_id& si,
                                    std::auto_ptr<e::buffer>,
                                    e::unpacker up,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    bool handled = pending_aggregation::handle_wtf_message(cl, si, std::auto_ptr<e::buffer>(), up, status, err);
    assert(handled);

    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();

    /* Put data in client's buffer. */
    uint64_t bi;
    response_returncode rc;
    up = up >> rc >> bi;
    struct buffer_block_len bbl = m_offset_map[std::make_pair(si.get(), bi)];
    e::slice data = up.as_slice();
    size_t len = std::min(data.size() - bbl.block_offset, m_max_buf_sz - bbl.buf_offset);
    memmove(m_buf + bbl.buf_offset, data.data() + bbl.block_offset, len); 
    *m_buf_sz += len; 
    return true;
}

bool
pending_read :: try_op()
{
    /* Get the file metadata from HyperDex */
    const char* path = m_file->path().get();
    e::intrusive_ptr<message_hyperdex_get> msg =
        new message_hyperdex_get(m_cl, "wtf", path); 
       
    if (msg->send() < 0)
    {
        PENDING_ERROR(IO) << "Couldn't get from HyperDex: " << msg->status();
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
pending_read :: handle_hyperdex_message(client* cl,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    //response from initial get
    if (m_state == 0)
    {
        e::intrusive_ptr<message_hyperdex_get> msg = dynamic_cast<message_hyperdex_get*>(m_outstanding_hyperdex[0].get());
        const hyperdex_client_attribute* attrs = msg->attrs();
        size_t attrs_sz = msg->attrs_sz();
        parse_metadata(attrs, attrs_sz);
        size_t rem = std::min(*m_buf_sz, m_file->length() - m_file->offset());

        size_t buf_offset = 0;
        *m_buf_sz = 0;

        while(rem > 0)
        {
            block_location bl;
            uint32_t block_length;
            std::vector<server_id> servers;
            //m_cl->prepare_read_op(m_file, rem, buf_offset, bl, block_length, this, servers);
            size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
                + sizeof(uint64_t) // bl.bi (local block number) 
                + sizeof(uint32_t); //block_length
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ) << bl.bi << block_length;

            m_cl->perform_aggregation(servers, this, REQ_GET, msg, status);
        }

        pending_aggregation::handle_hyperdex_message(cl, reqid, rc, status, err);
        m_state = 1;
    }
    //response from final metadata update
    else
    {
        pending_aggregation::handle_hyperdex_message(cl, reqid, rc, status, err);
    }

    return true;
}

void
pending_read :: parse_metadata(const hyperdex_client_attribute* attrs, size_t attrs_sz)
{
    for (size_t i = 0; i < attrs_sz; ++i)
    {
        if (strcmp(attrs[i].attr, "blockmap") == 0)
        {
            e::unpacker up(attrs[i].value, attrs[i].value_sz);

            if (attrs[i].value_sz == 0)
            {
                continue;
            }

            up = up >> m_file;
        }
        else if (strcmp(attrs[i].attr, "directory") == 0)
        {
            uint64_t is_dir;

            e::unpacker up(attrs[i].value, attrs[i].value_sz);
            up = up >> is_dir;
            e::unpack64be((uint8_t*)&is_dir, &is_dir);

            if (is_dir == 0)
            {
                m_file->is_directory = false;
            }
            else
            {
                m_file->is_directory = true;
            }
        }
        else if (strcmp(attrs[i].attr, "mode") == 0)
        {
            uint64_t mode;

            e::unpacker up(attrs[i].value, attrs[i].value_sz);
            up = up >> mode;
            e::unpack64be((uint8_t*)&mode, &mode);
            m_file->mode = mode;
        }
    }
} 

void 
pending_read :: set_offset(const uint64_t si,
                const uint64_t bi,
                const size_t buf_offset,   //offset in client buffer
                const size_t block_offset, //offset from start of block
                const size_t len)         //how many bytes to copy
{
    m_offset_map[std::make_pair(si, bi)] = buffer_block_len(buf_offset, block_offset, len);
}
