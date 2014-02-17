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
#include "client/pending_write.h"
#include "common/response_returncode.h"

using wtf::pending_write;

pending_write :: pending_write(uint64_t id, e::intrusive_ptr<file> f,
                                       wtf_client_returncode* status)
    : pending_aggregation(id, status)
    , m_old_blockmap(f->serialize_blockmap())
    , m_file(f)
    , m_done(false)
{
    set_status(WTF_CLIENT_SUCCESS);
    set_error(e::error());
}

pending_write :: ~pending_write() throw ()
{
}

bool
pending_write :: can_yield()
{
    return this->aggregation_done() && !m_done;
}

bool
pending_write :: yield(wtf_client_returncode* status, e::error* err)
{
    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();
    assert(this->can_yield());
    m_done = true;
    return true;
}

void
pending_write :: handle_failure(const server_id& si)
{
    PENDING_ERROR(RECONFIGURE) << "reconfiguration affecting "
                               << si;
    return pending_aggregation::handle_failure(si);
}

bool
pending_write :: handle_message(client* cl,
                                    const server_id& si,
                                    wtf_network_msgtype mt,
                                    std::auto_ptr<e::buffer>,
                                    e::unpacker up,
                                    wtf_client_returncode* status,
                                    e::error* err)
{
    bool handled = pending_aggregation::handle_message(cl, si, mt, std::auto_ptr<e::buffer>(), up, status, err);
    assert(handled);

    uint64_t bi;
    uint64_t file_offset;
    uint32_t block_capacity;
    uint64_t block_length;
    response_returncode rc;
    up = up >> rc >> bi >> file_offset >> block_capacity >> block_length;

    *status = WTF_CLIENT_SUCCESS;
    *err = e::error();

    if (mt != RESP_UPDATE)
    {
        PENDING_ERROR(SERVERERROR) << "server " << si << " responded to UPDATE with " << mt;
        return true;
    }

    changeset_t::iterator it = m_changeset.find(file_offset);
    e::intrusive_ptr<block> bl;

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
           
        m_file->apply_changeset(m_changeset);

        wtf_client_returncode cstatus;

        if (cl->update_file_metadata(m_file, reinterpret_cast<const char*>(m_old_blockmap->data()), 
                                     m_old_blockmap->size(), &cstatus))
        {
            CLIENT_ERROR(cstatus);
            return true;
        }
    }

    return true;
}
