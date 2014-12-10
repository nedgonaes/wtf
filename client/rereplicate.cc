// Copyright (c) 2014, Cornell University
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

#include <set>
#include "client/rereplicate.h"
#include "client/constants.h"
#include "client/pending_read.h"
#include "client/pending_write.h"
#include "client/file.h"

using wtf::rereplicate;

rereplicate :: rereplicate(const char* host, in_port_t port,
                         const char* hyper_host, in_port_t hyper_port)
{
    wc = new client(host, port, hyper_host, hyper_port);
}

rereplicate :: ~rereplicate() throw ()
{
    delete wc;
}

int64_t
rereplicate :: replicate_one(const char* path, uint64_t sid)
{
    wtf_client_returncode w_status;
    wtf_client_returncode l_status;

    // Check if daemon has actually failed
    if (!wc->maintain_coord_connection(&w_status))
    {
        std::cerr << "Failed to read reach coordinator" << std::endl;
        return -1;
    }
    wtf::server::state_t state = wc->m_coord.config()->get_state(server_id(sid));
    if (state == wtf::server::AVAILABLE)
    {
        std::cerr << "Daemon " << sid << " is still available; not replicating" << std::endl;
        return 0;
    }

    int64_t ret;
    int64_t reqid;

    e::intrusive_ptr<wtf::file> f;

    while (true)
    {
        
        int64_t fd;
        ret = wc->open(f->path().get(), O_RDONLY, 0, 0, 0, &fd, &w_status);

        if (ret < 0)
        {
            std::cerr << "Failed to read file metadata" << std::endl;
            return -1;
        }

        ret = wc->loop(ret, -1, &l_status);

        if (ret < 0)
        {
            std::cerr << "Failed to read file metadata" << std::endl;
            return -1;
        }

        f = wc->m_fds[fd];

        // If path is a directory, ignore
        if (f->is_directory)
        {
            break;
        }

        // Find first block with location(s) matching sid, replace location(s) with placeholder,
        // and assign new locations
        std::map<uint64_t, e::intrusive_ptr<wtf::block> >::const_iterator it;
        bool match = false;
        for (it = f->blocks_begin(); it != f->blocks_end(); ++it)
        {
            std::set<block_location> location_set;
            std::vector<wtf::block_location>::iterator it2;
            std::vector<wtf::block_location> block_locations = it->second->block_locations();
            for (it2 = block_locations.begin(); it2 != block_locations.end(); ++it2)
            {
                if (it2->si == sid)
                {
                    *it2 = wtf::block_location();
                    match = true;
                }
                else
                {
                    location_set.insert(*it2);
                }
            }
            if (match)
            {

                size_t buf_sz = it->second->length();
                char buf[buf_sz];
                std::vector<server_id> read_servers;
                std::set<block_location>::const_iterator location_set_it = location_set.begin();
                read_servers.push_back(server_id(location_set_it->si));

                // Read block
                reqid = wc->m_next_client_id++;
                e::intrusive_ptr<pending_read> read_op = new pending_read(wc, reqid, f, buf, &buf_sz, &w_status);

                size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
                    + sizeof(uint64_t)  // local block number
                    + sizeof(uint32_t); // block length
                std::auto_ptr<e::buffer> read_msg(e::buffer::create(sz));
                read_msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ) << (uint64_t)location_set_it->bi << (uint32_t)it->second->length();

                if (!wc->maintain_coord_connection(&w_status))
                {
                    std::cerr << "Failed to read reach coordinator" << std::endl;
                    return -1;
                }

                buf_sz = 0;
                wc->perform_aggregation(read_servers, read_op.get(), REQ_GET, read_msg, &w_status);

                reqid = wc->loop(reqid, -1, &w_status);
                if (reqid < 0)
                {
                    std::cerr << "Failed to read from daemon" << std::endl;
                    return -1;
                }

                e::slice data = e::slice(buf, buf_sz);


                // Write block
                reqid = wc->m_next_client_id++;
                //XXX: fix later.
                e::intrusive_ptr<pending_aggregation> write_op = NULL;//new pending_write(wc, reqid, f, buf, &buf_sz, &w_status);
                pending_write* write_op_downcasted = static_cast<pending_write*>(write_op.get());
                uint32_t block_capacity = 4096;
                uint64_t file_offset = it->second->offset();

                wc->m_coord.config()->assign_random_block_locations(block_locations, wc->m_addr);
                std::vector<wtf::block_location>::iterator it3;
                std::vector<server_id> write_servers;
                for (it3 = block_locations.begin(); it3 != block_locations.end(); ++it3)
                {
                    if (location_set.find(*it3) == location_set.end())
                    {
                        write_servers.push_back(server_id(it3->si));
                    }
                }
                if (write_servers.size() > 0)
                {
                    size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
                        + sizeof(uint64_t) // placeholder
                        + sizeof(uint32_t) // remote block offset
                        + sizeof(uint32_t) // block capacity
                        + sizeof(uint64_t) // file offset
                        + data.size();     // block data size
                    std::auto_ptr<e::buffer> write_msg(e::buffer::create(sz));
                    e::buffer::packer pa = write_msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ);
                    pa = pa << (uint64_t)wtf::block_location().bi << (uint32_t)0 << block_capacity << file_offset;
                    pa.copy(data);
                    if (!wc->maintain_coord_connection(&w_status))
                    {
                        std::cerr << "Failed to read reach coordinator" << std::endl;
                        return -1;
                    }
                    wc->perform_aggregation(write_servers, write_op, REQ_UPDATE, write_msg, &w_status);

                    reqid = wc->loop(reqid, -1, &w_status);
                    if (reqid < 0)
                    {
                        std::cerr << "Failed to write to daemon" << std::endl;
                        return -1;
                    }
                }

                // Update block metadata with existing locations containing data
                std::auto_ptr<e::buffer> old_blockmap = f->serialize_blockmap();
                std::map<uint64_t, e::intrusive_ptr<block> >::iterator changeset_it = write_op_downcasted->m_changeset.find(file_offset);
                e::intrusive_ptr<block> bl;

                if (changeset_it == write_op_downcasted->m_changeset.end())
                {
                    bl = new block(block_capacity, file_offset, 0);
                    bl->set_length(data.size());
                    bl->set_offset(file_offset);
                    write_op_downcasted->m_changeset[file_offset] = bl;
                }
                else
                {
                    bl = changeset_it->second;
                }

                for (location_set_it = location_set.begin(); location_set_it != location_set.end(); ++location_set_it)
                {
                    bl->add_replica(*location_set_it);
                }
                f->apply_changeset(write_op_downcasted->m_changeset);
                //XXX
                //wc->update_file_metadata(f, reinterpret_cast<const char*>(old_blockmap->data()), old_blockmap->size(), &w_status);

                // Re-replicate one block, commit, and repeat
                break;
            }
        }

        if (!match)
        {
            break;
        }
    }
    return 0;
}

int64_t
rereplicate :: replicate_all(uint64_t sid, const char* hyper_host, in_port_t hyper_port)
{
    wtf_client_returncode w_status;

    // Check if daemon has actually failed
    if (!wc->maintain_coord_connection(&w_status))
    {
        std::cerr << "Failed to read reach coordinator" << std::endl;
        return -1;
    }
    wtf::server::state_t state = wc->m_coord.config()->get_state(server_id(sid));
    if (state == wtf::server::AVAILABLE)
    {
        std::cerr << "Daemon " << sid << " is still available; not replicating" << std::endl;
        return 0;
    }

    // Search every file in the file system and call replicate_one
    // TODO Use wc's HyperDex client instead (creating new client currently to avoid multiple outstanding requests)
    hyperdex::Client* h = new hyperdex::Client(hyper_host, hyper_port);
    hyperdex_client_returncode h_status;
    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    int64_t retval;

    struct hyperdex_client_attribute_check check;
    check.attr = "path";
    check.value = "^";
    check.value_sz = strlen(check.value);
    check.datatype = HYPERDATATYPE_STRING;
    check.predicate = HYPERPREDICATE_REGEX;

    retval = h->search("wtf", &check, 1, &h_status, &attrs, &attrs_sz);
    while (true)
    {
        retval = h->loop(-1, &h_status);
        if (h_status != HYPERDEX_CLIENT_SUCCESS)
        {
            break;
        }
        for (size_t i = 0; i < attrs_sz; ++i)
        {
            if (strcmp(attrs[i].attr, "path") == 0)
            {
                std::string path(attrs[i].value, attrs[i].value_sz);
                replicate_one(path.c_str(), sid);
            }
        }
    }

    return 0;
}

