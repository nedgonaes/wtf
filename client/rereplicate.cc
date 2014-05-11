// Copyright (c) 2013, Sean Ogden
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

#include "client/rereplicate.h"
#include "client/constants.h"
#include "client/pending_read.h"
#include "client/pending_write.h"
#include "client/file.h"

#ifdef TRACECALLS
#define TRACE std::cerr << __FILE__ << ":" << __func__ << std::endl
#else
#define TRACE
#endif

using namespace std;
using wtf::rereplicate;

rereplicate :: rereplicate(const char* host, in_port_t port,
                         const char* hyper_host, in_port_t hyper_port)
{
    wc = new client(host, port, hyper_host, hyper_port);
    TRACE;
}

rereplicate :: ~rereplicate() throw ()
{
    delete wc;
    TRACE;
}

int64_t
rereplicate :: replicate(const char* filename, uint64_t sid)
{
    cout << "filename " << filename << " sid " << sid << endl;
    int64_t ret;
    int64_t reqid;

    e::intrusive_ptr<wtf::file> f = new wtf::file(filename, 0, CHUNKSIZE);
    ret = wc->get_file_metadata(f->path().get(), f, false);

    if (ret < 0)
    {
        return -1;
    }

    std::map<uint64_t, e::intrusive_ptr<wtf::block> >::const_iterator it;
    for (it = f->blocks_begin(); it != f->blocks_end(); ++it)
    {
        bool match = false;
        std::set<block_location> location_set;
        std::vector<wtf::block_location>::iterator it2;
        std::vector<wtf::block_location> block_locations = it->second->block_locations();
        for (it2 = block_locations.begin(); it2 != block_locations.end(); ++it2)
        {
            if (it2->si == sid)
            {
                cout << "match " << *it2 << endl;
                *it2 = wtf::block_location();
                match = true;
            }
            else
            {
                cout << "no match " << *it2 << endl;
                location_set.insert(*it2);
            }
        }
        if (match)
        {
            wtf_client_returncode status;
            size_t buf_sz = it->second->length();
            char buf[buf_sz];
            std::vector<server_id> servers;
            set<block_location>::const_iterator location_set_it = location_set.begin();
            servers.push_back(server_id(location_set_it->si));

            // Read block
            reqid = wc->m_next_client_id++;
            e::intrusive_ptr<pending_read> read_op = new pending_read(reqid, &status, buf, &buf_sz);

            size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
                + sizeof(uint64_t)  // bi (local block number)
                + sizeof(uint32_t); // block_length
            std::auto_ptr<e::buffer> read_msg(e::buffer::create(sz));
            read_msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ) << (uint64_t)location_set_it->bi << (uint32_t)it->second->length();

            if (!wc->maintain_coord_connection(&status))
            {
                return -1;
            }

            buf_sz = 0;
            wc->perform_aggregation(servers, read_op.get(), REQ_GET, read_msg, &status);

            reqid = wc->loop(reqid, -1, &status);
            if (reqid < 0)
            {
                cout << "read failed" << endl;
                return -1;
            }

            e::slice data = e::slice(buf, buf_sz);
            cout << "buf_sz read " << buf_sz << endl << data.hex() << endl;


            // Write block
            reqid = wc->m_next_client_id++;
            e::intrusive_ptr<pending_aggregation> write_op = new pending_write(reqid, f, &status);
            uint32_t block_capacity = 4096;
            uint64_t file_offset = it->second->offset();

            wc->m_coord.config()->assign_random_block_locations(block_locations);
            std::vector<wtf::block_location>::iterator it3;
            for (it3 = block_locations.begin(); it3 != block_locations.end(); ++it3)
            {
                if (location_set.find(*it3) == location_set.end())
                {
                    size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
                        + sizeof(uint64_t) // bi (remote block number) 
                        + sizeof(uint32_t) // block_offset (remote block offset) 
                        + sizeof(uint32_t) // block_capacity 
                        + sizeof(uint64_t) // file_offset 
                        + data.size();     // user data 
                    std::auto_ptr<e::buffer> write_msg(e::buffer::create(sz));
                    e::buffer::packer pa = write_msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ);
                    pa = pa << (uint64_t)it3->bi << (uint32_t)0 << block_capacity << file_offset;
                    pa.copy(data);
                    cout << "server " << it3->si << " bi " << it3->bi << " block_offset 0 block_capacity " << block_capacity << " file_offset " << file_offset << " data size " << data.size() << endl;

                    if (!wc->maintain_coord_connection(&status))
                    {
                        return -1;
                    }

                    std::vector<server_id> servers;
                    servers.push_back(server_id(it3->si));
                    wc->perform_aggregation(servers, write_op, REQ_UPDATE, write_msg, &status);

                    reqid = wc->loop(reqid, -1, &status);
                    if (reqid < 0)
                    {
                        cout << "write failed" << endl;
                        return -1;
                    }
                    cout << "file after write" << endl << *(f.get()) << endl;
                }
            }
            std::auto_ptr<e::buffer> old_blockmap = f->serialize_blockmap();
            pending_write* write_op_downcasted = static_cast<pending_write*>(write_op.get());
            std::map<uint64_t, e::intrusive_ptr<block> >::iterator changeset_it = write_op_downcasted->m_changeset.find(file_offset);
            e::intrusive_ptr<block> bl;

            if (changeset_it == write_op_downcasted->m_changeset.end())
            {
                cout << "changeset is empty" << endl;
                bl = new block(block_capacity, file_offset, 0);
                bl->set_length(data.size());
                bl->set_offset(file_offset);
                write_op_downcasted->m_changeset[file_offset] = bl;
            }
            else
            {
                cout << "changeset is not empty" << endl;
                bl = changeset_it->second;
            }

            for (location_set_it = location_set.begin(); location_set_it != location_set.end(); ++location_set_it)
            {
                bl->add_replica(*location_set_it);
            }
            f->apply_changeset(write_op_downcasted->m_changeset);
            wc->update_file_metadata(f, reinterpret_cast<const char*>(old_blockmap->data()), old_blockmap->size(), &status);
        }
    }
    return 0;
}

