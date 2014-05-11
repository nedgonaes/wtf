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
    int64_t client_id = 0;

    e::intrusive_ptr<wtf::file> f = new wtf::file(filename, 0, CHUNKSIZE);
    ret = wc->get_file_metadata(f->path().get(), f, false);

    if (ret < 0)
    {
        return -1;
    }

    std::auto_ptr<e::buffer> old_blockmap = f->serialize_blockmap();
    std::map<uint64_t, e::intrusive_ptr<wtf::block> >::const_iterator it;
    for (it = f->blocks_begin(); it != f->blocks_end(); ++it)
    {
        bool match = false;
        std::set<block_location> location_set;
        std::vector<wtf::block_location>::iterator it2;
        for (it2 = it->second->blocks_begin(); it2 != it->second->blocks_end(); ++it2)
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
            cout << "buf_sz created " << buf_sz << endl;
            std::vector<server_id> servers;
            set<block_location>::const_iterator location_set_it = location_set.begin();
            servers.push_back(server_id(location_set_it->si));

            // Read block
            client_id++;
            e::intrusive_ptr<pending_read> read_op = new pending_read(client_id, &status, buf, &buf_sz);

            size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
                + sizeof(uint64_t)  // bi (local block number)
                + sizeof(uint32_t); // block_length
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ) << location_set_it->bi << (uint32_t)it->second->length();

            if (!wc->maintain_coord_connection(&status))
            {
                return -1;
            }

            buf_sz = 0;
            wc->perform_aggregation(servers, read_op.get(), REQ_GET, msg, &status);

            wc->loop(client_id, -1, &status);
            e::slice data = e::slice(buf, buf_sz);
            cout << "buf_sz read " << buf_sz << endl << data.hex() << endl;


            // Write block
            client_id++;
            e::intrusive_ptr<pending_aggregation> write_op = new pending_write(client_id, f, &status);

            std::vector<wtf::block_location> modified_blocks = it->second->blocks();
            wc->m_coord.config()->assign_random_block_locations(modified_blocks);
            it->second->set_blocks(modified_blocks);
            for (it2 = it->second->blocks_begin(); it2 != it->second->blocks_end(); ++it2)
            {
                if (location_set.find(*it2) == location_set.end())
                {
                    cout << "server " << it2->si << " block " << it2->bi << endl;
                }
            }
            //std::vector<block_location> bl;
            //uint32_t block_offset;
            //uint32_t block_capacity;
            //uint64_t file_offset;
            //size_t slice_len;
            //prepare_write_op(f, rem, bl, next_buf_offset, block_offset, block_capacity, file_offset, slice_len);
            //e::slice data = e::slice(buf, slice_len);

            //for (size_t i = 0; i < bl.size(); ++i)
            //{
            //    size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
            //        + sizeof(uint64_t) // bl.bi (remote block number) 
            //        + sizeof(uint32_t) // block_offset (remote block offset) 
            //        + sizeof(uint32_t) // block_capacity 
            //        + sizeof(uint64_t) // file_offset 
            //        + data.size();     // user data 
            //    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            //    e::buffer::packer pa = msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ);
            //    pa = pa << bl[i].bi << block_offset << block_capacity << file_offset;
            //    pa.copy(data);

            //    if (!wc->maintain_coord_connection(&status))
            //    {
            //        return -1;
            //    }

            //    std::vector<server_id> servers;
            //    servers.push_back(server_id(bl[i].si));
            //    wc->perform_aggregation(servers, write_op, REQ_UPDATE, msg, &status);
            //}
        }
    }

    //wtf::Client wc("127.0.0.1", 1981, "127.0.0.1", 1982);
    //wtf_client_returncode s;
    //if (!wc.maintain_coord_connection(&s)) { cout << "NOOOOOOOOOOOOOOOOO" << endl; } else { cout << "YESSSSSSSSSSSSSSSS" << endl; }
    /*
    uint64_t mode = f->mode;
    uint64_t directory = f->is_directory;
    std::auto_ptr<e::buffer> new_blockmap = f->serialize_blockmap();
    struct hyperdex_client_attribute update_attr[3];

    update_attr[0].attr = "mode";
    update_attr[0].value = (const char*)&mode;
    update_attr[0].value_sz = sizeof(mode);
    update_attr[0].datatype = HYPERDATATYPE_INT64;

    update_attr[1].attr = "directory";
    update_attr[1].value = (const char*)&directory;
    update_attr[1].value_sz = sizeof(directory);
    update_attr[1].datatype = HYPERDATATYPE_INT64;

    update_attr[2].attr = "blockmap";
    update_attr[2].value = reinterpret_cast<const char*>(new_blockmap->data());
    update_attr[2].value_sz = new_blockmap->size();
    update_attr[2].datatype = HYPERDATATYPE_STRING;

    struct hyperdex_client_attribute_check cond_attr;

    cond_attr.attr = "blockmap";
    cond_attr.value = reinterpret_cast<const char*>(old_blockmap->data());
    cond_attr.value_sz = old_blockmap->size();
    cond_attr.datatype = HYPERDATATYPE_STRING;
    cond_attr.predicate = HYPERPREDICATE_EQUALS;

    ret = wc.m_hyperdex_client.cond_put("wtf", f->path().get(), strlen(f->path().get()), &cond_attr, 1,
                                     update_attr, 3, &status);

    print_return();

    wtf::Client wc("127.0.0.1", 1981, "127.0.0.1", 1982);
    wtf_client_returncode s;
    int64_t fd = wc.open(filename, O_RDONLY, 0, 0, 0, &s);
    char buf[4096];

    size_t size;
    int64_t reqid;
    do {
        size = 4096;
        reqid = wc.read(fd, buf, &size, &s);
        reqid = wc.loop(reqid, -1, &s);
        e::slice data = e::slice(buf, size);
        cout << "reqid " << reqid << " read " << size << " status " << s << endl;
        cout << data.hex() << endl;
        reqid = wc.write(fd, buf, &size, 0, &s);
        reqid = wc.loop(reqid, -1, &s);
    } while (false);

    wc.close(fd, &s);
    */

    return 0;
}

