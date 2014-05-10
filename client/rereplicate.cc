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
    : m_hyperdex_client(hyper_host, hyper_port)
{
	TRACE;
}

rereplicate :: ~rereplicate() throw ()
{
	TRACE;
}

int64_t
rereplicate :: replicate(const char* filename, uint64_t sid)
{
    cout << "filename " << filename << " sid " << sid << endl;
    int64_t retval;
    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    hyperdex_client_returncode status;

    retval = m_hyperdex_client.get("wtf", filename, strlen(filename), &status, &attrs, &attrs_sz);
    printf("first retval %ld status %d:", retval, status);
    cout << status << endl;
    retval = m_hyperdex_client.loop(-1, &status);
    printf("final retval %ld status %d:", retval, status);
    cout << status << endl;

    if (status != HYPERDEX_CLIENT_SUCCESS)
    {
        cout << "failed to get file from hyperdex" << endl;
        return -1;
    }

    if (attrs == NULL)
    {
        cout << "attrs is NULL" << endl;
        return -1;
    }

    e::intrusive_ptr<wtf::file> f = new wtf::file(filename, 0, CHUNKSIZE);
    for (size_t i = 0; i < attrs_sz; ++i)
    {
        if (strcmp(attrs[i].attr, "blockmap") == 0)
        {
            e::unpacker up(attrs[i].value, attrs[i].value_sz);

            if (attrs[i].value_sz == 0)
            {
                continue;
            }

            up = up >> f;
        }
        else if (strcmp(attrs[i].attr, "directory") == 0)
        {
            uint64_t is_dir;

            e::unpacker up(attrs[i].value, attrs[i].value_sz);
            up = up >> is_dir;
            e::unpack64be((uint8_t*)&is_dir, &is_dir);

            if (is_dir == 0)
            {
                f->is_directory = false;
            }
            else
            {
                f->is_directory = true;
            }
        }
        else if (strcmp(attrs[i].attr, "mode") == 0)
        {
            uint64_t mode;

            e::unpacker up(attrs[i].value, attrs[i].value_sz);
            up = up >> mode;
            e::unpack64be((uint8_t*)&mode, &mode);
            f->mode = mode;
        }
    }

    std::auto_ptr<e::buffer> old_blockmap = f->serialize_blockmap();

    std::map<uint64_t, e::intrusive_ptr<wtf::block> >::const_iterator it;
    for (it = f->blocks_begin(); it != f->blocks_end(); ++it)
    {
        std::vector<wtf::block_location>::iterator it2;
        for (it2 = it->second->blocks_begin(); it2 != it->second->blocks_end(); ++it2)
        {
            if (it2->si == sid)
            {
                cout << "match " << *it2 << endl;
                //*it2 = wtf::block_location();
            }
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

    retval = m_hyperdex_client.cond_put("wtf", f->path().get(), strlen(f->path().get()), &cond_attr, 1,
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

