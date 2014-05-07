#define __STDC_LIMIT_MACROS

// C/C++
#include <stdio.h>
#include <string.h>
#include <map>

// E
#include <e/endian.h>
#include <e/unpacker.h>
#include <e/intrusive_ptr.h>
#include <e/popt.h>

// HyperDex
#include <hyperdex/client.hpp>

// WTF 
#include "client/file.h"
#include <wtf/client.hpp>

using namespace std;

static long _hyper_port = 1982;
static const char* _hyper_host = "127.0.0.1";
static const char* _query = "";
static bool _verbose = true;

hyperdex::Client* h;
const char* WTF_SPACE = "wtf";
const struct hyperdex_client_attribute* attrs;
size_t attrs_sz;
hyperdex_client_returncode status;
int64_t retval;

void
print_return()
{
    if (_verbose) printf("first retval %ld status %d:", retval, status);
    if (_verbose) cout << status << endl;
    retval = h->loop(-1, &status);
    if (_verbose) printf("final retval %ld status %d:", retval, status);
    if (_verbose) cout << status << endl;
}

int64_t
rereplicate(const char* filename, uint64_t sid)
{
    cout << "filename " << filename << " sid " << sid << endl;
    h = new hyperdex::Client(_hyper_host, _hyper_port);
    retval = h->get("wtf", filename, strlen(filename), &status, &attrs, &attrs_sz);
    print_return();

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
                *it2 = wtf::block_location();
            }
        }
    }

    /* construct the attributes for the new metadata */
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

    /* construct the attributes for the cond_put condition */
    struct hyperdex_client_attribute_check cond_attr;

    cond_attr.attr = "blockmap";
    cond_attr.value = reinterpret_cast<const char*>(old_blockmap->data());
    cond_attr.value_sz = old_blockmap->size();
    cond_attr.datatype = HYPERDATATYPE_STRING;
    cond_attr.predicate = HYPERPREDICATE_EQUALS;

    retval = h->cond_put("wtf", f->path().get(), strlen(f->path().get()), &cond_attr, 1,
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

    return 0;
}

int
main(int argc, const char* argv[])
{
    if (argv[1] == NULL || argv[2] == NULL)
    {
        cout << "insufficient arguments" << endl;
        return EXIT_FAILURE;
    }
    else
    {
        const char* filename = argv[1];
        uint64_t sid = strtoull(argv[2], NULL, 10);
        int64_t ret = rereplicate(filename, sid);

        return ret;
    }
}
