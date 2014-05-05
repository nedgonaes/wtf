//#define __STDC_LIMIT_MACROS

// C/C++
#include <stdio.h>
#include <string.h>
#include <map>
//#include <stdint.h>

// E
#include <e/endian.h>
#include <e/unpacker.h>
#include <e/intrusive_ptr.h>
#include <e/popt.h>

// HyperDex
#include <hyperdex/client.hpp>

// WTF 
#include "client/file.h"

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

    e::intrusive_ptr<wtf::file> f = new wtf::file("", 0, CHUNKSIZE);
    for (size_t i = 0; i < attrs_sz; ++i)
    {
        if (strcmp(attrs[i].attr, "blockmap") == 0)
        {
            e::unpacker up(attrs[i].value, attrs[i].value_sz);
            up = up >> f;
        }
    }

    std::map<uint64_t, e::intrusive_ptr<wtf::block> >::const_iterator it;
    for (it = f->blocks_begin(); it != f->blocks_end(); ++it)
    {
        std::vector<wtf::block_location>::const_iterator it2;
        for (it2 = it->second->blocks_begin(); it2 != it->second->blocks_end(); ++it2)
        {
            if (it2->si == sid)
            {
                cout << "match " << *it2 << endl;
            }
        }
    }

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
        uint64_t sid = strtol(argv[2], NULL, 10);
        int64_t ret = rereplicate(filename, sid);

        return ret;
    }
}
