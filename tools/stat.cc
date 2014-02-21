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
static bool _verbose = false;

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

void
read()
{
    if (status == HYPERDEX_CLIENT_SEARCHDONE) return;

    // Reading
    if (_verbose) cout << "[" << attrs_sz << "] attributes" << endl;
    if (attrs == NULL)
    {
        cout << "attrs is NULL" << endl;
    }
    else
    {
        e::intrusive_ptr<wtf::file> f = new wtf::file("", 0);
        for (size_t i = 0; i < attrs_sz; ++i)
        {
            if (_verbose) cout << i << ". [" << attrs[i].attr << "]:";
            if (strcmp(attrs[i].attr, "path") == 0)
            {
                string path(attrs[i].value, attrs[i].value_sz);
                if (_verbose) cout << "[" << path << "]" << endl;
                f->path(path.c_str());
            }
            else if (strcmp(attrs[i].attr, "blockmap") == 0)
            {
                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                if (attrs[i].value_sz == 0)
                {
                    if (_verbose) cout << endl;
                }
                else
                {
                    if (_verbose) cout << "[" << up.as_slice().hex() << "]" << endl;
                    up = up >> f;
                }
            }
            else if (strcmp(attrs[i].attr, "directory") == 0)
            {
                uint64_t is_dir;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> is_dir;
                e::unpack64be((uint8_t*)&is_dir, &is_dir);
                if (_verbose) cout << "[" << is_dir << "]" << endl;
                f->is_directory = is_dir == 0 ? false : true;
            }
            else if (strcmp(attrs[i].attr, "mode") == 0)
            {
                uint64_t mode;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> mode;
                e::unpack64be((uint8_t*)&mode, &mode);
                if (_verbose) cout << "[" << mode << "]" << endl;
                f->mode = mode;
            }
            else
            {
                if (_verbose) cout << "<unexpected attribute>" << endl;
            }
        }
        cout << *f << endl;
    }
}

void
search(const char* attr, const char* value, hyperpredicate predicate)
{
    cout << ">>>>searching [" << attr << "] for [" << value << "] with [" << predicate << "]" << endl;
    struct hyperdex_client_attribute_check check;
    check.attr = attr;
    check.value = value;
    check.value_sz = strlen(check.value);
    check.datatype = HYPERDATATYPE_STRING;
    check.predicate = predicate;

    status = (hyperdex_client_returncode)NULL;
    // TODO max_int64 results
    retval = h->sorted_search(WTF_SPACE, &check, 1, "path", 100, false, &status, &attrs, &attrs_sz);

    int counter = 0;
    while (status != HYPERDEX_CLIENT_SEARCHDONE && status != HYPERDEX_CLIENT_NONEPENDING)
    {
        cout << "(" << ++counter << ")" << endl;
        print_return();
        read();
    }
    cout << "\nSearch finished successfully" << endl;
}

int
main(int argc, const char* argv[])
{
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('p', "port")
        .description("port on the hyperdex coordinator")
        .metavar("p")
        .as_long(&_hyper_port);
    ap.arg().name('h', "host")
        .description("address of hyperdex coordinator")
        .metavar("p")
        .as_string(&_hyper_host);
    ap.arg().name('q', "query")
        .description("file name regex")
        .metavar("q")
        .as_string(&_query);
    ap.arg().name('v', "verbose")
        .description("verbose outputs")
        .metavar("v")
        .set_true(&_verbose);

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }
    else
    {
        h = new hyperdex::Client(_hyper_host, _hyper_port);
        string query("^");
        query += string(_query);
        search("path", query.c_str(), HYPERPREDICATE_REGEX);

        //cout << "Usage: wtf-stat <file_name_regex>" << endl;
        //cout << "Type wtf-stat --help to see options" << endl;
        //cout << "Set up: echo 'space wtf key path attributes string blockmap, int directory, int mode' | hyperdex add-space -h 127.0.0.1 -p 1982" << endl;
    }

    return EXIT_SUCCESS;
}
