// C/C++
#include <stdio.h>
#include <string.h>
#include <map>

// E
#include <e/endian.h>
#include <e/unpacker.h>
#include <e/intrusive_ptr.h>

// HyperDex
#include <hyperdex/client.hpp>

// WTF 
#include "client/file.h"

using namespace std;

hyperdex::Client* h;
const char* WTF_SPACE = "wtf";
bool verbose = true;
const struct hyperdex_client_attribute* attrs;
size_t attrs_sz;
hyperdex_client_returncode status;
int64_t retval;

void
print_return()
{
    if (verbose) printf("first retval %ld status %d:", retval, status);
    if (verbose) cout << status << endl;
    retval = h->loop(-1, &status);
    if (verbose) printf("final retval %ld status %d:", retval, status);
    if (verbose) cout << status << endl;
}

void
read()
{
    if (status == HYPERDEX_CLIENT_SEARCHDONE) return;

    // Reading
    if (verbose) cout << "[" << attrs_sz << "] attributes" << endl;
    if (attrs == NULL)
    {
        cout << "attrs is NULL" << endl;
    }
    else
    {
        e::intrusive_ptr<wtf::file> f = new wtf::file("", 0);
        for (size_t i = 0; i < attrs_sz; ++i)
        {
            if (verbose) cout << i << ". [" << attrs[i].attr << "]:";
            if (strcmp(attrs[i].attr, "path") == 0)
            {
                string path(attrs[i].value, attrs[i].value_sz);
                cout << "[" << path << "]" << endl;
                f->path(path.c_str());
            }
            else if (strcmp(attrs[i].attr, "blockmap") == 0)
            {
                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                if (attrs[i].value_sz == 0)
                {
                    cout << endl;
                }
                else
                {
                    cout << "[" << up.as_slice().hex() << "]" << endl;
                    up = up >> f;
                }
            }
            else if (strcmp(attrs[i].attr, "directory") == 0)
            {
                uint64_t is_dir;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> is_dir;
                e::unpack64be((uint8_t*)&is_dir, &is_dir);
                cout << "[" << is_dir << "]" << endl;
                f->is_directory = is_dir == 0 ? false : true;
            }
            else if (strcmp(attrs[i].attr, "mode") == 0)
            {
                uint64_t mode;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> mode;
                e::unpack64be((uint8_t*)&mode, &mode);
                cout << "[" << mode << "]" << endl;
                f->mode = mode;
            }
            else
            {
                cout << "<unexpected attribute>" << endl;
            }
        }
        cout << *f << endl;
    }
}

void
search(const char* attr, const char* value, hyperpredicate predicate)
{
    if (verbose) cout << ">>>>searching [" << attr << "] for [" << value << "] with [" << predicate << "]" << endl;
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
        if (verbose) cout << "(" << ++counter << ")" << endl;
        print_return();
        read();
    }
    if (verbose) cout << endl;
}

int
main(int argc, const char* argv[])
{
    //hyperdex::connect_opts conn;
    //h = new hyperdex::Client(conn.host(), conn.port());
    h = new hyperdex::Client("127.0.0.1", 1982);
    
    if (argc > 1)
    {
        if (argc > 2) verbose = true;

        string query("^");
        query += string(argv[1]);

        search("path", query.c_str(), HYPERPREDICATE_REGEX);
    }
    else
    {
        cout << "Usage: ./wtf-stat <file_name_regex>" << endl;
        cout << "Set up: echo 'space wtf key path attributes string blockmap, int directory, int mode' | hyperdex add-space -h 127.0.0.1 -p 1982" << endl;
    }

    return EXIT_SUCCESS;
}
