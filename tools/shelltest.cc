/*
 * Simple program to test using HyperDex as inode map for a filesystem
 */

// e
#include <e/endian.h>
#include <e/unpacker.h>

// HyperDex
#include <hyperdex/client.hpp>
#include "tools/common.h"

// WTF 
#include "client/file.h"
#include "client/wtf.h"

using namespace std;

hyperdex::Client* h;
const char* space = "wtf";
bool verbose = true;
const struct hyperdex_client_attribute* attr_got;
size_t attr_size_got;
hyperdex_client_returncode status;
int64_t retval;

void
print_return()
{
    if (verbose) printf("first retval %ld status %d:", retval, status);
    if (verbose) cout << status << endl;
    retval = h->loop(-1, &status);
    if (verbose) printf("final retval %ld status %d:", retval, status);
    if (verbose) cout << status << endl << endl;
}

void
read()
{
    if (status == HYPERDEX_CLIENT_SEARCHDONE) return;

    // Reading
    if (verbose) cout << "attr_size_got [" << attr_size_got << "]" << endl;
    if (attr_got == NULL)
    {
        cout << "attr_got is NULL" << endl;
    }
    else
    {
        for (int i = 0; i < attr_size_got; ++i)
        {
            if (verbose) cout << "attribute " << i << ": " << attr_got[i].attr << endl;
            if (strcmp(attr_got[i].attr, "path") == 0)
            {
                if (verbose) cout << "attr [" << attr_got[i].attr << "] value [" << std::string(attr_got[i].value, attr_got[i].value_sz) << "] datatype [" << attr_got[i].datatype << "]" << endl;
                else cout << std::string(attr_got[i].value, attr_got[i].value_sz) << endl;
                //printf("value [%.5s]\n", attr_got[i].value);
            }
            else if (strcmp(attr_got[i].attr, "blockmap") == 0)
            {
                if (verbose) cout << "attr [" << attr_got[i].attr << "] value_sz [" << attr_got[i].value_sz << "] datatype [" << attr_got[i].datatype << "]" << endl;
                uint32_t keylen;
                uint64_t key;
                uint32_t vallen;

                e::unpacker up (attr_got[i].value, attr_got[i].value_sz);
                while (!up.empty())
                {
                    up = up >> keylen >> key >> vallen;

                    e::unpack32be((uint8_t *)&keylen, &keylen);
                    e::unpack64be((uint8_t *)&key, &key);
                    e::unpack32be((uint8_t *)&vallen, &vallen);
                    if (verbose) printf("keylen %x key %lx vallen %x\n", keylen, key, vallen);
                    e::intrusive_ptr<wtf::block> b = new wtf::block();
                    up = up >> b;
                }
            }
            else
            {
                cout << "unexpected attribute" << endl;
            }
        }
    }

    if (verbose) cout << endl;
}

void
put(const char* filename)
{
    if (verbose) cout << ">>>>putting [" << filename << "]" << endl;

    retval = h->put_if_not_exist(space, filename, strlen(filename), NULL, 0, &status);
    print_return();
    if (verbose) cout << endl;
}

void
del(const char* filename)
{
    if (verbose) cout << ">>>>deleting [" << filename << "]" << endl;

    retval = h->del(space, filename, strlen(filename), &status);
    print_return();
    if (verbose) cout << endl;
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
    retval = h->sorted_search(space, &check, 1, "path", 100, false, &status, &attr_got, &attr_size_got);

    int counter = 0;
    while (status != HYPERDEX_CLIENT_SEARCHDONE)
    {
        if (verbose) cout << ++counter << endl;
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
        if (strcmp(argv[1], "touch") == 0)
        {
            if (argc > 3) verbose = true;
            if (argc > 2)
            {
                cout << "touch " << argv[2] << endl;
                put(argv[2]);
            }
            else
            {
                cout << "usage: touch <filename>" << endl;
            }
        }
        else if (strcmp(argv[1], "ls") == 0)
        {
            if (argc > 3) verbose = true;
            string query("^");
            if (argc > 2)
            {
                query += string(argv[2]);
            }

            cout << "ls " << query << endl;
            search("path", query.c_str(), HYPERPREDICATE_REGEX);
        }
        else if (strcmp(argv[1], "rm") == 0)
        {
            if (argc > 3) verbose = true;
            if (argc > 2)
            {
                cout << "rm " << argv[2] << endl;
                del(argv[2]);
            }
            else
            {
                cout << "usage: rm <filename>" << endl;
            }
        }
        else if (strcmp(argv[1], "rmall") == 0)
        {
            verbose = true;

            struct hyperdex_client_attribute_check check;
            check.attr = "path";
            check.value = "*";
            check.value_sz = 0;
            check.datatype = HYPERDATATYPE_STRING;
            check.predicate = HYPERPREDICATE_REGEX;

            status = (hyperdex_client_returncode)NULL;
            retval = h->group_del(space, &check, 1, &status);
            print_return();
        }
        else
        {
            cout << argv[1] << " is not supported" << endl;
        }
    }
    else
    {
        cout << "Commands: touch, ls, rm, rmall" << endl;
        cout << "Set up: echo 'space wtf key path attributes map(string, string) blockmap, int directory, int mode' | hyperdex add-space -h 127.0.0.1 -p 1982" << endl;
    }

    return EXIT_SUCCESS;
}
