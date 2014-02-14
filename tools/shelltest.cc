// C
#include <stdio.h>

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
        for (size_t i = 0; i < attrs_sz; ++i)
        {
            if (verbose) cout << "\t" << i << ". [" << attrs[i].attr << "]:\t";
            if (strcmp(attrs[i].attr, "path") == 0)
            {
                cout << "[" << string(attrs[i].value, attrs[i].value_sz) << "]" << endl;
            }
            else if (strcmp(attrs[i].attr, "blockmap") == 0)
            {
                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                cout << "MESSAGE: " << up.as_slice().hex() << endl;

                if (attrs[i].value_sz == 0)
                {
                    continue;
                }
            }
            else if (strcmp(attrs[i].attr, "directory") == 0)
            {
                uint64_t is_dir;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> is_dir;
                e::unpack64be((uint8_t*)&is_dir, &is_dir);
                cout << "[" << is_dir << "]" << endl;
            }
            else if (strcmp(attrs[i].attr, "mode") == 0)
            {
                uint64_t mode;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> mode;
                e::unpack64be((uint8_t*)&mode, &mode);
                cout << "[" << mode << "]" << endl;
            }
            else
            {
                cout << "<unexpected attribute>" << endl;
            }
        }
    }

    if (verbose) cout << endl;
}

void
put(const char* filename)
{
    if (verbose) cout << ">>>>putting [" << filename << "]" << endl;

    retval = h->put_if_not_exist(WTF_SPACE, filename, strlen(filename), NULL, 0, &status);
    print_return();
    if (verbose) cout << endl;
}

void
del(const char* filename)
{
    if (verbose) cout << ">>>>deleting [" << filename << "]" << endl;

    retval = h->del(WTF_SPACE, filename, strlen(filename), &status);
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
    // TODO max_int64 results
    retval = h->sorted_search(WTF_SPACE, &check, 1, "path", 100, false, &status, &attrs, &attrs_sz);

    int counter = 0;
    while (status != HYPERDEX_CLIENT_SEARCHDONE && status != HYPERDEX_CLIENT_NONEPENDING)
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
            retval = h->group_del(WTF_SPACE, &check, 1, &status);
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
