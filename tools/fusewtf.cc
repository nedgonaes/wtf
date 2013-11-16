/* Simple program to test using HyperDex as inode map for a filesystem
 *
 * Set up:
 * echo 'space wtf key path attributes map(string, string) blockmap' | hyperdex add-space -h 127.0.0.1 -p 1982
 */

// e
#include <e/endian.h>
#include <e/unpacker.h>

// HyperDex
#include <hyperdex/client.hpp>
#include "tools/common.h"

using namespace std;

hyperdex::Client* h;
const char* space = "wtf";
bool verbose = false;
const struct hyperdex_client_attribute* attr_got;
size_t attr_size_got;
hyperdex_client_returncode status;
int64_t retval;

const char* log_name = "logfusewtf";
FILE *logfusewtf;

#ifdef __cplusplus
extern "C"
{
#endif //__cplusplus

void
fusewtf_loop()
{
    if (verbose) printf("first retval %ld status %d:", retval, status);
    if (verbose) cout << status << endl;
    retval = h->loop(-1, &status);
    if (verbose) printf("final retval %ld status %d:", retval, status);
    if (verbose) cout << status << endl << endl;
}

int
fusewtf_read(const char** output_filename)
{
    //fprintf(logfusewtf, "fusewtf_read called\n");
    if (status == HYPERDEX_CLIENT_SEARCHDONE)
    {
        *output_filename = NULL;
        return -1;
    }

    // Reading
    if (attr_got == NULL)
    {
        fprintf(logfusewtf, "attr_got is NULL\n");
        *output_filename = NULL;
        return -1;
    }
    else
    {
        for (int i = 0; i < attr_size_got; ++i)
        {
            if (strcmp(attr_got[i].attr, "path") == 0)
            {
                *output_filename = string(attr_got[i].value, attr_got[i].value_sz).c_str();
                return 0;
            }
        }
        return -1;
    }
}

void
put(const char* filename)
{
    if (verbose) cout << ">>>>putting [" << filename << "]" << endl;

    retval = h->put_if_not_exist(space, filename, strlen(filename), NULL, 0, &status);
    fusewtf_loop();
    if (verbose) cout << endl;
}

void
del(const char* filename)
{
    if (verbose) cout << ">>>>deleting [" << filename << "]" << endl;

    retval = h->del(space, filename, strlen(filename), &status);
    fusewtf_loop();
    if (verbose) cout << endl;
}

int
fusewtf_search(const char* value, const char** one_result)
{
    string query("^");
    query += string(value);

    struct hyperdex_client_attribute_check check;
    check.attr = "path";
    check.value = query.c_str();
    check.value_sz = strlen(check.value);
    check.datatype = HYPERDATATYPE_STRING;
    check.predicate = HYPERPREDICATE_REGEX;

    status = (hyperdex_client_returncode)NULL;
    retval = h->sorted_search(space, &check, 1, "path", 100, false, &status, &attr_got, &attr_size_got);

    fusewtf_loop();
    //fprintf(logfusewtf, "search [%s] status %d\n", check.value, status);
    if (status == HYPERDEX_CLIENT_SUCCESS)
    {
        fusewtf_read(one_result);
        return 0;
    }
    else
    {
        *one_result = NULL;
        return -1;
    }
}

int
fusewtf_search_exists(const char* value)
{
    const char* tmp_result;
    int ret;
    ret = fusewtf_search(value, &tmp_result);

    // Clear loop
    while (status == HYPERDEX_CLIENT_SUCCESS)
    {
        fusewtf_loop();
    }
    
    return ret;
}


void
fusewtf_initialize()
{
    //hyperdex::connect_opts conn;
    //h = new hyperdex::Client(conn.host(), conn.port());
    h = new hyperdex::Client("127.0.0.1", 1982);
    logfusewtf = fopen(log_name, "a");
    fprintf(logfusewtf, "\n==== fusewtf\n");
}

void
fusewtf_destroy()
{
    fclose(logfusewtf);
}

#ifdef __cplusplus
}
#endif //__cplusplus
