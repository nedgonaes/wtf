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

// WTF 
#include "client/file.h"
#include "client/wtf.h"

#include "fusewtf.h"

#include <unordered_set>
#include <unordered_map>

using namespace std;

hyperdex::Client* h;
wtf_client* w;
const char* space = "wtf";
bool verbose = false;
const struct hyperdex_client_attribute* attr_got;
size_t attr_size_got;

int64_t h_retval;
hyperdex_client_returncode h_status;
int64_t w_retval;
wtf_returncode w_status;

// Log
const char* log_name = "logfusewtf";
FILE *logfusewtf;

// Set to make sure no duplicate results returned for readdir
unordered_set<std::string> string_set;

// Map to keep track of open files
unordered_map<std::string,std::int64_t> file_map;

#ifdef __cplusplus
extern "C"
{
#endif //__cplusplus

int
fusewtf_extract_name(const char* input, const char* prefix, const char** output)
{
    string input_str(input);
    string output_str;

    int start = strlen(prefix);
    // If not root, then account for extra slash at end of prefix
    if (start != 1)
    {
        start++;
    }

    // In case the name extracted is a directory
    size_t next_slash_pos = input_str.find("/", start);
    if (next_slash_pos == string::npos)
    {
        output_str = input_str.substr(start).c_str();
    }
    else
    {
        output_str = input_str.substr(start, next_slash_pos - start).c_str();
    }

    //fprintf(logfusewtf, "extracted [%s]\n", *output);
    
    if (string_set.find(output_str) == string_set.end())
    {
        //fprintf(logfusewtf, "[%s] not in set\n", output_str.c_str());
        string_set.insert(output_str);
        *output = output_str.c_str();
    }
    else
    {
        //fprintf(logfusewtf, "[%s] in set\n", output_str.c_str());
        *output = NULL;
    }

    return 0;
}

void
fusewtf_loop()
{
    if (verbose) printf("first h_retval %ld h_status %d:", h_retval, h_status);
    if (verbose) cout << h_status << endl;
    h_retval = h->loop(-1, &h_status);
    if (verbose) printf("final h_retval %ld h_status %d:", h_retval, h_status);
    if (verbose) cout << h_status << endl << endl;
}

void
fusewtf_flush_loop()
{
    // Clear loop
    while (h_status != HYPERDEX_CLIENT_NONEPENDING)
    {
        fusewtf_loop();
    }
}

void
fusewtf_open(const char* path)
{
    int64_t fd;
    
    fd = w->open(path);
    cout << "opened path [" << path << "] fd " << fd << endl;
    file_map[path] = fd;
}

int
fusewtf_flush(const char* path)
{
    int64_t fd;
    fd = file_map[path];
    w_retval = w->flush(fd, &w_status);
    cout << "\t\tflush " << fd << " return " << w_retval << endl;
    return 0;
}

int
fusewtf_release(const char* path)
{
    int64_t fd;
    fd = file_map[path];
    w_retval = w->close(fd, &w_status);
    file_map.erase(path);
    cout << "\t\trelease " << fd << " return " << w_retval << endl;
    return 0;
}

int
fusewtf_read_len(uint32_t* output_filelen)
{
    if (h_status == HYPERDEX_CLIENT_SEARCHDONE)
    {
        *output_filelen = -1;
        return -1;
    }

    // Reading
    if (attr_got == NULL)
    {
        fprintf(logfusewtf, "attr_got is NULL\n");
        *output_filelen = -1;
        return -1;
    }
    else
    {
        for (int i = 0; i < attr_size_got; ++i)
        {
            if (strcmp(attr_got[i].attr, "blockmap") == 0)
            {
                uint32_t keylen;
                uint64_t key;
                uint32_t vallen;

                *output_filelen = 0;

                e::unpacker up (attr_got[i].value, attr_got[i].value_sz);
                while (!up.empty())
                {
                    up = up >> keylen >> key >> vallen;

                    e::unpack32be((uint8_t *)&keylen, &keylen);
                    e::unpack64be((uint8_t *)&key, &key);
                    e::unpack32be((uint8_t *)&vallen, &vallen);
                    e::intrusive_ptr<wtf::block> b = new wtf::block();
                    up = up >> b;

                    cout << "BLOCK LENGTH " << b->length() << endl;
                    *output_filelen += b->length();
                }
            }
        }
        return -1;
    }
}

int
fusewtf_read_filename(const char** output_filename)
{
    if (h_status == HYPERDEX_CLIENT_SEARCHDONE)
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
                string output_filename_str(attr_got[i].value, attr_got[i].value_sz);
                *output_filename = output_filename_str.c_str();
                //fprintf(logfusewtf, "filename [%s]\n", *output_filename);
                return 0;
            }
        }
        return -1;
    }
}

size_t
fusewtf_read_content(const char* path, char* buffer, size_t size, off_t offset)
{
    int64_t fd;
    uint32_t file_size;
    uint32_t read_size;
    const char* line_break = "\n";

    fd = file_map[path];

    fusewtf_get(path);
    fusewtf_read_len(&file_size);

    if (offset >= file_size)
    {
        return 0;
    }

    w->lseek(fd, offset);
    read_size = file_size - offset;
    read_size = size < read_size ? size : read_size;

    cout << "read content [" << path << "] size [" << size << "] offset [" << offset << "] read_size [" << read_size << "]" << endl;
    w_retval = w->read(fd, buffer, read_size, &w_status);
    fusewtf_flush(path);
    //w_retval = w->flush(fd, &w_status);

    if (file_size <= offset + size)
    {
        cout << "replace end character with link break" << endl;
        memcpy(buffer - offset + file_size - 1, line_break, strlen(line_break));
    }
    cout << "w_retval " << w_retval << " w_status " << w_status << " [" << buffer << "]" << endl;

    return read_size;
}

void
fusewtf_put(const char* filename)
{
    h_retval = h->put_if_not_exist(space, filename, strlen(filename), NULL, 0, &h_status);
    fusewtf_loop();
}

void
fusewtf_del(const char* filename)
{
    h_retval = h->del(space, filename, strlen(filename), &h_status);
    fusewtf_loop();
}

int
fusewtf_get(const char* path)
{
    struct hyperdex_client_attribute_check check;
    check.attr = "path";
    check.value = path;
    check.value_sz = strlen(check.value);
    check.datatype = HYPERDATATYPE_STRING;
    check.predicate = HYPERPREDICATE_EQUALS;

    h_status = (hyperdex_client_returncode)NULL;
    h_retval = h->sorted_search(space, &check, 1, "path", 100, false, &h_status, &attr_got, &attr_size_got);
    //h_retval = h->get(space, path, strlen(path), &h_status, &attr_got, &attr_size_got);
    fusewtf_loop();
    cout << h_status << " " << attr_got << " " << attr_size_got << " " << path << endl;
    if (h_status == HYPERDEX_CLIENT_SUCCESS && attr_got != NULL)
    {
        /*
        for (int i = 0; i < attr_size_got; ++i)
        {
            cout << "attribute " << i << ": " << attr_got[i].attr << endl;
            if (strcmp(attr_got[i].attr, "path") == 0)
            {
                //cout << "attr [" << attr_got[i].attr << "] value [" << std::string(attr_got[i].value, attr_got[i].value_sz) << "] datatype [" << attr_got[i].datatype << "]" << endl;
                //printf("value [%.5s]\n", attr_got[i].value);
            }
            else if (strcmp(attr_got[i].attr, "blockmap") == 0)
            {
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
                    e::intrusive_ptr<wtf::block> b = new wtf::block();
                    up = up >> b;
                    cout << "BLOCK LENGTH " << b->length() << endl;
                }
            }
            else
            {
                cout << "unexpected attribute" << endl;
            }
        }
        */
        return 0;
    }
    else
    {
        return -1;
    }
}

int
fusewtf_search_predicate(const char* value, hyperpredicate predicate, const char** one_result)
{
    string query(value);
    if (predicate == HYPERPREDICATE_REGEX)
    {
        query = string("^") + query;
    }
    else if (predicate != HYPERPREDICATE_EQUALS)
    {
        return -1;
    }

    string_set.clear();

    struct hyperdex_client_attribute_check check;
    check.attr = "path";
    check.value = query.c_str();
    check.value_sz = strlen(check.value);
    check.datatype = HYPERDATATYPE_STRING;
    check.predicate = predicate;

    h_status = (hyperdex_client_returncode)NULL;
    h_retval = h->sorted_search(space, &check, 1, "path", 100, false, &h_status, &attr_got, &attr_size_got);

    fusewtf_loop();
    //fprintf(logfusewtf, "search [%s] predicate %d h_status %d\n", check.value, predicate, h_status);
    if (h_status == HYPERDEX_CLIENT_SUCCESS)
    {
        fusewtf_read_filename(one_result);
        return 0;
    }
    else
    {
        *one_result = NULL;
        return -1;
    }
}

int
fusewtf_search(const char* value, const char** one_result)
{
    return fusewtf_search_predicate(value, HYPERPREDICATE_REGEX, one_result);
}

int
fusewtf_search_exists_predicate(const char* value, hyperpredicate predicate)
{
    const char* tmp_result;
    int ret;
    ret = fusewtf_search_predicate(value, predicate, &tmp_result);

    fusewtf_flush_loop();
    
    return ret;
}

int
fusewtf_search_exists(const char* value)
{
    return fusewtf_search_exists_predicate(value, HYPERPREDICATE_REGEX);
}

int
fusewtf_search_is_dir(const char* value)
{
    int reg = fusewtf_search_exists(value);
    int equ = fusewtf_search_exists_predicate(value, HYPERPREDICATE_EQUALS);
    int ret = reg == 0 && equ != 0? 0 : -1;

    //fprintf(logfusewtf, "[%s] reg %d equ %d ret %d\n", value, reg, equ, ret);
    return ret;
}

void
fusewtf_initialize()
{
    //hyperdex::connect_opts conn;
    //h = new hyperdex::Client(conn.host(), conn.port());
    h = new hyperdex::Client("127.0.0.1", 1982);
    w = new wtf_client("127.0.0.1", 1981, "127.0.0.1", 1982);
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
