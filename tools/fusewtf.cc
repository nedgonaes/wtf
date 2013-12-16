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

#include <stdio.h>
#include <unordered_set>
#include <unordered_map>

using namespace std;

#define BLOCKSIZE 16
#define NUM_REPLICATIONS 2

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

    if (strcmp(input, prefix) == 0)
    {
        *output = NULL;
        return 0;
    }
    else
    {
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
}

void
fusewtf_loop()
{
    printf("first h_retval %ld h_status %d:", h_retval, h_status);
    cout << h_status << endl;
    hyperdex_client_returncode status = HYPERDEX_CLIENT_GARBAGE;
    h_retval = h->loop(-1, &status);

    if (h_retval < 0)
    {
        h_status = status;
    }

    printf("final h_retval %ld h_status %d:", h_retval, h_status);
    cout << h_status << endl << endl;
    fflush(stdout);
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

int
fusewtf_create(const char* path, mode_t mode)
{
    cout << "\t\t\t\t\t\tcreate " << path << endl;
    fusewtf_open(path, O_CREAT, mode);
    fusewtf_flush(path);
    return 0;
}

int
fusewtf_open(const char* path, int flags, mode_t mode)
{
    int64_t fd;
    
    cout << "\t\t\t\t\t\tw open " << path << endl;
    fd = w->open(path, flags, mode);
    if (fd > 0)
    {
        cout << "opened path [" << path << "] fd " << fd << endl;
        file_map[path] = fd;
        return fd;
    }
    else
    {
        return -1;
    }
}

int
fusewtf_flush(const char* path)
{
    int64_t fd;
    fd = file_map[path];
    cout << "\t\t\t\t\t\tw flush " << path << endl;
    w_retval = w->flush(fd, &w_status);
    return 0;
}

int
fusewtf_release(const char* path)
{
    int64_t fd;
    fd = file_map[path];
    cout << "\t\t\t\t\t\tw close " << path << endl;
    w_retval = w->close(fd, &w_status);
    file_map.erase(path);
    return 0;
}

void
fusewtf_del(const char* path)
{
    cout << "\t\t\t\t\t\tdel " << path << endl;
    h_retval = h->del(space, path, strlen(path), &h_status);
    fusewtf_loop();
}

int
fusewtf_read_filesize(uint32_t* output_filesize)
{
    if (h_status == HYPERDEX_CLIENT_SEARCHDONE)
    {
        *output_filesize = -1;
        return -1;
    }

    // Reading
    if (attr_got == NULL)
    {
        fprintf(logfusewtf, "attr_got is NULL\n");
        *output_filesize = -1;
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

                *output_filesize = 0;

                e::unpacker up (attr_got[i].value, attr_got[i].value_sz);
                while (!up.empty())
                {
                    up = up >> keylen >> key >> vallen;

                    e::unpack32be((uint8_t *)&keylen, &keylen);
                    e::unpack64be((uint8_t *)&key, &key);
                    e::unpack32be((uint8_t *)&vallen, &vallen);
                    e::intrusive_ptr<wtf::block> b = new wtf::block();
                    up = up >> b;

                    //cout << "BLOCK LENGTH " << b->length() << endl;
                    *output_filesize += b->length();
                }
            }
        }
        cout << "output_filesize " << *output_filesize << endl;
        return 0;
    }
}

int
fusewtf_read_filename(const char** output_filename)
{
    std::cout << "read_filename" << std::endl;
    if (h_status == HYPERDEX_CLIENT_SEARCHDONE)
    {
        *output_filename = NULL;
        return -1;
    }

    // Reading
    if (attr_got == NULL)
    {
        fprintf(stdout, "attr_got is NULL\n");
        fflush(stdout);
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
                fprintf(stdout, "filename [%s]\n", *output_filename);
                fflush(stdout);
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
    uint32_t read_size = size;

    fd = file_map[path];
    w->lseek(fd, offset);
    cout << "\t\t\t\t\t\tw read content [" << path << "] size [" 
        << size << "] offset [" << offset << "]" << endl;
    w_retval = w->read(fd, buffer, &read_size, &w_status);
    fusewtf_flush(path);

    return read_size;
}

size_t
fusewtf_write(const char* path, const char* buffer, size_t size, off_t offset)
{
    int64_t fd;

    fd = file_map[path];

    cout << "\tWRITING [" << path << "] size [" << size << "] offset [" << offset << "] buffer [" << buffer << "]" << endl;
    cout << "\t\t\t\t\t\tw write [" << buffer << "] size [" << size << "]" << endl;
    w->lseek(fd, offset);
    w_retval = w->write(fd, buffer, size, NUM_REPLICATIONS, &w_status);
    cout << "w_retval " << w_retval << " w_status " << w_status << endl;
    w_retval = w->flush(fd, &w_status);
    cout << "w_retval " << w_retval << " w_status " << w_status << endl;

    return -1;
}

int
fusewtf_truncate(const char* path, off_t length)
{
    int64_t fd;

    fd = file_map[path];

    cout << "\t\t\t\t\t\tw truncate [" << path << "] fd [" << fd << "] length [" << length << "]" << endl;
    return w->truncate(fd, length);
}

int
fusewtf_get(const char* path)
{
    fusewtf_flush_loop();

    h_status = (hyperdex_client_returncode)NULL;
    cout << "\t\t\t\t\t\tget " << path << endl;
    h_retval = h->get(space, path, strlen(path), &h_status, &attr_got, &attr_size_got);
    fusewtf_loop();
    cout << "get: " << h_status << " " << attr_got << " " << attr_size_got << " " << path << endl;
    if (h_status == HYPERDEX_CLIENT_SUCCESS && attr_got != NULL)
    {
        for (int i = 0; i < attr_size_got; ++i)
        {
            cout << "attribute " << i << ": " << attr_got[i].attr << " sz " << attr_got[i].value_sz << endl;
            if (strcmp(attr_got[i].attr, "path") == 0)
            {
                cout << "attr [" << attr_got[i].attr << "] value [" << std::string(attr_got[i].value, attr_got[i].value_sz) << "] datatype [" << attr_got[i].datatype << "]" << endl;
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
        }
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

    fusewtf_flush_loop();

    h_status = (hyperdex_client_returncode)NULL;
    cout << "\t\t\t\t\t\tsorted search " << query << endl;
    h_retval = h->sorted_search(space, &check, 1, "path", 100, false, &h_status, &attr_got, &attr_size_got);

    fusewtf_loop();
    fprintf(logfusewtf, "search [%s] predicate %d h_status %d\n", check.value, predicate, h_status);
    fprintf(stdout, "search [%s] predicate %d h_status %d\n", check.value, predicate, h_status);
    fflush(stdout);
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
fusewtf_is_dir()
{
    uint64_t is_dir;
    string path;

    for (int i = 0; i < attr_size_got; ++i)
    {
        if (strcmp(attr_got[i].attr, "path") == 0)
        {
            path = string(attr_got[i].value, attr_got[i].value_sz);
        }
        else if (strcmp(attr_got[i].attr, "directory") == 0)
        {
            //cout << "directory sz " << attr_got[i].value_sz << " [" << string(attr_got[i].value, attr_got[i].value_sz) << "]" << endl;
            e::unpacker up (attr_got[i].value, attr_got[i].value_sz);
            up = up >> is_dir;
            e::unpack64be((uint8_t *)&is_dir, &is_dir);
        }
    }

    cout << "[" << path << "] is_dir: " << is_dir << endl;
    return is_dir == 0 ? 0 : 1;
}

mode_t
fusewtf_get_mode()
{
    uint64_t mode;
    string path;
    for (int i = 0; i < attr_size_got; ++i)
    {
        if (strcmp(attr_got[i].attr, "path") == 0)
        {
            path = string(attr_got[i].value, attr_got[i].value_sz);
        }
        else if (strcmp(attr_got[i].attr, "mode") == 0)
        {
            //cout << "mode sz " << attr_got[i].value_sz << " [" << string(attr_got[i].value, attr_got[i].value_sz) << "]" << endl;

            e::unpacker up(attr_got[i].value, attr_got[i].value_sz);
            up = up >> mode;
            e::unpack64be((uint8_t *)&mode, &mode);
        }
    }

    cout << "[" << path << "] mode: " << mode << endl;
    return mode;
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

int fusewtf_mkdir(const char* path, mode_t mode)
{
    return w->mkdir(path, mode);
}

#ifdef __cplusplus
}
#endif //__cplusplus
