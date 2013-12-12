/* Simple program to test using HyperDex as inode map for a filesystem
 *
 * Set up:
 * echo 'space wtf key path attributes map(string, string) blockmap' | hyperdex add-space -h 127.0.0.1 -p 1982
 */

#ifdef __cplusplus
extern "C"
{
#endif //__cplusplus

void fusewtf_initialize();
void fusewtf_destroy();

void fusewtf_loop();
void fusewtf_flush_loop();

int fusewtf_get(const char* path);
int fusewtf_search(const char* value, const char** one_result);
int fusewtf_search_exists(const char* value);
int fusewtf_search_is_dir(const char* value);

void fusewtf_create(const char* path);
void fusewtf_open(const char* path);
int fusewtf_flush(const char* path);
int fusewtf_release(const char* path);
void fusewtf_del(const char* path);

int fusewtf_read_filesize(uint32_t* output_filesize);
int fusewtf_read_filename(const char** output_filename);
size_t fusewtf_read_content(const char* path, char* buffer, size_t size, off_t offset);
size_t fusewtf_write(const char* path, const char* buffer, size_t size, off_t offset);
int fusewtf_extract_name(const char* input, const char* prefix, const char** output);

#ifdef __cplusplus
}
#endif //__cplusplus
