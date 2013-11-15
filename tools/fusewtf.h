/* Simple program to test using HyperDex as inode map for a filesystem
 *
 * Set up:
 * echo 'space wtf key path attributes map(string, string) blockmap' | hyperdex add-space -h 127.0.0.1 -p 1982
 */

#ifdef __cplusplus
extern "C"
{
#endif //__cplusplus

void
print_return();

void
wtf_read();

void
put(const char* filename);

void
del(const char* filename);

#ifdef __cplusplus
}
#endif //__cplusplus
