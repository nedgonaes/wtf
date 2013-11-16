/*
 * Use FUSE to interact with files in WTF
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

// WTF
#include "fusewtf.h"

static const char *hello_str = "Hello World!\n";
const char* log_name = "logfusetest";
FILE *logfile;

static int fusetest_getattr(const char *path, struct stat *stbuf)
{
	int ret = 0;

	memset(stbuf, 0, sizeof(struct stat));
	//if (strcmp(path, "/") == 0) {
	//	stbuf->st_mode = S_IFDIR | 0755;
	//	stbuf->st_nlink = 2;
	//} else if (strcmp(path, fusetest_path) == 0) {
	//	stbuf->st_mode = S_IFREG | 0444;
	//	stbuf->st_nlink = 1;
	//	stbuf->st_size = strlen(fusetest_str);
	//} else
	//	ret = -ENOENT;
	if (strcmp(path, "/") == 0) {
        fprintf(logfile, "GETATTR: root [%s]\n", path);
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 1;
	} else if (fusewtf_search_exists(path) == 0) {
        fprintf(logfile, "GETATTR: exists [%s]\n", path);
		stbuf->st_mode = S_IFREG | 0444;
		stbuf->st_nlink = 1;
    } else {
        fprintf(logfile, "GETATTR: not exists [%s]\n", path);
        ret = -ENOENT;
    }

    fflush(logfile);
	return ret;
}

static int fusetest_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
	(void) offset;
	(void) fi;
    int res = 0;
    int ret = 0;
    const char** to_add;

    if (fusewtf_search_exists(path) != 0)
    {
        fprintf(logfile, "\tREADDIR: not exists [%s]\n", path);
        ret = -ENOENT;
    }
    else
    {
        fprintf(logfile, "\tREADDIR: exists [%s]\n", path);
        //res = fusewtf_search(path, to_add);
        //filler(buf, *to_add, NULL, 0);
        filler(buf, "dir1", NULL, 0);
        //while (ret != NULL)
        //{
        //    filler(buf, ret, NULL, 0);
        //    wtf_loop();
        //    ret = wtf_read();
        //}

        filler(buf, ".", NULL, 0);
        filler(buf, "..", NULL, 0);
    }

    fflush(logfile);
	return ret;
}

static int fusetest_open(const char *path, struct fuse_file_info *fi)
{
    fprintf(logfile, "open called [%s]\n", path);
    if (fusewtf_search_exists(path) != 0) return -ENOENT;

    fflush(logfile);
	return 0;
}

static int fusetest_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;

    fprintf(logfile, "read called [%s]\n", path);
	if(fusewtf_search_exists(path) != 0)
		return -ENOENT;

	len = strlen(hello_str);
	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, hello_str + offset, size);
	} else
		size = 0;

    fflush(logfile);
	return size;
}

static struct fuse_operations fusetest_oper = {
	.getattr	= fusetest_getattr,
	.readdir	= fusetest_readdir,
	.open		= fusetest_open,
	.read		= fusetest_read,
};

int main(int argc, char *argv[])
{
    int ret;
    
    logfile = fopen(log_name, "a");
    fprintf(logfile, "\n==== fusetest\n");
    fusewtf_initialize();

	ret = fuse_main(argc, argv, &fusetest_oper, NULL);

    fclose(logfile);
    fusewtf_destroy();

    return ret;
}
