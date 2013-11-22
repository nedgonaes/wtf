/*
 * Use FUSE to interact with files in WTF, through HyperDex
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

#include <semaphore.h>

// WTF
#include "fusewtf.h"

static const char *hello_str = "Hello World!\n";
const char* log_name = "logfusetest";
FILE *logfile;

sem_t lock;

static int fusetest_getattr(const char *path, struct stat *stbuf)
{
	int ret = 0;

    sem_wait(&lock);
	memset(stbuf, 0, sizeof(struct stat));
	if (fusewtf_search_is_dir(path) == 0)
    {
        fprintf(logfile, "GETATTR: dir [%s]\n", path);
		stbuf->st_mode = S_IFDIR | 0755;
	}
    else if (fusewtf_search_exists(path) == 0)
    {
        fprintf(logfile, "GETATTR: file [%s]\n", path);
		stbuf->st_mode = S_IFREG | 0444;
		stbuf->st_size = strlen(hello_str);
    }
    else
    {
        fprintf(logfile, "GETATTR: invalid [%s]\n", path);
        ret = -ENOENT;
    }

    sem_post(&lock);
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
    const char* to_add;
    const char* to_add_extracted;

    sem_wait(&lock);
    //fprintf(logfile, "\tREADDIR: exists [%d] [%s]\n", fusewtf_search_exists(path), path);
    //if (fusewtf_search_exists(path) != 0)
	//if (strcmp(path, "/") != 0)
    //{
    //    ret = -ENOENT;
    //}
    //else
    //{
        res = fusewtf_search(path, &to_add);
        if (res != 0)
        {
            fprintf(logfile, "\tREADDIR: ERROR dir [%s] does not exist\n", path);
        }
        while (res == 0)
        {
            fusewtf_extract_name(to_add, path, &to_add_extracted);
            if (to_add_extracted != NULL)
            {
                filler(buf, to_add_extracted, NULL, 0);
            }
            fusewtf_loop();
            res = fusewtf_read(&to_add);
        }

        filler(buf, ".", NULL, 0);
        filler(buf, "..", NULL, 0);
    //}

    sem_post(&lock);
    fflush(logfile);
	return ret;
}

static int fusetest_open(const char *path, struct fuse_file_info *fi)
{
    int ret = 0;
    sem_wait(&lock);
    if (fusewtf_search_exists(path) != 0)
    {
        ret = -ENOENT;
    }

    sem_post(&lock);
    fflush(logfile);
	return ret;
}

static int fusetest_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;
    int ret;

    sem_wait(&lock);
	if(fusewtf_search_exists(path) != 0)
    {
        ret = -ENOENT;
    }
    else
    {
        len = strlen(hello_str);
        if (offset < len) {
            if (offset + size > len)
                size = len - offset;
            memcpy(buf, hello_str + offset, size);
        } else
            size = 0;
    }

    sem_post(&lock);
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

    sem_init(&lock, 0, 1);

    ret = fuse_main(argc, argv, &fusetest_oper, NULL);

    sem_destroy(&lock);

    fclose(logfile);
    fusewtf_destroy();

    return ret;
}
