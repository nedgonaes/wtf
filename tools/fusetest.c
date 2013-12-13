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

const char *ROOT = "/";
const char *log_name = "logfusetest";
FILE *logfile;

sem_t lock;

static int fusetest_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    printf("\t\t\t\tcreate [%s]\n", path);
    fflush(stdout);
    fi->fh = fusewtf_create(path, mode);
    return 0;
}

static int fusetest_utimens(const char *path, const struct timespec tv[2])
{
    printf("\t\t\t\tutimens [%s]\n", path);
    return 0;
}

static int fusetest_getattr(const char *path, struct stat *stbuf)
{
    int ret = 0;

    //printf("\t\t\t\tgetattr [%s]\n", path);
    sem_wait(&lock);
    memset(stbuf, 0, sizeof(struct stat));
    if (fusewtf_search_is_dir(path) == 0 || strcmp(path, ROOT) == 0)
    {
        //printf("GETATTR: dir [%s]\n", path);
        //fflush(stdout);
        fprintf(logfile, "GETATTR: dir [%s]\n", path);
        stbuf->st_mode = S_IFDIR | 0777;
    }
    else if (fusewtf_get(path) == 0)
    {
        //printf("GETATTR: file [%s]\n", path);
        //fflush(stdout);
        fprintf(logfile, "GETATTR: file [%s]\n", path);
        stbuf->st_mode = S_IFREG | 0777;

        uint32_t filesize;
        //printf("find size [%s]\n", path);
        fusewtf_read_filesize(&filesize);
        if (filesize < 0)
        {
            fprintf(logfile, "GETATTR ERROR: file [%s] has negative length\n", path);
        }
        stbuf->st_size = filesize;
    }
    else
    {
        //printf("GETATTR: invalid [%s]\n", path);
        //fflush(stdout);
        fprintf(logfile, "GETATTR: invalid [%s]\n", path);
        ret = -ENOENT;
    }

    fusewtf_flush_loop();
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

    //printf("\t\t\t\treaddir [%s]\n", path);
    sem_wait(&lock);
    res = fusewtf_search(path, &to_add);

    if (res != 0 && strcmp(path, ROOT) != 0)
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
        res = fusewtf_read_filename(&to_add);
    }

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    fusewtf_flush_loop();
    sem_post(&lock);
    fflush(logfile);
    return ret;
}

static int fusetest_open(const char *path, struct fuse_file_info *fi)
{
    int ret = 0;
    printf("\t\t\t\topen [%s]\n", path);
    fflush(stdout);
    sem_wait(&lock);
    fi->fh = fusewtf_open(path, fi->flags, 0);
    if (fi->fh < 0)
    {
        ret = -ENOENT;
    }

    fusewtf_flush_loop();
    sem_post(&lock);
    fflush(logfile);
    return ret;
}


static int fusetest_flush(const char *path, struct fuse_file_info *fi)
{
    int ret = 0;
    printf("\t\t\t\tflush [%s]\n", path);
    sem_wait(&lock);
    ret = fusewtf_flush(path);

    sem_post(&lock);
    return ret;
}

static int fusetest_release(const char *path, struct fuse_file_info *fi)
{
    int ret = 0;
    printf("\t\t\t\trelease [%s]\n", path);
    sem_wait(&lock);
    ret = fusewtf_release(path);

    sem_post(&lock);
    return ret;
}


static int fusetest_read(const char *path, char *buf, size_t size, off_t offset,
        struct fuse_file_info *fi)
{
    size_t len;
    (void) fi;
    int ret;
    size_t read_size;

    sem_wait(&lock);
    printf("\t\t\t\tread [%s]\n", path);
    printf("read [%s], size %zu, offset %zu\n", path, size, offset);
    if(fusewtf_search_exists(path) != 0)
    {
        ret = -ENOENT;
    }
    else
    {
        read_size = fusewtf_read_content(path, buf, size, offset);
    }

    fusewtf_flush_loop();
    sem_post(&lock);
    fflush(logfile);
    return read_size;
}

static int fusetest_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    printf("\t\t\t\twrite [%s] size [%zu] offset [%zu]\n", path, size, offset);
    fflush(stdout);
    sem_wait(&lock);
    fusewtf_write(path, buf, size, offset);

    fusewtf_flush_loop();
    sem_post(&lock);
    return size;
}

static int fusetest_rename(const char *old_path, const char *new_path)
{
    printf("\t\t\t\trename [%s] [%s]\n", old_path, new_path);
    return 0;
}

static int fusetest_unlink(const char *path)
{
    printf("\t\t\t\tunlink [%s]\n", path);
    sem_wait(&lock);
    fusewtf_del(path);

    fusewtf_flush_loop();
    sem_post(&lock);
    return 0;
}

static int fusetest_readlink(const char *path, char *buf, size_t bufsiz)
{
    printf("\t\t\t\treadlink [%s]\n", path);
    return 0;
}

static int fusetest_mknod(const char *pathname, mode_t mode, dev_t dev)
{
    printf("\t\t\t\tmknod [%s]\n", pathname);
    fflush(stdout);
    return 0;
}

static int fusetest_mkdir(const char *pathname, mode_t mode)
{
    return fusewtf_mkdir(pathname, mode);
    printf("\t\t\t\tmkdir [%s]\n", pathname);
    return 0;
}

static int fusetest_symlink(const char *old_path, const char *new_path)
{
    printf("\t\t\t\tsymlink [%s] [%s]\n", old_path, new_path);
    return 0;
}

static int fusetest_rmdir(const char *pathname)
{
    printf("\t\t\t\trmdir [%s]\n", pathname);
    return 0;
}

static int fusetest_link(const char *old_path, const char *new_path)
{
    printf("\t\t\t\tlink [%s] [%s]\n", old_path, new_path);
    return 0;
}

static int fusetest_chmod(const char *path, mode_t mode)
{
    printf("\t\t\t\tchmod [%s]\n", path);
    return 0;
}

static int fusetest_chown(const char *path, uid_t owner, gid_t group)
{
    printf("\t\t\t\tchown [%s]\n", path);
    return 0;
}

static int fusetest_truncate(const char *path, off_t length)
{
    printf("\t\t\t\ttruncate [%s] length [%zu]\n", path, length);
    return 0;
}

static int fusetest_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    printf("\t\t\t\tfsync [%s]\n", path);
    return 0;
}

static struct fuse_operations fusetest_oper = {
    .flush      = fusetest_flush,
    .getattr    = fusetest_getattr,
    .open       = fusetest_open,
    .read       = fusetest_read,
    .rename     = fusetest_rename,
    .readdir    = fusetest_readdir,
    .release    = fusetest_release,
    .unlink     = fusetest_unlink,
    .utimens    = fusetest_utimens,
    .write      = fusetest_write,
    .readlink   = fusetest_readlink,
    .mknod      = fusetest_mknod,
    .mkdir      = fusetest_mkdir, 
    .symlink    = fusetest_symlink,
    .rmdir      = fusetest_rmdir,
    .link       = fusetest_link,
    .chmod      = fusetest_chmod,
    .chown      = fusetest_chown,
    .truncate   = fusetest_truncate,
    .fsync      = fusetest_fsync,
#if FUSE_VERSION >= 25
    .create     = fusetest_create,
#endif
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
