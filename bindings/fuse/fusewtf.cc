/*
 * Use FUSE to interact with files in WTF, through HyperDex
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <semaphore.h>
#include "client/client.h"

#define DEFAULT_BLOCK_LENGTH 4096
#define DEFAULT_REPLICATION 3 


const char *ROOT = "/";
const char *log_name = "logfusewtf";
FILE *logfile;

#define LOGENTRY std::cout << __FILE__ << "::" << __LINE__<< "::" << __func__ << std::endl

#ifdef __cplusplus
extern "C"
{
#endif //__cplusplus

using wtf::client;

client* w;

static int fusewtf_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    LOGENTRY;
    wtf_client_returncode status;
    int64_t reqid = w->open(path, O_CREAT, mode, DEFAULT_REPLICATION, DEFAULT_BLOCK_LENGTH, (int64_t*)&fi->fh, &status);

    if (reqid < 0)
    {
        return -1;
    }

    int64_t ret = w->loop(reqid, -1, &status);

    if (ret < 0)
    {
        return -1;
    }

    return 0;
}

static int fusewtf_utimens(const char *path, const struct timespec tv[2])
{
    LOGENTRY;
    std::cout << "\t\t\t\tutimens [" << path << "]" << std::endl;
    std::cout << "XXX: utimens not implemented" << std::endl;
    return 0;
}

static int fusewtf_getattr(const char *path, struct stat *stbuf)
{
    LOGENTRY;
    memset(stbuf, 0, sizeof(struct stat));

    struct wtf_file_attrs fa;
    wtf_client_returncode status;
    wtf_client_returncode lstatus;
    int64_t reqid = w->getattr(path, &fa, &status);

    if (reqid < 0)
    {
        LOGENTRY;
        return -errno;
    }

    if (w->loop(reqid, -1, &lstatus) < 0)
    {
        LOGENTRY;
        return -errno;
    }

    if (status == WTF_CLIENT_NOTFOUND)
    {
        LOGENTRY;
        return -ENOENT;
    }

    //stbuf->st_mode = fa.mode;
    stbuf->st_mode = 0777;

    if (fa.is_dir)
    {
        stbuf->st_mode |= S_IFDIR;
    }
    else
    {
        stbuf->st_mode |= S_IFREG;
    }

    stbuf->st_size = fa.size;

    LOGENTRY;
    return 0;
}

static int fusewtf_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi)
{
    (void) offset;
    (void) fi;
    LOGENTRY;
    wtf_client_returncode status;
    char *de;
    int ret = 0;

    int64_t reqid = w->readdir(path, &de, &status);

    if (reqid < 0)
    {
        return -1;
    }

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, "/", NULL, 0);
    filler(buf, "/foo", NULL, 0);

    char abspath[PATH_MAX]; 
    w->canon_path(path, abspath, PATH_MAX); 

    while(w->loop(reqid, -1, &status) > -1)
    {
        if(strcmp(de, abspath) == 0 || strcmp(de, "") == 0) 
        {
            std::cout << "Skipping " << std::string(de) << std::endl;
            continue;
        }

        std::cout << std::string(de) << std::endl;
        //if(filler(buf, de, NULL, 0))
            //break;
    }
    
    LOGENTRY;
    return ret;
}

static int fusewtf_open(const char *path, struct fuse_file_info *fi)
{
    LOGENTRY;
    int ret = 0;
    mode_t mode = 0;
    wtf_client_returncode status;

    int64_t reqid = w->open(path, fi->flags, mode, DEFAULT_REPLICATION, DEFAULT_BLOCK_LENGTH, (int64_t*)&fi->fh, &status);

    if (fi->fh < 0)
    {
        LOGENTRY;
        ret = -ENOENT;
    }

    LOGENTRY;
    return ret;
}


static int fusewtf_flush(const char *path, struct fuse_file_info *fi)
{
    LOGENTRY;
    int ret = 0;
    std::cout << "Flush not implemented." << std::endl;
    wtf_client_returncode status;
    //w->flush(fi->fh, &status);
    return ret;
}

static int fusewtf_release(const char *path, struct fuse_file_info *fi)
{
    LOGENTRY;
    int ret = 0;
    wtf_client_returncode status;
    ret = w->close(fi->fh, &status);
    return ret;
}


static int fusewtf_read(const char *path, char *buf, size_t size, off_t offset,
        struct fuse_file_info *fi)
{
    LOGENTRY;
    size_t read_size = size;
    wtf_client_returncode status, lstatus;

    int64_t reqid = w->read((int64_t)fi->fh, buf, &read_size, &status);

    if(reqid < 0)
    {
        LOGENTRY;
        return -1;
    }

    if (w->loop(reqid, -1, &lstatus) < 0)
    {
        LOGENTRY;
        status = lstatus;
        return -1;
    }

    
    return read_size;
}

static int fusewtf_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    LOGENTRY;
    wtf_client_returncode status, lstatus;

    if(w->lseek(fi->fh, offset, SEEK_SET, &status) < 0)
    {
        return -1;
    }

    if (w->write((int64_t)fi->fh, buf, &size, &status) < 0)
    {
        LOGENTRY;
        return -1;
    }

    if (w->loop(fi->fh, -1, &lstatus) < 0)
    {
        LOGENTRY;
        status = lstatus;
        return -1;
    }

    LOGENTRY;
    return size;
}

static int fusewtf_rename(const char *old_path, const char *new_path)
{
    LOGENTRY;
    return 0;
}

static int fusewtf_unlink(const char *path)
{
    LOGENTRY;
    return 0;
}

static int fusewtf_readlink(const char *path, char *buf, size_t bufsiz)
{
    LOGENTRY;
    return 0;
}

static int fusewtf_mknod(const char *pathname, mode_t mode, dev_t dev)
{
    LOGENTRY;
    return 0;
}

static int fusewtf_mkdir(const char *pathname, mode_t mode)
{
    LOGENTRY;
    int reqid;
    wtf_client_returncode status, lstatus;
    reqid = w->mkdir(pathname, mode, &status);

    if (reqid < 0)
    {
        LOGENTRY;
        return -1;
    }

    if (w->loop(reqid, -1, &lstatus) < 0)
    {
        LOGENTRY;
        return -1;
    }

    return 0;
}

static int fusewtf_symlink(const char *old_path, const char *new_path)
{
    LOGENTRY;
    return 0;
}

static int fusewtf_rmdir(const char *path)
{
    LOGENTRY;
    return 0;
}

static int fusewtf_link(const char *old_path, const char *new_path)
{
    LOGENTRY;
    return 0;
}

static int fusewtf_chmod(const char *path, mode_t mode)
{
    LOGENTRY;
    return 0;
}

static int fusewtf_chown(const char *path, uid_t owner, gid_t group)
{
    LOGENTRY;
    return 0;
}

static int fusewtf_truncate(const char *path, off_t length)
{
    LOGENTRY;
    int ret = 0;
    wtf_client_returncode status;
    int64_t fd;
    mode_t mode = 0;

    int64_t reqid = w->open(path, O_WRONLY, mode, DEFAULT_REPLICATION, DEFAULT_BLOCK_LENGTH, &fd, &status);

    if (fd < 0)
    {
        ret = -ENOENT;
    }

    w->truncate(fd, length, &status);
    w->close(fd, &status);

    return 0;
}

static int fusewtf_ftruncate(const char *path, off_t length, struct fuse_file_info *fi)
{
    LOGENTRY;
    wtf_client_returncode status;
    w->truncate(fi->fh, length, &status);
    return 0;
}

static int fusewtf_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    LOGENTRY;
    return 0;
}

static struct fuse_operations fusewtf_oper;

int main(int argc, char *argv[])
{
    int ret = 0;

    w = new client("127.0.0.1", 1981, "127.0.0.1", 1982);
//    fusewtf_oper.flush      = fusewtf_flush;
    fusewtf_oper.getattr    = fusewtf_getattr;
    fusewtf_oper.open       = fusewtf_open;
    fusewtf_oper.read       = fusewtf_read;
//    fusewtf_oper.rename     = fusewtf_rename;
    fusewtf_oper.readdir    = fusewtf_readdir;
//    fusewtf_oper.release    = fusewtf_release;
//    fusewtf_oper.unlink     = fusewtf_unlink;
//    fusewtf_oper.utimens    = fusewtf_utimens;
//    fusewtf_oper.write      = fusewtf_write;
//    fusewtf_oper.readlink   = fusewtf_readlink;
//    fusewtf_oper.mknod      = fusewtf_mknod;
    fusewtf_oper.mkdir      = fusewtf_mkdir; 
//    fusewtf_oper.symlink    = fusewtf_symlink;
//    fusewtf_oper.rmdir      = fusewtf_rmdir;
//    fusewtf_oper.link       = fusewtf_link;
//    fusewtf_oper.chmod      = fusewtf_chmod;
//    fusewtf_oper.chown      = fusewtf_chown;
//    fusewtf_oper.truncate   = fusewtf_truncate;
//    fusewtf_oper.ftruncate   = fusewtf_ftruncate;
//    fusewtf_oper.fsync      = fusewtf_fsync;
#if FUSE_VERSION >= 25
    fusewtf_oper.create     = fusewtf_create;
#endif


    ret = fuse_main(argc, argv, &fusewtf_oper, NULL);


    delete w;
    return ret;
}

#ifdef __cplusplus
}
#endif //__cplusplus
