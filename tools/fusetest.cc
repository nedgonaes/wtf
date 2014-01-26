/*
 * Use FUSE to interact with files in WTF, through HyperDex
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <client/wtf.h>

#include <semaphore.h>

wtf_client* w;

const char *ROOT = "/";
const char *log_name = "logfusetest";
FILE *logfile;

#define LOGENTRY std::cout << __FILE__ << "::" << __LINE__<< "::" << __func__ << std::endl

sem_t lock;

#ifdef __cplusplus
extern "C"
{
#endif //__cplusplus


static int fusetest_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    LOGENTRY;
    sem_wait(&lock);
    int64_t fd = w->open(path, O_CREAT, mode);

    if (fd < 0)
    {
        sem_post(&lock);
        return -1;
    }
    
    fi->fh = fd;

    std::cout << "OPENED FD " << fd << std::endl;
    sem_post(&lock);
    return 0;
}

static int fusetest_utimens(const char *path, const struct timespec tv[2])
{
    LOGENTRY;
    std::cout << "\t\t\t\tutimens [" << path << "]" << std::endl;
    std::cout << "XXX: utimens not implemented" << std::endl;
    return 0;
}

static int fusetest_getattr(const char *path, struct stat *stbuf)
{
    LOGENTRY;
    sem_wait(&lock);
    memset(stbuf, 0, sizeof(struct stat));

    struct wtf_file_attrs fa;
    if (w->getattr(path, &fa) < 0)
    {
        LOGENTRY;
        std::cout << "ERRNO " << errno << std::endl;
        sem_post(&lock);
        return -errno;
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

    std::cout << "MODE: " << stbuf->st_mode << std::endl;
    std::cout << "SIZE: " << stbuf->st_size << std::endl;

    LOGENTRY;
    sem_post(&lock);
    return 0;
}

static int fusetest_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi)
{
    LOGENTRY;
    std::cout << "XXX:READDIR" << std::endl;
    return 0;
}

static int fusetest_open(const char *path, struct fuse_file_info *fi)
{
    LOGENTRY;
    int ret = 0;
    sem_wait(&lock);
    int64_t fd = w->open(path, fi->flags);
    if (fd < 0)
    {
        LOGENTRY;
        ret = -ENOENT;
    }
    else
    {
        fi->fh = fd;
    }

    std::cout << "OPENED FD " << fd << std::endl;

    sem_post(&lock);
    LOGENTRY;
    return ret;
}


static int fusetest_flush(const char *path, struct fuse_file_info *fi)
{
    LOGENTRY;
    int ret = 0;
    std::cout << "FD = " << fi->fh << std::endl;
    wtf_returncode status;
    sem_wait(&lock);
    w->flush(fi->fh, &status);
    sem_post(&lock);
    return ret;
}

static int fusetest_release(const char *path, struct fuse_file_info *fi)
{
    LOGENTRY;
    int ret = 0;
    sem_wait(&lock);
    wtf_returncode status;
    ret = w->close(fi->fh, &status);
    sem_post(&lock);
    return ret;
}


static int fusetest_read(const char *path, char *buf, size_t size, off_t offset,
        struct fuse_file_info *fi)
{
    LOGENTRY;
    uint32_t read_size = size;
    wtf_returncode status, lstatus;

    std::cout << "reading " << size << " bytes from " << path << " at offset " << offset << std::endl;

    sem_wait(&lock);
    if (w->read(fi->fh, buf, &read_size, &status) < 0)
    {
        LOGENTRY;
        sem_post(&lock);
        return -1;
    }

    if (w->flush(fi->fh, &lstatus) < 0)
    {
        LOGENTRY;
        status = lstatus;
        sem_post(&lock);
        return -1;
    }

    
    std::cout << "READ RETURNED " << read_size << " bytes :"  << std::string(buf, read_size) << std::endl;
    sem_post(&lock);
    return read_size;
}

static int fusetest_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    LOGENTRY;
    wtf_returncode status, lstatus;

    sem_wait(&lock);
    w->lseek(fi->fh, offset);
    if (w->write(fi->fh, buf, size, 3, &status) < 0)
    {
        LOGENTRY;
        sem_post(&lock);
        return -1;
    }

    if (w->flush(fi->fh, &lstatus) < 0)
    {
        LOGENTRY;
        status = lstatus;
        sem_post(&lock);
        return -1;
    }

    LOGENTRY;
    sem_post(&lock);
    return size;
}

static int fusetest_rename(const char *old_path, const char *new_path)
{
    LOGENTRY;
    printf("\t\t\t\trename [%s] [%s]\n", old_path, new_path);
    return 0;
}

static int fusetest_unlink(const char *path)
{
    LOGENTRY;
    printf("\t\t\t\tunlink [%s]\n", path);
    return 0;
}

static int fusetest_readlink(const char *path, char *buf, size_t bufsiz)
{
    LOGENTRY;
    printf("\t\t\t\treadlink [%s]\n", path);
    return 0;
}

static int fusetest_mknod(const char *pathname, mode_t mode, dev_t dev)
{
    LOGENTRY;
    printf("\t\t\t\tmknod [%s]\n", pathname);
    return 0;
}

static int fusetest_mkdir(const char *pathname, mode_t mode)
{
    LOGENTRY;
    printf("\t\t\t\tmkdir [%s]\n", pathname);
    int ret;
    sem_wait(&lock);
    ret = w->mkdir(pathname, mode);
    sem_post(&lock);
    return ret;
}

static int fusetest_symlink(const char *old_path, const char *new_path)
{
    LOGENTRY;
    printf("\t\t\t\tsymlink [%s] [%s]\n", old_path, new_path);
    return 0;
}

static int fusetest_rmdir(const char *path)
{
    LOGENTRY;
    printf("\t\t\t\trmdir [%s]\n", path);
    return 0;
}

static int fusetest_link(const char *old_path, const char *new_path)
{
    LOGENTRY;
    printf("\t\t\t\tlink [%s] [%s]\n", old_path, new_path);
    return 0;
}

static int fusetest_chmod(const char *path, mode_t mode)
{
    LOGENTRY;
    printf("\t\t\t\tchmod [%s]\n", path);
    return 0;
}

static int fusetest_chown(const char *path, uid_t owner, gid_t group)
{
    LOGENTRY;
    printf("\t\t\t\tchown [%s]\n", path);
    return 0;
}

static int fusetest_truncate(const char *path, off_t length)
{
    LOGENTRY;
    sem_wait(&lock);
    int ret = 0;
    wtf_returncode status;
    int64_t fd;
    fd = w->open(path, O_WRONLY);
    if (fd < 0)
    {
        ret = -ENOENT;
    }

    w->truncate(fd, length);
    w->close(fd, &status);
    printf("\t\t\t\ttruncate [%s] length [%zu]\n", path, length);
    sem_post(&lock);
    return 0;
}

static int fusetest_ftruncate(const char *path, off_t length, struct fuse_file_info *fi)
{
    LOGENTRY;
    sem_wait(&lock);
    printf("\t\t\t\tftruncate [%s] length [%zu]\n", path, length);
    w->truncate(fi->fh, length);
    sem_post(&lock);
    return 0;
}

static int fusetest_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    LOGENTRY;
    printf("\t\t\t\tfsync [%s]\n", path);
    return 0;
}

static struct fuse_operations fusetest_oper;

int main(int argc, char *argv[])
{
    int ret = 0;

    w = new wtf_client("127.0.0.1", 1981, "127.0.0.1", 1982);
    fusetest_oper.flush      = fusetest_flush;
    fusetest_oper.getattr    = fusetest_getattr;
    fusetest_oper.open       = fusetest_open;
    fusetest_oper.read       = fusetest_read;
//    fusetest_oper.rename     = fusetest_rename;
    fusetest_oper.readdir    = fusetest_readdir;
    fusetest_oper.release    = fusetest_release;
//    fusetest_oper.unlink     = fusetest_unlink;
//    fusetest_oper.utimens    = fusetest_utimens;
    fusetest_oper.write      = fusetest_write;
//    fusetest_oper.readlink   = fusetest_readlink;
//    fusetest_oper.mknod      = fusetest_mknod;
    fusetest_oper.mkdir      = fusetest_mkdir; 
//    fusetest_oper.symlink    = fusetest_symlink;
//    fusetest_oper.rmdir      = fusetest_rmdir;
//    fusetest_oper.link       = fusetest_link;
//    fusetest_oper.chmod      = fusetest_chmod;
//    fusetest_oper.chown      = fusetest_chown;
    fusetest_oper.truncate   = fusetest_truncate;
    fusetest_oper.ftruncate   = fusetest_ftruncate;
//    fusetest_oper.fsync      = fusetest_fsync;
#if FUSE_VERSION >= 25
    fusetest_oper.create     = fusetest_create;
#endif

    sem_init(&lock, 0, 1);

    ret = fuse_main(argc, argv, &fusetest_oper, NULL);

    sem_destroy(&lock);

    delete w;
    return ret;
}

#ifdef __cplusplus
}
#endif //__cplusplus
