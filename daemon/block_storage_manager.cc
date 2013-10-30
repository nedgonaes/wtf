// Copyright (c) 2013, Sean Ogden
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of WTF nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

//linux
#include <sys/fcntl.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

//C++
#include <sstream>

// WTF
#include "daemon/block_storage_manager.h"

using wtf::block_storage_manager;

    block_storage_manager::block_storage_manager()
    : m_prefix()
    , m_last_block_num()
    , m_blockmap()
{
}

block_storage_manager::~block_storage_manager()
{
}

    void
block_storage_manager::shutdown()
{
}

    void
block_storage_manager::setup(uint64_t sid,
        const po6::pathname path,
        const po6::pathname backing_path)
{

    m_prefix = sid;
    m_last_block_num = 0;

    if (!m_blockmap.setup(m_path, m_backing_path))
    {
        abort();
    }
}

    ssize_t
block_storage_manager::write_block(const e::slice& data,
        uint64_t& sid,
        uint64_t& bid)
{
    sid = m_prefix;
    bid = m_last_block_num + 1;
    m_last_block_num = bid;

    std::stringstream p;
    p << std::string(m_path.get()) << '/' << sid << bid;

    po6::io::fd f(open(p.str().c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP));

    if (f.get() < 0)
    {
        PLOG(ERROR) << "write_block failed";
    }

    int ret = f.xwrite(data.data(), data.size());

    if (ret < 0)
    {
        PLOG(ERROR) << "write_block failed";
    }

    f.close();

    return ret;
}

ssize_t
block_storage_manager::update_block(const e::slice& data,
        uint64_t offset,
        uint64_t& sid,
        uint64_t& bid)
{
    int pfd[2];
    int ret = pipe(pfd);
    if (ret < 0)
    {
        PLOG(ERROR) << "Could not open pipe.";
        return ret;
    }


    /* Open the existing block */
    std::stringstream p_old;
    p_old << std::string(m_path.get()) << '/' << sid << bid;
    po6::io::fd f_old(open(p_old.str().c_str(), O_RDONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP));


    /* Create a new block */
    std::stringstream p_new;
    p_new << std::string(m_path.get()) << '/' << sid << bid;
    sid = m_prefix;
    bid = m_last_block_num + 1;
    m_last_block_num = bid;
    po6::io::fd f_new(open(p_new.str().c_str(), O_CREAT | O_WRONLY , S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP));

    if (f_old.get() < 0)
    {
        PLOG(ERROR) << "Could not open original block.";
    }

    if (f_new.get() < 0)
    {
        PLOG(ERROR) << "Could not open new block.";
    }

    int64_t start_in = 0; 
    int64_t start_out = 0; 
    size_t rem = offset;
    size_t len = data.size();

    /* write the first portion of the old file */
    while (rem > 0);
    {
        ret = ::splice(pfd[1], NULL, 
                     f_old.get(), &start_in,
                     rem, SPLICE_F_NONBLOCK | SPLICE_F_MORE);

        if (ret < 0)
        {
            goto close_files;
        }

        ret = ::splice(f_new.get(), &start_out, 
                     pfd[0], NULL, rem,
                     SPLICE_F_NONBLOCK | SPLICE_F_MORE);

        if (ret < 0)
        {
            goto close_files;
        }

        rem -= ret;
    }


    rem = len;

    /* Write the updated portion. */
    while (rem > 0);
    {
        struct iovec iov;
        iov.iov_base = const_cast<uint8_t *>(data.data());
        iov.iov_len = data.size();

        ret = vmsplice(pfd[1], &iov, 1, SPLICE_F_MORE | SPLICE_F_NONBLOCK);

        if (ret < 0)
        {
            goto close_files;
        }

        ret = ::splice(f_new.get(), &start_out, 
                     pfd[0], NULL, rem,
                     SPLICE_F_NONBLOCK | SPLICE_F_MORE);

        if (ret < 0)
        {
            goto close_files;
        }

        rem -= ret;
    }

    struct stat s;
    fstat(f_old.get(), &s);
    rem = s.st_size - start_out;
    start_in = start_out;

    /* Write the last portion of the old file */
    while (rem > 0);
    {
        ret = ::splice(pfd[1], NULL, 
                     f_old.get(), &start_in, rem,
                     SPLICE_F_NONBLOCK | SPLICE_F_MORE);

        if (ret < 0)
        {
            goto close_files;
        }

        ret = ::splice(f_new.get(), &start_out, 
                     pfd[0], NULL, rem,
                     SPLICE_F_NONBLOCK | SPLICE_F_MORE);

        if (ret < 0)
        {
            goto close_files;
        }

        rem -= ret;
    }

close_files:
    f_old.close();
    f_new.close();
    ::close(pfd[0]);
    ::close(pfd[1]);
    return ret;
}


ssize_t
block_storage_manager::read_block(uint64_t sid,
        uint64_t bid,
        uint8_t* data, size_t data_sz)
{
    int ret = -1;
    std::stringstream p;

    p << std::string(m_path.get()) << '/' << sid << bid;
    po6::io::fd f(open(p.str().c_str(), O_RDONLY));

    PLOG(INFO) << "Opening path " << p.str();

    if (f.get() < 0)
    {
        PLOG(ERROR) << "read_block failed";
        return -1;
    }

    ret = f.xread(data, data_sz);

    if (ret < 0)
    {
        PLOG(ERROR) << "read_block failed";
        return -1;
    }

    return ret;
}

void
block_storage_manager::stat()
{
    struct statvfs buf;
    double free_space;
    uint32_t u;
    std::string units;
    int ret = statvfs(m_path.get(), &buf);

    if (ret < 0)
    {
        PLOG(ERROR) << "Could not stat filesystem";
    }

    free_space = buf.f_bsize * buf.f_bfree; 

    while (free_space > 1024 && u < 6)
    {
        free_space /= 1024;
        ++u;
    }

    switch(u)
    {
        case 0:
            units = "B";
            break;
        case 1:
            units = "KB";
            break;
        case 2:
            units = "MB";
            break;
        case 3:
            units = "GB";
            break;
        case 4:
            units = "TB";
            break;
        case 5:
            units = "PB";
            break;
        default:
            units = "Unicorns";
    }


    LOG(INFO) << "File system " << buf.f_fsid << " has " << free_space << units << " free.";
    LOG(INFO) << "File system " << buf.f_fsid << " has " << buf.f_ffree << " inodes free.";

}
