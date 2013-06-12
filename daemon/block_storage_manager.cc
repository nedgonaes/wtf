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
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>

//C++
#include <sstream>

// WTF
#include "daemon/block_storage_manager.h"

using wtf::block_storage_manager;

block_storage_manager::block_storage_manager()
   : m_prefix()
   , m_last_block_num()
   , m_path()
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
                             const po6::pathname path)
{
    m_path = path;
    m_prefix = sid;
    m_last_block_num = 0;
    if (mkdir(m_path.get(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) < 0)
    {
        switch(errno)
        {
            case EEXIST:
                break;
            case ENOENT:
            case ENOSPC:
            case ENOTDIR:
            case EROFS:
            default:
                PLOG(ERROR) << "Failed to create data directory.";
                abort();
        }
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

    return ret;
}

ssize_t
block_storage_manager::read_block(uint64_t sid,
                                  uint64_t bid,
                                  std::vector<uint8_t>& data)
{
    int ret;
    std::stringstream p;

    p << std::string(m_path.get()) << '/' << sid << bid;
    po6::io::fd f(open(p.str().c_str(), O_RDONLY));

    if (f.get() < 0)
    {
        PLOG(ERROR) << "read_block failed";
    }

    ret = f.xread(&data[0], data.size());

    if (ret < 0)
    {
        PLOG(ERROR) << "read_block failed";
    }

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
