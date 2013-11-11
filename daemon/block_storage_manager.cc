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

    if (!m_blockmap.setup(path, backing_path))
    {
        abort();
    }
}

    ssize_t
block_storage_manager::write_block(const e::slice& data,
        uint64_t& sid,
        uint64_t& bid)
{
    return m_blockmap.write(data,bid);
}

ssize_t
block_storage_manager::update_block(const e::slice& data,
        uint64_t offset,
        uint64_t& sid,
        uint64_t& bid)
{

    return m_blockmap.update(data,offset,bid);

}


ssize_t
block_storage_manager::read_block(uint64_t sid,
        uint64_t bid,
        uint8_t* data, 
        size_t data_sz)
{
    return m_blockmap.read(bid, data, data_sz);
}

void
block_storage_manager::stat()
{
}
