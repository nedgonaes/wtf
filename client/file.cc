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

#define __STDC_LIMIT_MACROS

//STL
#include <vector>

// e
#include <e/endian.h>

// WTF
#include "client/file.h"

using wtf::file;

file :: file(const char* path)
    : m_ref(0)
    , m_path(path)
    , m_commands()
    , m_fd(0)
    , m_block_map()
    , m_offset(0)
    , is_directory(false)
    , flags(0)
{
}

file :: ~file() throw ()
{
}

void
file :: update_blocks(uint64_t block_index, uint64_t len, 
                           uint64_t version, uint64_t sid,
                           uint64_t bid)
{

    //std::cout << "Updating block " << block_index << std::endl;

    if (m_block_map.find(block_index) == m_block_map.end())
    {
        //std::cout << "block didn't exist." << std::endl;
        m_block_map[block_index] = new block();
    }

    m_block_map[block_index]->update(version, len, block_location(sid, bid), true); 
}

void
file :: update_blocks(uint64_t bid, e::intrusive_ptr<block>& b)
{
    //std::cout << "Caching block " << bid << ": " << *b << std::endl;
    m_block_map[bid] = b;
}

uint64_t
file :: get_block_version(uint64_t bid)
{
    if (m_block_map.find(bid) == m_block_map.end())
    {
        return 0;
    }

    return m_block_map[bid]->version();
}

uint64_t
file :: get_block_length(uint64_t bid)
{
    if (m_block_map.find(bid) == m_block_map.end())
    {
        return 0;
    }

    return m_block_map[bid]->length();
}

uint64_t
file :: length()
{
    if (m_block_map.size() == 0)
    {
        return 0;
    }

    block_map::iterator it = m_block_map.end();
    it--;
    uint64_t bid = it->first;
    uint64_t len = it->second->length();
    return CHUNKSIZE*bid + len;
}

uint64_t
file :: pack_size()
{
    uint64_t ret = sizeof(uint64_t); /* number of blocks */

    for (file::block_map::const_iterator it = m_block_map.begin();
         it != m_block_map.end(); ++it)
    {
        ret += sizeof(uint64_t) + it->second->pack_size();
    }

    return ret;
}

void
file :: truncate(off_t length)
{
    uint64_t block_index = length/CHUNKSIZE;
    uint64_t sz = m_block_map.size();

    if (sz > block_index + 1)
    {
        for (int i = block_index + 1; i < sz; ++i)
        {
            m_block_map.erase(i);
        }
    }

    e::intrusive_ptr<block> b = m_block_map[block_index];
    b->resize(length - block_index*CHUNKSIZE);
    m_block_map[block_index] = b;
    std::cout << "BLOCK " << block_index << " LEN: " << m_block_map[block_index]->length() << std::endl;
    std::cout << "FILE LENGTH IS NOW " << this->length() << std::endl;
}

void
file :: truncate()
{
    uint64_t block_index = m_offset/CHUNKSIZE;
    uint64_t sz = m_block_map.size();

    for (int i = block_index + 1; i < sz; ++i)
    {
        m_block_map.erase(i);
    }
}
