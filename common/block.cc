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

// WTF
#include <client/client.h>
#include <common/block.h>

using wtf::block;
using wtf::block_location;

block :: block(size_t block_capacity, size_t file_offset, size_t replicas)
   : m_ref(0)
   , m_block_list()
   , m_offset(file_offset)
   , m_length(0)
   , m_capacity(block_capacity)
{
    for (size_t i = 0; i < replicas; ++i)
    {
        m_block_list.push_back(block_location());
    }
}

block :: ~block() throw()
{
}

void
block :: add_replica(const block_location& bl)
{
    m_block_list.push_back(bl);
}

block_location
block :: first_location()
{
    if (m_block_list.empty())
    {
        return block_location();
    }

    return m_block_list[0];
}

uint64_t
block :: pack_size()
{
    uint64_t ret = 4 * sizeof(uint64_t);  /* replicas, offset, length */
    ret += m_block_list.size() * block_location::pack_size() ; /* server, block */
    return ret;
}

