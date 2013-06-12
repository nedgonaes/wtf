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
#include <client/wtf.h>
#include <common/block.h>

using wtf::block;

block :: block()
   : m_ref(0)
   , m_block_list()
   , m_version(0)
   , m_length(0)
   , m_dirty(false)
{
}

block :: ~block() throw()
{
}

void
block :: update(uint64_t version,
                uint64_t len,
                const wtf::block_id& bid,
                bool dirty)
{
    if (dirty)
    {
        m_dirty = dirty;
    }

    if(m_version < version)
    {
        m_length = len;
        m_version = version;
        m_block_list.clear();
    }

    m_block_list.push_back(bid);
}

uint64_t
block :: pack_size()
{
    uint64_t ret = 2 * sizeof(uint64_t);  /* length, size */
    ret += m_block_list.size() * block_id::pack_size() ; /* server, block */
    return ret;
}

void 
block :: pack(char* buf) const
{
    uint64_t sz = m_block_list.size();

    memmove(buf, &m_length, sizeof(uint64_t));  
    buf += sizeof(uint64_t);

    memmove(buf, &sz, sizeof(uint64_t));  
    buf += sizeof(uint64_t);

    for (block::block_list::const_iterator it = m_block_list.begin();
            it != m_block_list.end(); ++it)
    {
        it->pack(buf);
        buf += block_id::pack_size();
    }
}
