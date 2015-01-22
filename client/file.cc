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
#include "client/pending_write.h"
#include "common/block.h"
#include "common/block_location.h"

#define DEFAULT_BLOCK_SIZE 4096 

using wtf::file;
using wtf::block_location;

file :: file(const char* path, size_t reps, size_t block_sz)
    : m_ref(0)
    , m_path(path)
    , m_pending()
    , m_fd(0)
    , m_block_map()
    , m_last_op()
    , m_offset(0)
    , m_replicas(reps)
    , is_directory(false)
    , flags(0)
    , mode(0)
    , m_block_size(block_sz)
{
}

file :: ~file() throw ()
{
}

bool
file :: has_last_op(uint32_t block_offset)
{
    return (m_last_op.find(block_offset) != m_last_op.end());
}

e::intrusive_ptr<wtf::pending_write>
file :: last_op(uint32_t block_offset)
{
    return m_last_op.find(block_offset)->second;
}

void
file :: set_last_op(uint32_t block_offset, e::intrusive_ptr<wtf::pending_write> op)
{
    m_last_op[block_offset] = op;
}

bool 
file :: pending_ops_empty()
{
    return m_pending.empty();
}

int64_t 
file :: pending_ops_pop_front()
{
    int64_t client_id = m_pending.front();
    m_pending.pop_front();
    return client_id;
}

void
file :: add_pending_op(uint64_t client_id)
{
    m_pending.push_back(client_id);
}

void
file :: insert_block(uint64_t insert_address, wtf::slice& slc)
{
    m_block_map.insert(insert_address, slc);
}

void
file :: apply_changeset(std::map<uint64_t, e::intrusive_ptr<block> >& changeset)
{
    std::map<uint64_t, e::intrusive_ptr<block> >::iterator it = changeset.begin();

    for (; it != changeset.end(); ++it)
    {
        uint64_t insert_address = it->first;
        uint64_t insert_length = it->second->length();
        std::vector<block_location> bl = it->second->blocks();
        m_block_map.insert(insert_address, insert_length, bl);
    }
}

void
file :: truncate(size_t length)
{
    m_block_map.truncate(length);
}

uint64_t
file :: length() const
{
    return m_block_map.length();
}

std::auto_ptr<e::buffer>
file :: serialize_blockmap()
{
    std::cout << "Packing blockmap of size " << this->pack_size() << std::endl;
    std::auto_ptr<e::buffer> blockmap(e::buffer::create(pack_size()));
    e::buffer::packer pa = blockmap->pack_at(0); 

    pa = pa << *this;

    return blockmap;
}

uint64_t
file :: pack_size()
{
    uint64_t ret = 2*sizeof(uint64_t) /* replicas, block_size */;
    ret += m_block_map.pack_size();

    return ret;
}

