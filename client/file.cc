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
    , m_current_block()
    , m_bytes_left_in_block(0)
    , m_bytes_left_in_file(0)
    , m_current_block_length(0)
    , m_offset(0)
    , m_file_length(0)
    , m_replicas(reps)
    , is_directory(false)
    , flags(0)
    , mode(0)
    , m_block_size(block_sz)
{
        m_current_block = new block(m_block_size, 0, reps);
        m_block_map[m_offset] = m_current_block;
        m_bytes_left_in_block = m_current_block->capacity();
        m_current_block_length = m_bytes_left_in_block;
}

file :: ~file() throw ()
{
}


block_location 
file :: current_block_location()
{
    return m_current_block->first_location();
}

size_t 
file :: bytes_left_in_block()
{
    return m_bytes_left_in_block;
}

size_t
file :: bytes_left_in_file()
{
    return m_file_length - m_offset;
}

size_t 
file :: current_block_capacity()
{
    return m_current_block->capacity();
}

size_t 
file :: current_block_length()
{
    return m_current_block->length();
}

size_t
file :: current_block_offset()
{
    return m_current_block->capacity() - m_bytes_left_in_block;
}

size_t
file :: current_block_start()
{
    return m_current_block->offset();
}

void 
file :: copy_current_block_locations(std::vector<block_location>& bl)
{
    if (m_current_block->size() == 0)
    {
        for (size_t i = 0; i < m_replicas; ++i)
        {
            /* default block location causes send
               to any server. */
            bl.push_back(block_location());
        }
    }
    else
    {
        bl = m_current_block->m_block_list;
    }

}

size_t
file :: advance_to_end_of_block(size_t len)
{
    size_t ret = 0;

    if (m_bytes_left_in_block == 0)
    {
        move_to_next_block();
    }

    if (len < m_bytes_left_in_block)
    {
        std::cerr << "len = " << len << ", m_bytes_left_in_block = " << m_bytes_left_in_block << std::endl;
        m_offset += len;
        m_bytes_left_in_block -= len;
        ret = len;
    }
    else
    {
        ret = m_bytes_left_in_block;
        m_offset += m_bytes_left_in_block;

        /* updates m_bytes_left_in_block, m_current_block
           and m_current_block_length. creates new empty
           blocks where necessary.*/
        
        move_to_next_block();
    }

    return ret;
}

void
file :: move_to_next_block()
{
    block_map::iterator it = m_block_map.find(m_offset);
    if (it == m_block_map.end())
    {
        m_current_block = new block(m_block_size, m_offset, m_replicas);
        m_block_map[m_offset] = m_current_block;
    }
    else
    {
        m_current_block = it->second;
    }

    m_bytes_left_in_block = m_current_block->capacity();
    m_current_block_length = m_bytes_left_in_block;

    std::cerr << "m_bytes_left_in_block = " << m_bytes_left_in_block << std::endl;
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
file :: insert_block(e::intrusive_ptr<block> bl)
{
    m_block_map[bl->offset()] = bl;
}

void
file :: apply_changeset(std::map<uint64_t, e::intrusive_ptr<block> >& changeset)
{
    std::cerr << "changing file from " << std::endl;
    std::cerr << *this << std::endl << " to " << std::endl;
    
    std::map<uint64_t, e::intrusive_ptr<block> >::iterator last = changeset.end();
    last--;
    uint64_t end = last->first;
    uint64_t start = changeset.begin()->first;

    block_map::iterator lbound = m_block_map.lower_bound(start);
    block_map::iterator ubound = m_block_map.upper_bound(end);
    m_block_map.erase(lbound, ubound);
    m_block_map.insert(changeset.begin(), changeset.end());
    
    last = m_block_map.end();

    m_file_length = 0;

    do {
        last--;

        if (!last->second->is_hole())
         {
             m_file_length = last->first + last->second->length();
             break;
         }

    } while (last != m_block_map.begin());

    std::cerr << *this << std::endl;
}

size_t
file :: get_block_length(size_t offset)
{
    if (m_block_map.find(offset) == m_block_map.end())
    {
        return 0;
    }

    return m_block_map[offset]->capacity();
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

std::auto_ptr<e::buffer>
file :: serialize_blockmap()
{
    size_t sz = sizeof(uint64_t) //blockmap size
              + sizeof(uint64_t); //file length

    for (file::block_map::iterator it = m_block_map.begin(); it != m_block_map.end(); ++it)
    {
        sz += it->second->pack_size();
    }

    std::auto_ptr<e::buffer> blockmap(e::buffer::create(sz));
    e::buffer::packer pa = blockmap->pack_at(0); 

    uint64_t num_blocks = m_block_map.size();
    pa = pa << m_file_length << num_blocks;

    for (file::block_map::iterator it = m_block_map.begin(); it != m_block_map.end(); ++it)
    {
        pa = pa << it->second; 
    }

    return blockmap;
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
    //XXX: implement truncate 
}

void
file :: truncate()
{
    //XXX: implement truncate
}
