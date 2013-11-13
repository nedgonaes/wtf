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
#include "vblock.h"

using wtf::vblock;

vblock :: vblock()
   : m_ref(0)
   , m_slice_map()
{
}

vblock :: ~vblock() throw()
{
}

vblock :: slice :: slice()
    : m_ref(0)
    , m_offset(0)
    , m_length(0)
    , m_disk_offset(0)
{
}

vblock :: slice :: slice(size_t offset, size_t length, size_t disk_offset)
    : m_ref(0)
    , m_offset(offset)
    , m_length(length)
    , m_disk_offset(disk_offset)
{
}

vblock :: slice :: ~slice() throw()
{
}

void
vblock :: update(size_t offset, size_t len, size_t disk_offset)
{
    size_t cursor = 0;
    size_t start = offset;
    size_t end = offset + len;

    vblock::slice_map::iterator after = m_slice_map.upper_bound(start);
    e::intrusive_ptr<vblock::slice> slc(new vblock::slice(start, len, disk_offset));

    //at or beyond the end of the block 
    if (after != m_slice_map.end())
    {
        size_t old_length = after->second->m_length;
        size_t old_start = after->second->m_offset;

        if (old_start != start)
        {
            m_slice_map[start] = slc;
        }
        else
        {
           if (old_length > len)
           {
               //we found a slice that starts at the same offset
               //but is larger, so we need to split the slice..
           }
           else if (old_length < len)
           {
               //we found a slice that starts at the same offset
               //but is smaller, so we get rid of this and look at
               //the next one.
                m_slice_map[start] = slc;

                //XXX: look at next blocks and see how they should
                //     be adjusted.
                while (++after != m_slice_map.end())
                {
                    old_length = after->second->m_length;
                    old_start = after->second->m_offset;

                    if(old_start + old_length > offset)
                    {
                    }
                    else
                    {
                    }
                }
           }
           else
           {
               //we found a block that starts at the same offset.
               //and ends at the same offset, so we can simply replace.
                m_slice_map[start] = slc;
                return;
           }
        }
    }
    else
    {
        //just put it at the end.
        m_slice_map[start] = slc;
    }
}

