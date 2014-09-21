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

vblock :: slice :: slice(size_t off, size_t len, size_t disk_off)
    : m_ref(0)
    , m_offset(off)
    , m_length(len)
    , m_disk_offset(disk_off)
{
}

vblock :: slice :: ~slice() throw()
{
}

uint64_t
vblock :: length()
{
    if (m_slice_map.empty())
    {
        return 0;
    }

    slice_map::iterator end = m_slice_map.end();
    end--;

    uint64_t offset = end->first;
    uint64_t len = end->second->length();
    return offset + len;
}

void
vblock :: update(size_t off, size_t len, size_t disk_off)
{
    size_t new_start = off;
    size_t new_end = off+ len - 1;


    vblock::slice_map::iterator after = m_slice_map.lower_bound(new_start);
    e::intrusive_ptr<vblock::slice> new_slice(new vblock::slice(new_start, len, disk_off));

    

    //new block
    if (m_slice_map.size() == 0)
    {
        m_slice_map[new_start] = new_slice;
        return;
    }

    //beginning of the block
    if (after == m_slice_map.begin())
    {
        e::intrusive_ptr<slice> old_slice = after->second;
        size_t old_length = old_slice->m_length;
        size_t old_start = old_slice->m_offset;
        size_t old_end = old_start + old_length - 1;

        if (old_start != new_start)
        {
            m_slice_map[new_start] = new_slice;
        }
        else if (old_end <= new_end)
        {
            after++;
            m_slice_map[new_start] = new_slice;

            if (after == m_slice_map.end())
            {
                return;
            }

            old_slice = after->second;
            old_length = old_slice->m_length;
            old_start = old_slice->m_offset;
            old_end = old_start + old_length - 1;
        }
        else
        {
            old_slice->m_length = old_end - new_end;
            old_slice->m_offset = new_end + 1;
            old_slice->m_disk_offset += (new_end + 1 - old_start);

            m_slice_map[old_slice->m_offset] = old_slice;
            m_slice_map[new_start] = new_slice;
            return;
        }

        //remove completely overwritten slices
        while (old_end <= new_end)
        {
            vblock::slice_map::iterator old_iter = after;
            old_slice = old_iter->second;
            after++;

            m_slice_map.erase(old_iter);

            if (after == m_slice_map.end())
            {
                break;
            }

            old_slice = after->second;
            old_start = old_slice->m_offset;
            old_length = old_slice->m_length;
            old_end = old_start + old_length - 1;
        }

        //cut the beginning off the overlapping slice on the right
        if (after != m_slice_map.end())
        {
            old_slice->m_length = old_end - new_end;
            old_slice->m_offset = new_end + 1;
            old_slice->m_disk_offset += (new_end + 1 - old_start);
        }

        //XXX: Check this later.  dinner time.
        return;
    }
    //somewhere in the middle of the block 
    else if (after != m_slice_map.end())
    {
        e::intrusive_ptr<slice> old_slice = after->second;
        size_t old_length = old_slice->m_length;
        size_t old_start = old_slice->m_offset;
        size_t old_end = old_start + old_length - 1;

        if (old_start != new_start)
        {
            //store a pointer to the slice before this one
            //we can do before-- because we know this isn't the first one.
            vblock::slice_map::iterator before = after;
            before--;

            m_slice_map[new_start] = new_slice;

            //remove completely overwritten slices
            while (old_end <= new_end)
            {
                vblock::slice_map::iterator old_iter = after;
                old_slice = old_iter->second;
                after++;

                m_slice_map.erase(old_iter);

                if (after == m_slice_map.end())
                {
                    break;
                }

                old_slice = after->second;
                old_start = old_slice->m_offset;
                old_length = old_slice->m_length;
                old_end = old_start + old_length - 1;
            }

            //cut the beginning off the overlapping slice on the right
            if (after != m_slice_map.end())
            {
                old_slice->m_length = old_end - new_end;
                old_slice->m_offset = new_end + 1;
                old_slice->m_disk_offset += (new_end + 1 - old_start);
            }

            old_slice = before->second;
            old_start = old_slice->m_offset;
            old_length = old_slice->m_length;
            old_end = old_start + old_length - 1;

            //cut the end off the overlapping slice to the left
            if (old_end >= new_start)
            {
                old_slice->m_length -= (old_end - new_start + 1);  
            }

            //mic drop.
            return;
        }

        //starts at the same offset
        else
        {
           if (old_end > new_end)
           {
               //smaller than existing slice, so we need to split the slice..
               old_slice->m_offset = new_end + 1;
               old_slice->m_length = old_end - new_end;
               m_slice_map[new_end + 1] = old_slice;
               m_slice_map[new_start] = new_slice;
               return;
           }
           else if (old_end < new_end)
           {
               //larger than existing slice, so we get rid of this slice
               //and check the next one.  Note that after is == to current
               //slice, so we have to increment before replacing.
                after++;
                m_slice_map[new_start] = new_slice;

                //remove completely overwritten slices
                while (old_end <= new_end)
                {
                    vblock::slice_map::iterator old_iter = after;
                    old_slice = old_iter->second;
                    after++;

                    m_slice_map.erase(old_iter);

                    if (after == m_slice_map.end())
                    {
                        break;
                    }

                    old_slice = after->second;
                    old_start = old_slice->m_offset;
                    old_length = old_slice->m_length;
                    old_end = old_start + old_length - 1;
                }

                //cut the beginning off the overlapping slice on the right
                if (after != m_slice_map.end())
                {
                    old_slice->m_length = old_end - new_end;
                    old_slice->m_offset = new_end + 1;
                    old_slice->m_disk_offset += (new_end + 1 - old_start);
                }

                //fuck yeah.
                return;
           }
           else
           {
               //exact same offset and size as existing block, just replace it
                m_slice_map[new_start] = new_slice;
                return;
           }
        }
    }
    else
    {
        after--;

        //just put it at the end.
        m_slice_map[new_start] = new_slice;

        e::intrusive_ptr<slice> old_slice = after->second;
        size_t old_length = old_slice->m_length;
        size_t old_start = old_slice->m_offset;
        size_t old_end = old_start + old_length - 1;

        //cut the end off the overlapping slice to the left
        if (old_end >= new_start)
        {
            old_slice->m_length -= (old_end - new_start + 1);  
        }

        return;
    }
}

size_t
vblock :: slice :: pack_size()
{ 
    return 3*sizeof(size_t);
}

size_t
vblock :: pack_size()
{ 
    return sizeof(size_t) + m_slice_map.size() * slice::pack_size();
}

ssize_t
vblock :: get_slices(size_t offset, size_t len, vblock::slice_map::const_iterator& slices)
{
    size_t start = offset;
    size_t end = offset + len - 1;

    if (m_slice_map.size() == 0)
    {
        return -1;
    }

    vblock::slice_map::const_iterator after = m_slice_map.lower_bound(offset);

    if (after == m_slice_map.end())
    {
        after--;
        if (after->second->m_offset + after->second->m_length - 1 >= start)
        {
            slices = after;
            return 0;
        }
    }
    else if (after->second->m_offset <= end)
    {
        slices = after;
        return 0;
    }

    return -1;
}

