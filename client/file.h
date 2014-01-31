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

#ifndef wtf_file_h_
#define wtf_file_h_

// e
#include <e/intrusive_ptr.h>

// STL
#include <vector>
#include <map>

//PO6
#include <po6/pathname.h>

//WTF
#include <client/wtf.h>
#include <common/block.h>
#include <common/block_id.h>

class wtf_client::file
{

    public:
        file(const char* path);
        ~file() throw ();

    public:
        void set_fd(int64_t fd) { m_fd = fd; }
        int64_t fd() { return m_fd; }
        void path(const char* path) { m_path = po6::pathname(path); }
        po6::pathname path() { return m_path; }
        void add_command(int64_t op);
        int64_t gc_completed(wtf_returncode* rc);
        void set_offset(uint64_t offset) { m_offset = offset; }
        uint64_t offset() { return m_offset; }
        void update_blocks(uint64_t block_index, uint64_t len, 
                           uint64_t version, uint64_t sid,
                           uint64_t bid);
        void update_blocks(uint64_t bid, e::intrusive_ptr<wtf::block>& b);
        uint64_t get_block_version(uint64_t bid);
        uint64_t get_block_length(uint64_t bid);
        uint64_t pack_size();
        uint64_t size() { return m_block_map.size(); }
        uint64_t length();
        wtf::block_id lookup_block(uint64_t index) { return m_block_map[index]->get_first_location(); }
        std::vector<wtf::block_id>::iterator lookup_block_begin(uint64_t index) { return m_block_map[index]->blocks_begin(); }
        std::vector<wtf::block_id>::iterator lookup_block_end(uint64_t index) { return m_block_map[index]->blocks_end(); }
        void truncate();
        void truncate(off_t length);

    private:
        friend class e::intrusive_ptr<file>;
        friend e::buffer::packer 
            operator << (e::buffer::packer pa, const wtf_client::file& rhs);
        friend e::unpacker 
            operator >> (e::unpacker up, wtf_client::file& rhs);

    private:
        file(const file&);

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        typedef std::map<uint64_t, e::intrusive_ptr<wtf::block> > block_map;
        file& operator = (const file&);

    private:
        size_t m_ref;
        po6::pathname m_path;
        wtf_client* m_client;
        int64_t m_fd;
        std::list<int64_t> m_commands;
        block_map m_block_map;
        uint64_t m_offset;

    public:
        bool is_directory;
        int flags;
        uint64_t mode;

    public:
        block_map::const_iterator blocks_begin() { return m_block_map.begin(); }
        block_map::const_iterator blocks_end() { return m_block_map.end(); }
};

inline e::buffer::packer 
operator << (e::buffer::packer pa, const wtf_client::file& rhs) 
{ 
    pa = pa << rhs.m_block_map.size(); 

    for (wtf_client::file::block_map::const_iterator it = rhs.m_block_map.begin();
            it != rhs.m_block_map.end(); ++it)
    {
        pa = pa << it->first << *it->second;
    }

    return pa;
} 

inline e::unpacker 
operator >> (e::unpacker up, wtf_client::file& rhs) 
{ 
    uint64_t sz;
    up = up >> sz; 

    for (int i = 0; i < sz; ++i) 
    {
        e::intrusive_ptr<wtf::block> b = new wtf::block();
        up = up >> *b;
        rhs.update_blocks(i, b); 
    }

    return up; 
}

#endif // wtf_file_h_
