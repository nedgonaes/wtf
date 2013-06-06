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

//PO6
#include <po6/pathname.h>

//WTF
#include <client/command.h>
#include <client/wtf.h>
#include <common/block.h>

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
        void add_command(e::intrusive_ptr<command>& c);
        int64_t gc_completed(wtf_returncode* rc);
        command_map::iterator commands_begin();
        command_map::iterator commands_end(); 
        void set_offset(uint64_t offset) { m_offset = offset; }
        void update_blocks(uint64_t offset, uint64_t len, 
                           uint64_t version, uint64_t sid,
                           uint64_t bid);

    private:
        friend class e::intrusive_ptr<file>;

    private:
        file(const file&);

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        file& operator = (const file&);
        typedef std::map<uint64_t, wtf::block > block_map;

    private:
        size_t m_ref;
        po6::pathname m_path;
        wtf_client* m_client;
        int64_t m_fd;
        command_map m_commands;
        block_map m_block_map;
        uint64_t m_offset;
};

#endif // wtf_file_h_
