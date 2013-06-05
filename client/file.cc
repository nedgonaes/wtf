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

wtf_client :: file :: file(const char* path)
    : m_ref(0)
    , m_path(path)
    , m_commands()
    , m_fd(0)
    , m_block_map()
    , m_offset(0)
{
}

wtf_client :: file :: ~file() throw ()
{
}

wtf_client::command_map::iterator
wtf_client :: file :: commands_begin()
{
    return m_commands.begin();
}

wtf_client::command_map::iterator
wtf_client :: file :: commands_end()
{
    return m_commands.end();
}

int64_t
wtf_client :: file :: gc_completed(wtf_returncode* rc)
{
    std::vector<int64_t> complete;

    for (command_map::iterator it = m_commands.begin();
         it != m_commands.end(); ++it)
    {
        if (it->second->status() == WTF_SUCCESS)
        {
            complete.push_back(it->first);
        }
    }

    for (std::vector<int64_t>::iterator it = complete.begin();
         it != complete.end(); ++it)
    {
        m_commands.erase(*it);
    }

    *rc = WTF_SUCCESS;
    return 0;
}

void
wtf_client :: file :: add_command(e::intrusive_ptr<command>& cmd)
{
    m_commands[cmd->nonce()] = cmd;
}