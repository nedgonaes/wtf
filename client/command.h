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

#ifndef wtf_command_h_
#define wtf_command_h_

// e
#include <e/intrusive_ptr.h>

// WTF
#include "common/network_msgtype.h"
#include "common/wtf_node.h"
#include "client/wtf.h"

class wtf_client::command
{
    public:
        command(wtf_returncode* status,
                uint64_t nonce,
                int64_t fd,
                uint64_t block,
                uint64_t offset,
                uint64_t length,
                wtf::wtf_network_msgtype msgtype,
                std::auto_ptr<e::buffer> msg,
                const char** output, size_t* output_sz);
        ~command() throw ();

    public:
        uint64_t nonce() const throw () { return m_nonce; }
        wtf::wtf_network_msgtype msgtype() const throw () { return m_msgtype; }
        const char* output() { return *m_output; }
        size_t output_sz() { return *m_output_sz; }
        uint64_t clientid() const throw () { return m_clientid; }
        int64_t fd() const throw() { return m_fd; }
        int64_t block() const throw() { return m_block; }
        int64_t offset() const throw() { return m_offset; }
        int64_t length() const throw() { return m_length; }
        e::buffer* request() const throw () { return m_request.get(); }
        const wtf::wtf_node& sent_to() const throw () { return m_sent_to; }
        const char* last_error_desc() const throw() { return m_last_error_desc; }
        const char* last_error_file() const throw() { return m_last_error_file; }
        uint64_t last_error_line() const throw() { return m_last_error_line; }
        wtf_returncode status() const throw() { return m_status; }

    public:
        void set_fd(int64_t fd) { m_fd = fd; }
        void set_nonce(uint64_t nonce);
        void set_sent_to(const wtf::wtf_node& sent_to);
        void fail(wtf_returncode status);
        void succeed(std::auto_ptr<e::buffer> msg,
                     const e::slice& resp,
                     wtf_returncode status);
        void set_last_error_desc(const char* desc) throw() { m_last_error_desc = desc; }
        void set_last_error_file(const char* file) throw() { m_last_error_file = file; }
        void set_last_error_line(uint64_t line) throw() { m_last_error_line = line; }

    private:
        friend class e::intrusive_ptr<command>;

    private:
        command(const command&);

    private:
        void inc() { ++m_ref; }
        void dec() { assert(m_ref > 0); if (--m_ref == 0) delete this; }

    private:
        command& operator = (const command&);

    private:
        size_t m_ref;
        uint64_t m_nonce;
        uint64_t m_clientid;
        int64_t m_fd;
        uint64_t m_block;
        uint64_t m_offset;
        uint64_t m_length;
        std::auto_ptr<e::buffer> m_request;
        wtf_returncode m_status;
        const char** m_output;
        size_t* m_output_sz;
        wtf::wtf_node m_sent_to;
        wtf::wtf_network_msgtype m_msgtype;
        const char* m_last_error_desc;
        const char* m_last_error_file;
        uint64_t m_last_error_line;
};

#endif // wtf_command_h_
