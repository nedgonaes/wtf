// Copyright (c) 2012-2013, Cornell University
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

#ifndef wtf_client_pending_read_h_
#define wtf_client_pending_read_h_

// STL
#include <map>

// WTF
#include "client/pending_aggregation.h"
#include "client/file.h"
#include "client/client.h"

namespace wtf __attribute__ ((visibility("hidden")))
{
class pending_read : public pending_aggregation
{
    public:
        pending_read(client* cl, uint64_t id, e::intrusive_ptr<file> f,
                               char* buf, size_t* buf_sz, 
                               wtf_client_returncode* status);
        virtual ~pending_read() throw ();

    // return to client
    public:
        virtual bool can_yield();
        virtual bool yield(wtf_client_returncode* status, e::error* error);

    friend class file;
    friend class rereplicate;

    // events
    public:
        virtual void handle_wtf_failure(const server_id& si);
        virtual bool handle_wtf_message(client*,
                                    const server_id& si,
                                    std::auto_ptr<e::buffer> msg,
                                    e::unpacker up,
                                    wtf_client_returncode* status,
                                    e::error* error);
        virtual bool handle_hyperdex_message(client*,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* error);
        virtual bool try_op();

    public:
        void set_offset(const uint64_t si, const uint64_t bi, const size_t buf_offset,
                   const size_t block_offset, const size_t len);

    // noncopyable
    private:
        pending_read(const pending_read& other);
        pending_read& operator = (const pending_read& rhs);

    private:
        struct buffer_block_len 
        {
            buffer_block_len()
                : buf_offset(), block_offset(), len() {}
            buffer_block_len(const size_t bu,
                                const size_t bl,
                                const size_t l)
                : buf_offset(bu), block_offset(bl), len(l) {}
            ~buffer_block_len() throw () {}
            size_t buf_offset;   //offset in client buffer
            size_t block_offset; //offset from start of block
            size_t len;         //how many bytes to copy
        };

        typedef std::map<std::pair<uint64_t, uint64_t>, struct buffer_block_len> offset_map_t;

    private:
        client* m_cl;
        char* m_buf;
        size_t* m_buf_sz;
        size_t m_max_buf_sz;
        offset_map_t m_offset_map;
        e::intrusive_ptr<file> m_file;
        std::string m_path;
        bool m_done;
        int m_state;
};

}

#endif // wtf_client_pending_read_h_
