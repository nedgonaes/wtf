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

#ifndef wtf_client_pending_open_h_
#define wtf_client_pending_open_h_

// STL
#include <map>
#include <vector>

// WTF
#include "client/pending_aggregation.h"
#include "client/file.h"

namespace wtf __attribute__ ((visibility("hidden")))
{
class pending_open : public pending_aggregation
{
    public:
        pending_open(client* cl, uint64_t client_visible_id,
                          wtf_client_returncode* status,
                          e::intrusive_ptr<file> f, int64_t* fd); 
        virtual ~pending_open() throw ();

    // return to client
    public:
        virtual bool can_yield();
        virtual bool yield(wtf_client_returncode* status, e::error* error);

    // events
    public:
        virtual bool handle_hyperdex_message(client*,
                                    int64_t reqid,
                                    hyperdex_client_returncode rc,
                                    wtf_client_returncode* status,
                                    e::error* error);
        virtual bool try_op();

    friend class e::intrusive_ptr<pending_aggregation>;

    // noncopyable
    private:
        pending_open(const pending_open& other);
        pending_open& operator = (const pending_open& rhs);

    private:
        bool send_put(std::string& dst, const hyperdex_client_attribute* attrs, size_t attrs_sz);

    private:
        client* m_cl;
        e::intrusive_ptr<file> m_file;
        int64_t* m_fd;
        bool m_done;
};

}

#endif // wtf_client_pending_open_h_
