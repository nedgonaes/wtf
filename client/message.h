// Copyright (c) 2011-2013, Cornell University
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

#ifndef wtf_client_message_h_
#define wtf_client_message_h_

// e
#include <e/intrusive_ptr.h>

//WTF  
#include "common/ids.h"
#include "client/client.h"

namespace wtf __attribute__ ((visibility("hidden")))
{
class client;

enum wtf_opcode
{
    OPCODE_HYPERDEX_PUT,
    OPCODE_HYPERDEX_GET,
    OPCODE_HYPERDEX_DEL,
    OPCODE_HYPERDEX_SEARCH,
    OPCODE_WTF_UPDATE,
    OPCODE_WTF_GET
};

class message
{
    public:
        message(client* cl,
                wtf_opcode oc)
            : m_oc(oc) 
            , m_cl(cl)
            , m_reqid(0) {};
        virtual ~message() throw ();
        wtf_opcode opcode() { return m_oc; }
        int64_t reqid() { return m_reqid; }

    // refcount
    protected:
        friend class e::intrusive_ptr<message>;
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }
        size_t m_ref;

    // noncopyable
    private:
        message(const message& other);
        message& operator = (const message& rhs);

    // operation state
    protected:
        client* m_cl;
        wtf_opcode m_oc;
        int64_t m_reqid;
};

}
#endif // wtf_client_message_h_
