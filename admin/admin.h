// Copyright (c) 2013, Cornell University
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

#ifndef wtf_admin_admin_h_
#define wtf_admin_admin_h_

// STL
#include <map>
#include <list>

// BusyBee
#include <busybee_st.h>

// WTF
#include "include/wtf/admin.h"
#include "common/coordinator_link.h"
#include "common/mapper.h"
#include "admin/coord_rpc.h"
#include "admin/multi_yieldable.h"
#include "admin/pending.h"

namespace wtf
{
class admin
{
    public:
        admin(const char* coordinator, uint16_t port);
        ~admin() throw ();

    public:
        // introspect the config
        int64_t dump_config(enum wtf_admin_returncode* status,
                            const char** config);
        // cluster
        int64_t read_only(int ro,
                          enum wtf_admin_returncode* status);
        // manage servers
        int64_t server_register(uint64_t token, const char* address,
                                enum wtf_admin_returncode* status);
        int64_t server_online(uint64_t token, enum wtf_admin_returncode* status);
        int64_t server_offline(uint64_t token, enum wtf_admin_returncode* status);
        int64_t server_forget(uint64_t token, enum wtf_admin_returncode* status);
        int64_t server_kill(uint64_t token, enum wtf_admin_returncode* status);
        // looping/polling
        int64_t loop(int timeout, wtf_admin_returncode* status);
        // error handling
        const char* error_message();
        const char* error_location();
        void set_error_message(const char* msg);
        // translate returncodes
        void interpret_rpc_request_failure(replicant_returncode rstatus,
                                           wtf_admin_returncode* status);
        void interpret_rpc_loop_failure(replicant_returncode rstatus,
                                        wtf_admin_returncode* status);
        void interpret_rpc_response_failure(replicant_returncode rstatus,
                                            wtf_admin_returncode* status,
                                            e::error* err);

    private:
        struct pending_server_pair
        {
            pending_server_pair()
                : si(), op() {}
            pending_server_pair(const server_id& s,
                                const e::intrusive_ptr<pending>& o)
                : si(s), op(o) {}
            ~pending_server_pair() throw () {}
            server_id si;
            e::intrusive_ptr<pending> op;
        };
        typedef std::map<uint64_t, pending_server_pair> pending_map_t;
        typedef std::map<int64_t, e::intrusive_ptr<coord_rpc> > coord_rpc_map_t;
        typedef std::map<int64_t, e::intrusive_ptr<multi_yieldable> > multi_yieldable_map_t;
        typedef std::list<pending_server_pair> pending_queue_t;

    private:
        bool maintain_coord_connection(wtf_admin_returncode* status);
        bool send(wtf_network_msgtype mt,
                  server_id id,
                  uint64_t nonce,
                  std::auto_ptr<e::buffer> msg,
                  e::intrusive_ptr<pending> op,
                  wtf_admin_returncode* status);
        void handle_disruption(const server_id& si);

    private:
        coordinator_link m_coord;
        mapper m_busybee_mapper;
        busybee_st m_busybee;
        int64_t m_next_admin_id;
        uint64_t m_next_server_nonce;
        bool m_handle_coord_ops;
        coord_rpc_map_t m_coord_ops;
        pending_map_t m_server_ops;
        multi_yieldable_map_t m_multi_ops;
        pending_queue_t m_failed;
        std::list<e::intrusive_ptr<yieldable> > m_yieldable;
        e::intrusive_ptr<yieldable> m_yielding;
        e::intrusive_ptr<yieldable> m_yielded;
        e::error m_last_error;
};

}
#endif // wtf_admin_admin_h_
