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

#ifndef wtf_coordinator_coordinator_h_
#define wtf_coordinator_coordinator_h_

// STL
#include <map>
#include <tr1/memory>

// po6
#include <po6/net/location.h>

// Replicant
#include <replicant_state_machine.h>

// WTF 
#include "common/ids.h"
#include "common/server.h"
#include "coordinator/server_barrier.h"

namespace wtf __attribute__ ((visibility("hidden")))
{
class coordinator
{
    public:
        coordinator();
        ~coordinator() throw ();

    // identity
    public:
        void init(replicant_state_machine_context* ctx, uint64_t token);
        uint64_t cluster() const { return m_cluster; }

    // cluster management
    public:
        void read_only(replicant_state_machine_context* ctx, bool ro);

    // server management
    public:
        void server_register(replicant_state_machine_context* ctx,
                             const server_id& sid,
                             const po6::net::location& bind_to);
        void server_online(replicant_state_machine_context* ctx,
                           const server_id& sid,
                           const po6::net::location* bind_to);
        void server_offline(replicant_state_machine_context* ctx,
                            const server_id& sid);
        void server_shutdown(replicant_state_machine_context* ctx,
                             const server_id& sid);
        void server_kill(replicant_state_machine_context* ctx,
                         const server_id& sid);
        void server_forget(replicant_state_machine_context* ctx,
                           const server_id& sid);
        void server_suspect(replicant_state_machine_context* ctx,
                            const server_id& sid);
        void report_disconnect(replicant_state_machine_context* ctx,
                               const server_id& sid, uint64_t version);

    // config management
    public:
        void config_get(replicant_state_machine_context* ctx);
        void config_ack(replicant_state_machine_context* ctx,
                        const server_id& sid, uint64_t version);
        void config_stable(replicant_state_machine_context* ctx,
                           const server_id& sid, uint64_t version);

    // checkpoint management
    public:
        void checkpoint(replicant_state_machine_context* ctx);
        void checkpoint_stable(replicant_state_machine_context* ctx,
                               const server_id& sid,
                               uint64_t config,
                               uint64_t number);

    // alarm
    public:
        void alarm(replicant_state_machine_context* ctx);

    // debug
    public:
        void debug_dump(replicant_state_machine_context* ctx);

    // backup/restore
    public:
        static coordinator* recreate(replicant_state_machine_context* ctx,
                                     const char* data, size_t data_sz);
        void snapshot(replicant_state_machine_context* ctx,
                      const char** data, size_t* data_sz);

    // utilities
    private:
        // servers
        server* new_server(const server_id& sid);
        server* get_server(const server_id& sid);
        void remove_offline(const server_id& sid);
        // configuration
        void check_ack_condition(replicant_state_machine_context* ctx);
        void check_stable_condition(replicant_state_machine_context* ctx);
        void generate_next_configuration(replicant_state_machine_context* ctx);
        void generate_cached_configuration(replicant_state_machine_context* ctx);
        void servers_in_configuration(std::vector<server_id>* sids);
        // checkpoints
        void check_checkpoint_stable_condition(replicant_state_machine_context* ctx);

    private:
        // meta state
        uint64_t m_cluster;
        uint64_t m_counter;
        uint64_t m_version;
        uint64_t m_flags;
        // servers
        std::vector<server> m_servers;
        std::vector<server> m_offline;
        // barriers
        uint64_t m_config_ack_through;
        server_barrier m_config_ack_barrier;
        uint64_t m_config_stable_through;
        server_barrier m_config_stable_barrier;
        // checkpoints
        uint64_t m_checkpoint;
        uint64_t m_checkpoint_stable_through;
        uint64_t m_checkpoint_gc_through;
        server_barrier m_checkpoint_stable_barrier;
        // cached config
        std::auto_ptr<e::buffer> m_latest_config;

    private:
        coordinator(const coordinator&);
        coordinator& operator = (const coordinator&);
};

}
#endif // wtf_coordinator_coordinator_h_
