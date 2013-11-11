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

#ifndef wtf_daemon_h_
#define wtf_daemon_h_

// STL
#include <queue>
#include <functional>
#include <set>
#include <tr1/memory>
#include <utility>
#include <vector>
#include <map>

// po6
#include <po6/net/hostname.h>
#include <po6/net/ipaddr.h>
#include <po6/pathname.h>
#include <po6/threads/thread.h>

// BusyBee
#include <busybee_mta.h>

// WTF
#include "daemon/coordinator_link.h"
#include "common/mapper.h"
#include "common/wtf_node.h"
#include "common/configuration.h"
#include "daemon/settings.h"
#include "daemon/connection.h"
#include "daemon/block_storage_manager.h"

namespace wtf
{

class daemon
{
    public:
        daemon();
        ~daemon() throw ();

    public:
        int run(bool daemonize,
                po6::pathname data,
                po6::pathname backing_data,
                bool set_bind_to,
                po6::net::location bind_to,
                bool set_coordinator,
                po6::net::hostname coordinator,
                unsigned threads);

    // Handle file operations
    private:
        void loop(size_t thread);
        void process_get(const wtf::connection& conn,
                          uint64_t nonce,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void process_put(const wtf::connection& conn,
                          uint64_t nonce,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);
        void process_update(const wtf::connection& conn,
                          uint64_t nonce,
                          std::auto_ptr<e::buffer> msg,
                          e::unpacker up);


    // Manage communication
    private:
        bool recv(wtf::connection* conn, std::auto_ptr<e::buffer>* msg);
        bool send(const wtf::connection& conn, std::auto_ptr<e::buffer> msg);
        bool send(const wtf_node& node, std::auto_ptr<e::buffer> msg);
        bool send_no_disruption(uint64_t token, std::auto_ptr<e::buffer> msg);

    // Handle communication disruptions
    private:
        void handle_disruption(uint64_t token);
        void periodic_handle_disruption(uint64_t now);
        void periodic_stat(uint64_t token);
        void handle_disruption_reset_healing(uint64_t token);
        void handle_disruption_reset_reconfiguration(uint64_t token);

    // Periodically run certain functions
    private:
        typedef void (daemon::*periodic_fptr)(uint64_t now);
        typedef std::pair<uint64_t, periodic_fptr> periodic;
        void trip_periodic(uint64_t when, periodic_fptr fp);
        void run_periodic();
        void periodic_nop(uint64_t now);

    // Utilities
    private:
        bool generate_token(uint64_t* token);

    private:
        friend class coordinator_link;

    private:
        settings m_s;
        wtf::mapper m_busybee_mapper;
        std::auto_ptr<busybee_mta> m_busybee;
        wtf_node m_us;
        std::vector<std::tr1::shared_ptr<po6::threads::thread> > m_threads;
        coordinator_link m_coord;
        wtf::block_storage_manager m_blockman;
        std::vector<periodic> m_periodic;
        std::map<uint64_t, uint64_t> m_temporary_servers;
        std::set<uint64_t> m_disrupted_backoff;
        bool m_disrupted_retry_scheduled;
        configuration m_config;
};

} // namespace wtf

#endif // wtf_daemon_h_
