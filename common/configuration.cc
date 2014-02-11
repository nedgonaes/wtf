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
//     * Neither the name of WTFt nor the names of its contributors may be
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

// STL
#include <algorithm>
#include <memory>

// Google Log
#include <glog/logging.h>

// WTF
#include "common/configuration.h"
#include "common/block_location.h"
#include "common/server.h"

using wtf::server;
using wtf::configuration;

configuration :: configuration()
    : m_cluster()
    , m_version()
    , m_flags()
    , m_servers()
{
}

configuration :: configuration(const configuration& other)
    : m_cluster(other.m_cluster)
    , m_version(other.m_version)
    , m_flags(other.m_flags)
    , m_servers(other.m_servers)
{
}

configuration :: ~configuration() throw ()
{
}

bool
configuration :: validate() const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        for (size_t j = i + 1; j < m_servers.size(); ++j)
        {
            if (m_servers[i].id== m_servers[j].id ||
                m_servers[i].bind_to == m_servers[j].bind_to)
            {
                return false;
            }
        }
    }

    return true;
}

bool
configuration :: exists(const server_id& id) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == id)
        {
            return true;
        }
    }

    return false;
}

const server*
configuration :: server_from_id(server_id id) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == id)
        {
            return &m_servers[i];
        }
    }

    return NULL;
}

po6::net::location
configuration :: get_address(const server_id& id) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == id)
        {
            return m_servers[i].bind_to;
        }
    }

    return po6::net::location();
}

server::state_t
configuration :: get_state(const server_id& id) const
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == id)
        {
            return m_servers[i].state;
        }
    }

    return server::KILLED;
}

const server*
configuration :: servers_begin() const
{
    return &m_servers.front();
}

const server*
configuration :: servers_end() const
{
    return &m_servers.front() + m_servers.size();
}

void
configuration :: bump_version()
{
    ++m_version;
}

void
configuration :: add_server(const server& node)
{
    assert(!exists(node.id));
    m_servers.push_back(node);
    std::sort(m_servers.begin(), m_servers.end());
}

const server*
configuration :: get_random_server(uint64_t sid) const
{
    uint32_t id = sid % m_servers.size();
    return &m_servers[id];
}

void 
configuration :: assign_random_block_locations(std::vector<block_location>& bl) const
{
    static size_t last_server = 0;
    for (size_t i = 0; i < bl.size(); ++i)
    {
        /* if this server could be any server */
        if (bl[i] == block_location())
        {
            /* find the next available server and assign it to the block location. */
            for (size_t j = 0; j < m_servers.size(); ++j)
            {
                server s = m_servers[last_server++ % m_servers.size()];
                if (s.state == server::AVAILABLE)
                {
                    bl[i].si = s.id.get();
                }
            }
        }
    }
}

bool
wtf :: operator == (const configuration& lhs, const configuration& rhs)
{
    if (lhs.m_cluster != rhs.m_cluster ||
        lhs.m_version != rhs.m_version ||
	lhs.m_flags != rhs.m_flags ||
        lhs.m_servers.size() != rhs.m_servers.size())
    {
        return false;
    }

    for (size_t i = 0; i < lhs.m_servers.size(); ++i)
    {
        if (lhs.m_servers[i].id != rhs.m_servers[i].id ||
            lhs.m_servers[i].bind_to != rhs.m_servers[i].bind_to ||
            lhs.m_servers[i].state != rhs.m_servers[i].state)
        {
            return false;
        }
    }

    return true;
}

e::unpacker
wtf :: operator >> (e::unpacker up, configuration& c)
{
    uint64_t num_servers;
    up = up >> c.m_cluster >> c.m_version >> c.m_flags
            >> num_servers;
    c.m_servers.clear();
    c.m_servers.reserve(num_servers);

    for (size_t i = 0; !up.error() && i < num_servers; ++i)
    {
        server s;
        up = up >> s;
        c.m_servers.push_back(s);
    }

    return up;
}

std::string
wtf :: configuration :: dump() const
{
    std::ostringstream out;
    out << "cluster " << m_cluster << "\n";
    out << "version " << m_version << "\n";
    out << "flags " << std::hex << m_flags << std::dec << "\n";

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        out << "server "
            << m_servers[i].id.get() << " "
            << m_servers[i].bind_to << " "
            << server::to_string(m_servers[i].state) << "\n";
    }

    return out.str();
}
