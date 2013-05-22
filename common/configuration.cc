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

using wtf::wtf_node;
using wtf::configuration;

configuration :: configuration()
    : m_cluster()
    , m_prev_token()
    , m_this_token()
    , m_version()
    , m_members()
{
}

configuration :: configuration(uint64_t c,
                               uint64_t pt,
                               uint64_t tt,
                               uint64_t v)
    : m_cluster(c)
    , m_prev_token(pt)
    , m_this_token(tt)
    , m_version(v)
    , m_members()
{
}

configuration :: ~configuration() throw ()
{
}

bool
configuration :: validate() const
{
    for (size_t i = 0; i < m_members.size(); ++i)
    {
        for (size_t j = i + 1; j < m_members.size(); ++j)
        {
            if (m_members[i].token == m_members[j].token ||
                m_members[i].address == m_members[j].address)
            {
                return false;
            }
        }
    }

    return true;
}

bool
configuration :: has_token(uint64_t token) const
{
    return node_from_token(token) != NULL;
}

bool
configuration :: is_member(const wtf_node& node) const
{
    const wtf_node* n = node_from_token(node.token);
    return n && *n == node;
}

const wtf_node*
configuration :: node_from_token(uint64_t token) const
{
    for (size_t i = 0; i < m_members.size(); ++i)
    {
        if (m_members[i].token == token)
        {
            return &m_members[i];
        }
    }

    return NULL;
}

const wtf_node*
configuration :: members_begin() const
{
    return &m_members.front();
}

const wtf_node*
configuration :: members_end() const
{
    return &m_members.front() + m_members.size();
}

void
configuration :: bump_version()
{
    m_prev_token = m_this_token;
    // XXX m_this_token = token
    ++m_version;
}

void
configuration :: add_member(const wtf_node& node)
{
    assert(node_from_token(node.token) == NULL);
    m_members.push_back(node);
    std::sort(m_members.begin(), m_members.end());
}

bool
wtf :: operator == (const configuration& lhs, const configuration& rhs)
{
    if (lhs.m_cluster != rhs.m_cluster ||
        lhs.m_prev_token != rhs.m_prev_token ||
        lhs.m_this_token != rhs.m_this_token ||
        lhs.m_version != rhs.m_version ||
        lhs.m_members.size() != rhs.m_members.size())
    {
        return false;
    }

    for (size_t i = 0; i < lhs.m_members.size(); ++i)
    {
        if (lhs.m_members[i] != rhs.m_members[i])
        {
            return false;
        }
    }

    return true;
}

std::ostream&
wtf :: operator << (std::ostream& lhs, const configuration& rhs)
{
    lhs << "configuration(cluster=" << rhs.m_cluster
        << ", prev_token=" << rhs.m_prev_token
        << ", this_token=" << rhs.m_this_token
        << ", version=" << rhs.m_version
        << ", members=[";

    for (size_t i = 0; i < rhs.m_members.size(); ++i)
    {
        lhs << rhs.m_members[i] << (i + 1 < rhs.m_members.size() ? ", " : "");
    }

    lhs << "])";
    return lhs;
}

e::buffer::packer
wtf :: operator << (e::buffer::packer lhs, const configuration& rhs)
{
    lhs = lhs << rhs.m_cluster
              << rhs.m_prev_token
              << rhs.m_this_token
              << rhs.m_version
              << uint64_t(rhs.m_members.size());

    for (uint64_t i = 0; i < rhs.m_members.size(); ++i)
    {
        lhs = lhs << rhs.m_members[i];
    }

    return lhs;
}

e::unpacker
wtf :: operator >> (e::unpacker lhs, configuration& rhs)
{
    uint64_t members_sz;
    uint64_t chain_sz;
    lhs = lhs >> rhs.m_cluster
              >> rhs.m_prev_token
              >> rhs.m_this_token
              >> rhs.m_version
              >> members_sz;
    rhs.m_members.resize(members_sz);

    for (uint64_t i = 0; i < rhs.m_members.size(); ++i)
    {
        lhs = lhs >> rhs.m_members[i];
    }

    return lhs;
}


char*
wtf :: pack_config(const configuration& config, char* ptr)
{
    // XXX inefficient, lazy hack
    std::auto_ptr<e::buffer> msg(e::buffer::create(pack_size(config)));
    msg->pack_at(0) << config;
    memmove(ptr, msg->data(), msg->size());
    return ptr + msg->size();
}

size_t
wtf :: pack_size(const configuration& rhs)
{
    size_t sz = sizeof(uint64_t) // rhs.m_cluster
              + sizeof(uint64_t) // rhs.m_prev_token
              + sizeof(uint64_t) // rhs.m_this_token
              + sizeof(uint64_t) // rhs.m_version
              + sizeof(uint64_t); // rhs.m_members.size()

    for (size_t i = 0; i < rhs.m_members.size(); ++i)
    {
        sz += pack_size(rhs.m_members[i]);
    }

    return sz;
}

size_t
wtf :: pack_size(const std::vector<configuration>& rhs)
{
    size_t sz = sizeof(uint32_t);

    for (size_t i = 0; i < rhs.size(); ++i)
    {
        sz += pack_size(rhs[i]);
    }

    return sz;
}
