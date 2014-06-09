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

#ifndef wtf_client_message_hyperdex_search_h_
#define wtf_client_message_hyperdex_search_h_

#include "client/message_hyperdex_search.h"

using wtf::message_hyperdex_search;

message_hyperdex_search :: message_hyperdex_search(wtf_client* cl,
                                             const char* space,
                                             const char* name,
                                             const char* regex)
    : message(cl, OPCODE_HYPERDEX_GET, cl->m_hyperdex_client->poll_fd()) 
    , m_space(space)
    , m_key(name)
    , m_regex(regex)
    , m_status(HYPERDEX_CLIENT_GARBAGE)
    , m_attrs(NULL)
    , m_attrs_size(0)
    , m_reqid(0)
{
}

int64_t
message_hyperdex_search :: send()
{
    struct hyperdex_client_attribute_check check;

    check.attr = m_name.c_str();
    check.value = m_regex.c_str();
    check.value_sz = m_regex.size();
    check.datatype = HYPERDATATYPE_STRING;
    check.predicate = HYPERPREDICATE_REGEX;

    hyperdex::Client* hc = m_cl->m_hyperdex_client;

    //XXX: begin transaction here
    int64_t reqid = hc->search("wtf", 
                               &check, 
                               1, 
                               &m_status, 
                               &m_attrs,
                               &m_attrs_sz);

    if (ret < 0)
    {
        PENDING_ERROR(IO) << "Couldn't delete from HyperDex";
        return false;
    }

    return m_reqid;
}
