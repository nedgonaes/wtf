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

#define __STDC_LIMIT_MACROS

//busybee
#include <busybee_constants.h>

// busybee header + nonce + msgtype
#define COMMAND_MSGTYPE_OFFSET (BUSYBEE_HEADER_SIZE)
#define COMMAND_NONCE_OFFSET (BUSYBEE_HEADER_SIZE + pack_size(wtf::WTFNET_PUT))
#define COMMAND_DATA_OFFSET (COMMAND_NONCE_OFFSET + sizeof(uint64_t))
#define MIN(X,Y) ((X) < (Y) ? (X) : (Y))


// e
#include <e/endian.h>

// WTF
#include "client/command.h"
#include "common/network_msgtype.h"
#include "common/block_id.h"

using wtf::wtf_node;

wtf_client :: command :: command(wtf::wtf_node send_to,
                                 uint64_t remote_bid,
                                 int64_t fd,
                                 uint64_t block,
                                 uint64_t offset,
                                 uint64_t version,
                                 const char* data,
                                 uint64_t length,
                                 uint64_t& nonce,
                                 wtf::wtf_network_msgtype msgtype,
                                 char* output,
                                 size_t output_sz)
    : m_ref(0)
    , m_nonce(nonce++)
    , m_sent_to(send_to)
    , m_remote_bid(remote_bid)
    , m_fd(fd)
    , m_block(block)
    , m_offset(offset)
    , m_length(length)
    , m_version(version)
    , m_request()
    , m_status(WTF_GARBAGE)
    , m_output(output)
    , m_output_sz(output_sz)
    , m_msgtype(msgtype)
    , m_last_error_desc()
    , m_last_error_file()
    , m_last_error_line()
{
    m_request = std::auto_ptr<e::buffer>(e::buffer::create(req_size()));
    e::buffer::packer pa = m_request->pack_at(COMMAND_MSGTYPE_OFFSET);
    pa = pa << m_msgtype << m_nonce;

    switch(m_msgtype)
    {
        case wtf::WTFNET_PUT:
            pa = pa.copy(e::slice(data, length));
            m_output = new char[sizeof(uint64_t)*2];
            m_output_sz = sizeof(uint64_t)*2;
            break;
        case wtf::WTFNET_UPDATE:
            pa = pa << m_sent_to.token << remote_bid << m_length;
            pa = pa.copy(e::slice(data, length));
            m_output = new char[sizeof(uint64_t)*2];
            m_output_sz = sizeof(uint64_t)*2;
            break;
        case wtf::WTFNET_GET:
            pa = pa << m_sent_to.token << remote_bid << uint64_t(m_output_sz);
            break;
        default:
            break;
    }
}

wtf_client :: command :: ~command() throw ()
{
    //XXX:  Make this into separate classes.
    switch(m_msgtype)
    {
        /* Put and update must clean up their own
           output, which is created in constructor */
        case wtf::WTFNET_PUT:
        case wtf::WTFNET_UPDATE:
            delete [] m_output;
            m_output = NULL;
            break;
        /* Get output is created by the user, they clean
           it up */
        case wtf::WTFNET_GET:
        default:
            break;
    }

    return;
}

void
wtf_client :: command :: set_sent_to(const wtf_node& s)
{
    m_sent_to = s;
}

size_t
wtf_client :: command :: req_size()
{
    switch (m_msgtype)
    {
        case wtf::WTFNET_PUT:
            return COMMAND_DATA_OFFSET + m_length;
        case wtf::WTFNET_GET:
            return COMMAND_DATA_OFFSET + wtf::block_id::pack_size() + sizeof(uint64_t);
        case wtf::WTFNET_UPDATE:
            return COMMAND_DATA_OFFSET + wtf::block_id::pack_size() + sizeof(uint64_t) + m_length;
        default:
            return -1;
    };
}

void
wtf_client :: command :: fail(wtf_returncode status)
{
    m_status = status;
}

void
wtf_client :: command :: succeed(std::auto_ptr<e::buffer> backing,
                                       const e::slice& resp,
                                       wtf_returncode status)
{
    char* base = reinterpret_cast<char*>(backing.get());
    const char* data = reinterpret_cast<const char*>(resp.data());
    std::cout << "received " << resp.hex() << std::endl;
    assert(data >= base);
    assert(data - base < UINT16_MAX);
    uint16_t diff = data - base;
    assert(diff >= 2);
    e::pack16le(diff, base + diff - 2);
    m_output_sz = MIN(m_output_sz, resp.size());
    memmove(m_output, data, m_output_sz);
    std::cout << "m_output = " << (void*)m_output << std::endl;
    backing.release();
    m_status = status;
}
