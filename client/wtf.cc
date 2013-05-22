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

// e
#include <e/endian.h>
#include <e/time.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_st.h>

// WTF
#include "common/configuration.h"
#include "common/macros.h"
#include "common/mapper.h"
#include "common/network_msgtype.h"
#include "common/response_returncode.h"
#include "common/special_objects.h"
#include "client/command.h"
#include "client/wtf.h"

using namespace wtf;

#define WTFSETERROR(CODE, DESC) \
    do \
    { \
        m_last_error_desc = DESC; \
        m_last_error_file = __FILE__; \
        m_last_error_line = __LINE__; \
        *status = CODE; \
    } while (0)

#define WTFSETSUCCESS WTFSETERROR(WTF_SUCCESS, "operation succeeded")

#define COMMAND_HEADER_SIZE (BUSYBEE_HEADER_SIZE + pack_size(WTFNET_PUT) + 4 * sizeof(uint64_t))

#define BUSYBEE_ERROR(REPRC, BBRC) \
    case BUSYBEE_ ## BBRC: \
        WTFSETERROR(WTF_ ## REPRC, "BusyBee returned " xstr(BBRC)); \
        return -1

#define BUSYBEE_ERROR_DISCONNECT(REPRC, BBRC) \
    case BUSYBEE_ ## BBRC: \
        WTFSETERROR(WTF_ ## REPRC, "BusyBee returned " xstr(BBRC)); \
        reset_to_disconnected(); \
        return -1

#define BUSYBEE_ERROR_CONTINUE(REPRC, BBRC) \
    case BUSYBEE_ ## BBRC: \
        WTFSETERROR(WTF_ ## REPRC, "BusyBee returned " xstr(BBRC)); \
        continue

#define WTF_UNEXPECTED(MT) \
    case WTFNET_ ## MT: \
        WTFSETERROR(WTF_SERVERERROR, "unexpected " xstr(MT) " message"); \
        m_last_error_host = from; \
        return -1

#define WTF_UNEXPECTED_DISCONNECT(MT) \
    case WTFNET_ ## MT: \
        WTFSETERROR(WTF_SERVERERROR, "unexpected " xstr(MT) " message"); \
        m_last_error_host = from; \
        reset_to_disconnected(); \
        return -1

void
wtf_destroy_output(const char* output, size_t)
{
    uint16_t sz = 0;
    e::unpack16le(output - 2, &sz);
    const e::buffer* buf = reinterpret_cast<const e::buffer*>(output - sz);
    delete buf;
}

wtf_client :: wtf_client(const char* host, in_port_t port)
    : m_busybee_mapper(new wtf::mapper())
    , m_busybee(new busybee_st(m_busybee_mapper.get(), 0))
    , m_config(new wtf::configuration())
    , m_token(0x4141414141414141ULL)
    , m_nonce(1)
    , m_commands()
    , m_complete()
    , m_resend()
    , m_last_error_desc("")
    , m_last_error_file(__FILE__)
    , m_last_error_line(__LINE__)
    , m_last_error_host()
{
}

wtf_client :: ~wtf_client() throw ()
{
}

int64_t
wtf_client :: send(const char* host, in_port_t port, uint64_t token,
                   wtf_network_msgtype msgtype,
                   const char* data, size_t data_sz,
                   wtf_returncode* status,
                   const char** output, size_t* output_sz)
{

    // Pack the message to send
    uint64_t nonce = m_nonce;
    ++m_nonce;
    size_t sz = COMMAND_HEADER_SIZE + data_sz;
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    e::buffer::packer pa = msg->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << msgtype << m_token << nonce;
    pa = pa.copy(e::slice(data, data_sz));

    // Create the command object
    e::intrusive_ptr<command> cmd = new command(status, nonce, msg, output, output_sz);
    cmd->set_sent_to(chain_node(token, po6::net::location(host, port)));
    return send_to_preferred_chain_position(cmd, status);
}

int64_t
wtf_client :: loop(int timeout, wtf_returncode* status)
{
    while ((!m_commands.empty() || !m_resend.empty())
           && m_complete.empty())
    {
        // Always set timeout
        m_busybee->set_timeout(timeout);
        int64_t ret = inner_loop(status);

        if (ret < 0)
        {
            return ret;
        }

        assert(ret == 0);
    }

    if (!m_complete.empty())
    {
        e::intrusive_ptr<command> c = m_complete.begin()->second;
        m_complete.erase(m_complete.begin());
        m_last_error_desc = c->last_error_desc();
        m_last_error_file = c->last_error_file();
        m_last_error_line = c->last_error_line();
        m_last_error_host = c->sent_to().address;
        return c->clientid();
    }

    if (m_commands.empty())
    {
        WTFSETERROR(WTF_NONEPENDING, "no outstanding operations to process");
        return -1;
    }

    WTFSETERROR(WTF_INTERNAL, "unhandled exit case from loop");
    return -1;
}

int64_t
wtf_client :: loop(int64_t id, int timeout, wtf_returncode* status)
{
    while (m_commands.find(id) != m_commands.end() ||
           m_resend.find(id) != m_resend.end())
    {
        // Always set timeout
        m_busybee->set_timeout(timeout);
        int64_t ret = inner_loop(status);

        if (ret < 0)
        {
            return ret;
        }

        assert(ret == 0);
    }

    command_map::iterator it = m_complete.find(id);

    if (it == m_complete.end())
    {
        WTFSETERROR(WTF_NONEPENDING, "no outstanding operation with the specified id");
        return -1;
    }

    e::intrusive_ptr<command> c = it->second;
    m_complete.erase(it);
    m_last_error_desc = c->last_error_desc();
    m_last_error_file = c->last_error_file();
    m_last_error_line = c->last_error_line();
    m_last_error_host = c->sent_to().address;
    return c->clientid();
}

void
wtf_client :: kill(int64_t id)
{
    m_commands.erase(id);
    m_complete.erase(id);
    m_resend.erase(id);
}

#ifdef _MSC_VER
fd_set*
#else
int
#endif
wtf_client :: poll_fd()
{
    return m_busybee->poll_fd();
}

int64_t
wtf_client :: inner_loop(wtf_returncode* status)
{

    int64_t ret = -1;
    // Resend all those that need it
    while (!m_resend.empty())
    {
        ret = send_to_preferred_chain_position(m_resend.begin()->second, status);

        // As this is a retransmission, we only care about errors (< 0)
        // not the success half (>=0).
        if (ret < 0)
        {
            return ret;
        }

        m_resend.erase(m_resend.begin());
    }

    // Receive a message
    uint64_t id;
    std::auto_ptr<e::buffer> msg;
    busybee_returncode rc = m_busybee->recv(&id, &msg);
    const chain_node* node = m_config->node_from_token(id);

    // And process it
    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            break;
        case BUSYBEE_DISRUPTED:
            if (node)
            {
                handle_disruption(*node, status);
            }

            return 0;
        case BUSYBEE_INTERRUPTED:
            WTFSETERROR(WTF_INTERRUPTED, "signal received");
            return -1;
        case BUSYBEE_TIMEOUT:
            WTFSETERROR(WTF_TIMEOUT, "operation timed out");
            return -1;
        BUSYBEE_ERROR(INTERNAL, SHUTDOWN);
        BUSYBEE_ERROR(INTERNAL, POLLFAILED);
        BUSYBEE_ERROR(INTERNAL, ADDFDFAIL);
        BUSYBEE_ERROR(INTERNAL, EXTERNAL);
        default:
            WTFSETERROR(WTF_INTERNAL, "BusyBee returned unknown error");
            return -1;
    }

    if (!node)
    {
        m_busybee->drop(id);
        return 0;
    }

    po6::net::location from = node->address;
    e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    wtf_network_msgtype mt;
    up = up >> mt;

    if (up.error())
    {
        WTFSETERROR(WTF_SERVERERROR, "unpack failed");
        m_last_error_host = from;
        return -1;
    }

    switch (mt)
    {
        case WTFNET_COMMAND_RESPONSE:
            if ((ret = handle_command_response(from, msg, up, status)) < 0)
            {
                return ret;
            }
            break;
        WTF_UNEXPECTED(NOP);
        default:
            WTFSETERROR(WTF_SERVERERROR, "invalid message type");
            m_last_error_host = from;
            return -1;
    }

    return 0;
}

int64_t
wtf_client :: send_to_preferred_chain_position(e::intrusive_ptr<command> cmd,
                                               wtf_returncode* status)
{
    bool sent = false;

    std::auto_ptr<e::buffer> msg(cmd->request()->copy());
    chain_node send_to = cmd->sent_to();
    m_busybee_mapper->set(send_to);
    busybee_returncode rc = m_busybee->send(send_to.token, msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            sent = true;
            break;
        case BUSYBEE_DISRUPTED:
            handle_disruption(send_to, status);
            WTFSETERROR(WTF_BACKOFF, "backoff before retrying");
            BUSYBEE_ERROR(INTERNAL, SHUTDOWN);
            BUSYBEE_ERROR(INTERNAL, POLLFAILED);
            BUSYBEE_ERROR(INTERNAL, ADDFDFAIL);
            BUSYBEE_ERROR(INTERNAL, TIMEOUT);
            BUSYBEE_ERROR(INTERNAL, EXTERNAL);
            BUSYBEE_ERROR(INTERNAL, INTERRUPTED);
        default:
            WTFSETERROR(WTF_INTERNAL, "BusyBee returned unknown error");
    }

    if (sent)
    {
        m_commands[cmd->nonce()] = cmd;
        return cmd->clientid();
    }
    else
    {
        // We have an error captured by REPLSETERROR above.
        return -1;
    }
}

int64_t
wtf_client :: handle_command_response(const po6::net::location& from,
                                            std::auto_ptr<e::buffer> msg,
                                            e::unpacker up,
                                            wtf_returncode* status)
{
    // Parse the command response
    uint64_t nonce;
    wtf::response_returncode rc;
    up = up >> nonce >> rc;

    if (up.error())
    {
        WTFSETERROR(WTF_SERVERERROR, "unpack failed");
        m_last_error_host = from;
        return -1;
    }

    // Find the command
    command_map::iterator it = m_commands.find(nonce);
    command_map* map = &m_commands;

    if (it == map->end())
    {
        it = m_resend.find(nonce);
        map = &m_resend;
    }

    if (it == map->end())
    {
        return 0;
    }

    // Pass the response to the command
    e::intrusive_ptr<command> c = it->second;
    WTFSETSUCCESS;

    switch (rc)
    {
        case wtf::RESPONSE_SUCCESS:
            c->succeed(msg, up.as_slice(), WTF_SUCCESS);
            break;
        case wtf::RESPONSE_REGISTRATION_FAIL:
            c->fail(WTF_SERVERERROR);
            m_last_error_desc = "server treated request as a registration";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case wtf::RESPONSE_OBJ_NOT_EXIST:
            c->fail(WTF_NOTFOUND);
            m_last_error_desc = "object not found";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case wtf::RESPONSE_SERVER_ERROR:
            c->fail(WTF_SERVERERROR);
            m_last_error_desc = "server reports error; consult server logs for details";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        case wtf::RESPONSE_MALFORMED:
            c->fail(WTF_INTERNAL);
            m_last_error_desc = "server reports that request was malformed";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
        default:
            c->fail(WTF_SERVERERROR);
            m_last_error_desc = "unknown response code";
            m_last_error_file = __FILE__;
            m_last_error_line = __LINE__;
            break;
    }

    c->set_last_error_desc(m_last_error_desc);
    c->set_last_error_file(m_last_error_file);
    c->set_last_error_line(m_last_error_line);
    map->erase(it);
    m_complete.insert(std::make_pair(c->nonce(), c));
    return 0;
}

void
wtf_client :: handle_disruption(const chain_node& from,
                                      wtf_returncode*)
{
    for (command_map::iterator it = m_commands.begin(); it != m_commands.end(); )
    {
        e::intrusive_ptr<command> c = it->second;

        // If this op wasn't sent to the failed host, then skip it
        if (c->sent_to() != from)
        {
            ++it;
            continue;
        }

        m_resend.insert(*it);
        m_commands.erase(it);
        it = m_commands.begin();
    }
}

uint64_t
wtf_client :: generate_token()
{
    try
    {
        po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

        if (sysrand.get() < 0)
        {
            return e::time();
        }

        uint64_t token;

        if (sysrand.read(&token, sizeof(token)) != sizeof(token))
        {
            return e::time();
        }

        return token;
    }
    catch (po6::error& e)
    {
        return e::time();
    }
}

std::ostream&
operator << (std::ostream& lhs, wtf_returncode rhs)
{
    switch (rhs)
    {
        stringify(WTF_SUCCESS);
        stringify(WTF_NOTFOUND);
        stringify(WTF_READONLY);
        stringify(WTF_UNKNOWNSPACE);
        stringify(WTF_COORDFAIL);
        stringify(WTF_SERVERERROR);
        stringify(WTF_POLLFAILED);
        stringify(WTF_OVERFLOW);
        stringify(WTF_RECONFIGURE);
        stringify(WTF_TIMEOUT);
        stringify(WTF_NONEPENDING);
        stringify(WTF_NOMEM);
        stringify(WTF_BADCONFIG);
        stringify(WTF_DUPLICATE);
        stringify(WTF_INTERRUPTED);
        stringify(WTF_CLUSTER_JUMP);
        stringify(WTF_COORD_LOGGED);
        stringify(WTF_BACKOFF);
        stringify(WTF_INTERNAL);
        stringify(WTF_EXCEPTION);
        stringify(WTF_GARBAGE);
        default:
            lhs << "unknown returncode (" << static_cast<unsigned int>(rhs) << ")";
    }

    return lhs;
}
