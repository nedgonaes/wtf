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

// C
#include <string.h>

// e
#include <e/endian.h>
#include <e/time.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_st.h>

// WTF
#include "common/configuration.h"
#include "common/coordinator_returncode.h"
#include "common/macros.h"
#include "common/mapper.h"
#include "common/network_msgtype.h"
#include "common/response_returncode.h"
#include "common/special_objects.h"
#include "client/command.h"
#include "client/file.h"
#include "client/coordinator_link.h"
#include "client/wtf.h"

using namespace wtf;
using namespace std;

#define MAINTAIN_COORD_CONNECTION(STATUS) \
    if (maintain_coord_connection(STATUS) < 0) \
    { \
        return -1; \
    }

#define WTFSETERROR(CODE, DESC) \
    do \
    { \
        m_last_error_desc = DESC; \
        m_last_error_file = __FILE__; \
        m_last_error_line = __LINE__; \
        *status = CODE; \
    } while (0)

#define WTFSETSUCCESS WTFSETERROR(WTF_SUCCESS, "operation succeeded")

// busybee header + nonce + msgtype
#define COMMAND_MSGTYPE_OFFSET (BUSYBEE_HEADER_SIZE)
#define COMMAND_NONCE_OFFSET (BUSYBEE_HEADER_SIZE + pack_size(wtf::WTFNET_PUT))
#define COMMAND_DATA_OFFSET (COMMAND_NONCE_OFFSET + sizeof(uint64_t))

#define BUSYBEE_ERROR(REPRC, BBRC) \
    case BUSYBEE_ ## BBRC: \
        WTFSETERROR(WTF_ ## REPRC, "BusyBee returned " XSTR(BBRC)); \
        return -1

#define BUSYBEE_ERROR_DISCONNECT(REPRC, BBRC) \
    case BUSYBEE_ ## BBRC: \
        WTFSETERROR(WTF_ ## REPRC, "BusyBee returned " XSTR(BBRC)); \
        reset_to_disconnected(); \
        return -1

#define BUSYBEE_ERROR_CONTINUE(REPRC, BBRC) \
    case BUSYBEE_ ## BBRC: \
        WTFSETERROR(WTF_ ## REPRC, "BusyBee returned " XSTR(BBRC)); \
        continue

#define WTF_UNEXPECTED(MT) \
    case WTFNET_ ## MT: \
        WTFSETERROR(WTF_SERVERERROR, "unexpected " XSTR(MT) " message"); \
        m_last_error_host = from; \
        return -1

#define WTF_UNEXPECTED_DISCONNECT(MT) \
    case WTFNET_ ## MT: \
        WTFSETERROR(WTF_SERVERERROR, "unexpected " XSTR(MT) " message"); \
        m_last_error_host = from; \
        reset_to_disconnected(); \
        return -1


wtf_client :: wtf_client(const char* host, in_port_t port,
                         const char* hyper_host, in_port_t hyper_port)
    : m_config(new wtf::configuration())
    , m_busybee_mapper(new wtf::mapper())
    , m_busybee(new busybee_st(m_busybee_mapper.get(), 0))
    , m_coord(new wtf::coordinator_link(po6::net::hostname(host, port)))
    , m_nonce(1)
    , m_fileno(1)
    , m_have_seen_config(false)
    , m_commands()
    , m_complete()
    , m_resend()
    , m_fds()
    , m_last_error_desc("")
    , m_last_error_file(__FILE__)
    , m_last_error_line(__LINE__)
    , m_last_error_host()
    , m_hyperdex_client(hyper_host, hyper_port)
{
}

wtf_client :: ~wtf_client() throw ()
{
}

int64_t
wtf_client :: send(e::intrusive_ptr<command>& cmd, wtf_returncode* status)
{
    MAINTAIN_COORD_CONNECTION(status);

    // Create the command object
    return send_to_blockserver(cmd, status);
}

int64_t
wtf_client :: loop(int timeout, wtf_returncode* status)
{
    ////std::cout << "Starting loop, status: " << *status << std::endl;
    while ((!m_commands.empty() || !m_resend.empty())
           && m_complete.empty())
    {
        if (maintain_coord_connection(status) < 0)
        {
            return -1;
        }
        ////std::cout << "Looping, status: " << *status << std::endl;


        // Always set timeout
        m_busybee->set_timeout(timeout);
        int64_t ret = inner_loop(status);

        ////std::cout << "after inner_loop, status: " << *status << std::endl;

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
        return c->nonce();
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
        if (maintain_coord_connection(status) < 0)
        {
            return -1;
        }

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
    return c->nonce();
}

int64_t
wtf_client :: maintain_coord_connection(wtf_returncode* status)
{
    if (!m_have_seen_config)
    {
        if (!m_coord->wait_for_config(status))
        {
            return -1;
        }

        if (m_busybee->set_external_fd(m_coord->poll_fd()) < 0)
        {
            *status = WTF_POLLFAILED;
            return -1;
        }
    }
    else
    {
        if (!m_coord->poll_for_config(status))
        {
            return 0;
        }
    }

    int64_t reconfigured = 0;

    if (m_config->version() < m_coord->config().version())
    {
        *m_config = m_coord->config();
        command_map::iterator i = m_commands.begin();

        while (i != m_commands.end())
        {
            //XXX: Find the reconfigured nodes and retry ops to new nodes
            ++i;
        }

        m_have_seen_config = true;
    }

    return reconfigured;
}

wtf_returncode
wtf_client :: initialize_cluster(uint64_t cluster, const char* path)
{
    replicant_client* repl = m_coord->replicant();
    replicant_returncode rstatus;
    const char* errmsg = NULL;
    size_t errmsg_sz = 0;

    int64_t noid = repl->new_object("wtf", path, &rstatus,
                                    &errmsg, &errmsg_sz);

    if (noid < 0)
    {
        switch (rstatus)
        {
            case REPLICANT_BAD_LIBRARY:
                return WTF_NOTFOUND;
            case REPLICANT_INTERRUPTED:
                return WTF_INTERRUPTED;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_BACKOFF:
                return WTF_COORDFAIL;
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_TIMEOUT:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                return WTF_INTERNAL;
        }
    }

    replicant_returncode lstatus;
    int64_t lid = repl->loop(noid, -1, &lstatus);

    if (lid < 0)
    {
        repl->kill(noid);

        switch (lstatus)
        {
            case REPLICANT_INTERRUPTED:
                return WTF_INTERRUPTED;
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_BACKOFF:
                return WTF_COORDFAIL;
            case REPLICANT_TIMEOUT:
            case REPLICANT_SUCCESS:
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_GARBAGE:
            default:
                return WTF_INTERNAL;
        }
    }

    assert(lid == noid);

    switch (rstatus)
    {
        case REPLICANT_SUCCESS:
            break;
        case REPLICANT_INTERRUPTED:
            return WTF_INTERRUPTED;
        case REPLICANT_OBJ_EXIST:
            return WTF_DUPLICATE;
        case REPLICANT_FUNC_NOT_FOUND:
        case REPLICANT_OBJ_NOT_FOUND:
        case REPLICANT_COND_NOT_FOUND:
        case REPLICANT_COND_DESTROYED:
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_NEED_BOOTSTRAP:
        case REPLICANT_MISBEHAVING_SERVER:
            return WTF_COORDFAIL;
        case REPLICANT_NAME_TOO_LONG:
        case REPLICANT_BAD_LIBRARY:
            return WTF_COORD_LOGGED;
        case REPLICANT_TIMEOUT:
        case REPLICANT_BACKOFF:
        case REPLICANT_INTERNAL_ERROR:
        case REPLICANT_NONE_PENDING:
        case REPLICANT_GARBAGE:
        default:
            return WTF_INTERNAL;
    }

    wtf_returncode status;
    char data[sizeof(uint64_t)];
    e::pack64be(cluster, data);
    const char* output;
    size_t output_sz;

    if (!m_coord->make_rpc("initialize", data, sizeof(uint64_t),
                           &status, &output, &output_sz))
    {
        return status;
    }

    status = WTF_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case wtf::COORD_SUCCESS:
                status = WTF_SUCCESS;
                break;
            case wtf::COORD_MALFORMED:
                status = WTF_INTERNAL;
                break;
            case wtf::COORD_DUPLICATE:
                status = WTF_CLUSTER_JUMP;
                break;
            case wtf::COORD_NOT_FOUND:
                status = WTF_INTERNAL;
                break;
            case wtf::COORD_INITIALIZED:
                status = WTF_DUPLICATE;
                break;
            case wtf::COORD_UNINITIALIZED:
                status = WTF_COORDFAIL;
                break;
            default:
                status = WTF_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
}

wtf_returncode
wtf_client :: show_config(std::ostream& out)
{
    wtf_returncode status;

    if (maintain_coord_connection(&status) < 0)
    {
        return status;
    }

    m_config->debug_dump(out);
    return WTF_SUCCESS;
}

wtf_returncode
wtf_client :: kill(uint64_t server_id)
{
    wtf_returncode status;
    char data[sizeof(uint64_t)];
    e::pack64be(server_id, data);
    const char* output;
    size_t output_sz;

    if (!m_coord->make_rpc("kill", data, sizeof(uint64_t),
                           &status, &output, &output_sz))
    {
        return status;
    }

    status = WTF_SUCCESS;

    if (output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case wtf::COORD_SUCCESS:
                status = WTF_SUCCESS;
                break;
            case wtf::COORD_MALFORMED:
                status = WTF_INTERNAL;
                break;
            case wtf::COORD_DUPLICATE:
                status = WTF_DUPLICATE;
                break;
            case wtf::COORD_NOT_FOUND:
                status = WTF_NOTFOUND;
                break;
            case wtf::COORD_INITIALIZED:
                status = WTF_INTERNAL;
                break;
            case wtf::COORD_UNINITIALIZED:
                status = WTF_COORDFAIL;
                break;
            default:
                status = WTF_INTERNAL;
                break;
        }
    }

    if (output)
    {
        replicant_destroy_output(output, output_sz);
    }

    return status;
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
        ////std::cout << "Resending..." << std::endl;
        ret = send_to_blockserver(m_resend.begin()->second, status);

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
    const wtf_node* node = m_config->node_from_token(id);
    
    ////std::cout << rc << std::endl;

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
wtf_client :: open(const char* path)
{
    wtf_returncode status;
    MAINTAIN_COORD_CONNECTION(&status);
    e::intrusive_ptr<file> f = new file(path);
    m_fds[m_fileno] = f; 
    
    update_file_cache(path, f);

    return m_fileno++;
}

void
wtf_client :: lseek(int64_t fd, uint64_t offset)
{
    m_fds[fd]->set_offset(offset);
}

int64_t
wtf_client :: write(int64_t fd,
                    const char* data,
                    uint32_t data_sz,
                    uint32_t replicas,
                    wtf_returncode* status)
{
    int64_t rid = 0;
    int64_t lid = 0;
    uint32_t rem = data_sz;

    if (m_fds.find(fd) == m_fds.end())
    {
        ////std::cout << "invalid fd: " << fd <<  std::endl;
        return -1;
    }

    e::intrusive_ptr<file> f = m_fds[fd];

    while(rem > 0)
    {
        uint64_t bid = f->offset()/CHUNKSIZE;
        ////std::cout << "f->offset(): " << f->offset() << " CHUNKSIZE: " << CHUNKSIZE << std::endl;
        ////std::cout << "bid " << bid << std::endl; 
        uint64_t len = ROUNDUP(f->offset() + 1, CHUNKSIZE) - f->offset();
        len = MIN(len, rem); 
        uint64_t version = f->get_block_version(bid) + 1;
        uint64_t block_off = f->offset() - f->offset()/CHUNKSIZE * CHUNKSIZE;

        ////std::cout << "data_sz = " << data_sz << std::endl;
        ////std::cout << "Len = " << len << std::endl;
        ////std::cout << "Rem = " << rem << std::endl;
        uint64_t bl = f->get_block_length(bid);

        if ((bl > 0 && block_off > 0) || (len < bl))
        {
            //Overwrite a partial block.

            /*XXX: send a special instruction to servers that already have the
              block so that we can copy it and append new
              data rather than copy the whole block over the network. */ 

            ////std::cout << "ERROR (not implemented): You tried "
                //<< "over-writing a partial block." << std::endl;
            if (block_off > 0)
            {
                ////std::cout << "Block offset is " << block_off << std::endl;
            }    
            else
            {
                ////std::cout << "Block length " << len << " does not match "
                   // << "existing block length " << bl << std::endl;
            }
            abort();

            typedef std::vector<wtf::block_id> block_map;
            block_map::iterator it = f->lookup_block_begin(bid);

            /* XXX: Need to handle cases where replicas != existing replica set */
            for (block_map::iterator it = f->lookup_block_begin(bid);
                 it != f->lookup_block_end(bid); ++it)
            {
                wtf::wtf_node node = *m_config->node_from_token(it->server());
                e::intrusive_ptr<command> cmd = new command(node,
                                                it->block(),
                                                fd, bid, block_off, version,
                                                data, len, m_nonce, wtf::WTFNET_UPDATE,
                                                NULL, 0);

                rid = send(cmd, status);
            }
        }
        else
        {
            for (int i = 0; i < replicas; ++i)
            {
                //write new blocks or full-blocks.
                e::intrusive_ptr<command> cmd = new command(wtf::wtf_node() /*send_to*/,
                                                0 /*remote_bid*/,
                                                fd, bid, block_off, version,
                                                data, len, m_nonce, wtf::WTFNET_PUT,
                                                NULL, 0);

                rid = send(cmd, status);

                if (rid < 0)
                {
                    return -1;
                }
            }
        }

        rem -= len;
        data += len;
        f->set_offset(f->offset() + len);
    }

    return rid;
}

int64_t
wtf_client :: read(int64_t fd, char* data,
                   uint32_t data_sz,
                   wtf_returncode* status)
{
    int64_t rid = 0;
    int64_t lid = 0;
    uint32_t rem = data_sz;

    cout << "wtf read:" << endl;
    cout << "fd " << fd << " data_sz " << data_sz << endl;

    if (m_fds.find(fd) == m_fds.end())
    {
        return -1;
    }

    e::intrusive_ptr<file> f = m_fds[fd];

    update_file_cache(f->path().get(), f);

    while(rem > 0)
    {
        uint64_t bid = f->offset()/CHUNKSIZE;
        std::cout << "bid " << bid << " f->offset() " << f->offset() << " CHUNKSIZE " << CHUNKSIZE << std::endl;
        uint64_t len = ROUNDUP(f->offset() + 1, CHUNKSIZE) - f->offset();
        len = MIN(len, rem); 
        uint64_t version = f->get_block_version(bid);
        uint64_t block_off = f->offset() - f->offset()/CHUNKSIZE * CHUNKSIZE;

        std::cout << "data_sz " << data_sz << " len " << len << " rem " << rem << std::endl;

        wtf::block_id block = f->lookup_block(bid);
        wtf::wtf_node send_to = *m_config->node_from_token(block.server());
        //XXX: pass in output buffer.
        //XXX: If read fails, read from the next node on the list.

        e::intrusive_ptr<command> cmd = new command(send_to,
                block.block(),
                fd, bid, block_off, version,
                NULL, 0, m_nonce, wtf::WTFNET_GET,
                data, len);

        rid = send(cmd, status);

        if (rid < 0)
        {
            return -1;
        }

        rem -= len;
        data += len;
        f->set_offset(f->offset() + len);
    }

    return rid;


//    //XXX: Make sure not to overwrite the outstanding changes.

//    /*
//     * Always read from hyperdex.  If we want to see latest
//     * changes, must call flush() first.
//     */ 
}

int64_t
wtf_client :: close(int64_t fd, wtf_returncode* rc)
{
    int64_t ret = flush(fd, rc);

    if (ret > 0)
    {
        m_fds.erase(fd);
        return 0;
    }

    return -1;
}

int64_t
wtf_client :: flush(int64_t fd, wtf_returncode* rc)
{
    int64_t rid = -1;
    if(m_fds.find(fd) == m_fds.end())
    {
        //std::cout << "bad fd" << std::endl;
    }

    e::intrusive_ptr<file> f = m_fds[fd];

    if(f->commands_begin() == f->commands_end())
    {
        //std::cout << "no commands" << std::endl;
    }

    for (command_map::const_iterator it = f->commands_begin();
         it != f->commands_end(); ++it)
    {
        e::intrusive_ptr<command> cmd = it->second;
        uint64_t id = it->first;

        //std::cout << "Flushing " << cmd->nonce() << std::endl;
        //std::cout << "STATUS: " << cmd->status() << std::endl;
        //std::cout << "id: " << id << std::endl;

        if (cmd->status() != WTF_SUCCESS)
        {
            *rc = WTF_GARBAGE;

            //std::cout << "Looping" << std::endl;
            rid = loop(it->first, -1, rc);

            if (rid < 0 || *rc != WTF_SUCCESS)
            {
                return rid;
            }
        }
        else
        {
            rid = 1;
        }

        switch (cmd->msgtype())
        {
            case WTFNET_PUT:
                handle_put(cmd, f); //XXX: rename
                break;
            case WTFNET_GET:
                handle_get(cmd, f);
                break;
            default:
                //XXX: handle this case.
                abort();
        }

    }

    update_hyperdex(f);

    //std::cout << "Done flushing." << std::endl;
    
    f->gc_completed(rc);
    return rid;
}

int64_t
wtf_client :: send_to_blockserver(e::intrusive_ptr<command> cmd,
                                               wtf_returncode* status)
{
    static int last_node = 0;
    bool sent = false;

    //std::cout << "Sending to random node." << std::endl;
    //std::cout << "Configuration is: " ;
    //m_config->debug_dump(std::cout);

    if(cmd->sent_to() == wtf_node())
    {
        cmd->set_sent_to(*m_config->get_random_member(generate_token()));
    }

    std::auto_ptr<e::buffer> msg(cmd->request()->copy());
    wtf_node send_to = cmd->sent_to();
    m_busybee_mapper->set(send_to);
    busybee_returncode rc = m_busybee->send(send_to.token, msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            sent = true;
            break;
        case BUSYBEE_DISRUPTED:
            //XXX
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

        if (m_fds.find(cmd->fd()) == m_fds.end())
        {
            //std::cout << "invalid fd: " << cmd->fd() <<  std::endl;
            return -1;
        }

        m_commands[cmd->nonce()] = cmd;
        e::intrusive_ptr<file> f = m_fds[cmd->fd()];
        f->add_command(cmd);
        return cmd->nonce();
    }
    else
    {
        // We have an error captured by WTFSETERROR above.
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

    //std::cout << "nonce: " << nonce << " rc: " << rc << std::endl;

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
wtf_client :: handle_put(e::intrusive_ptr<command>& cmd,
                         e::intrusive_ptr<file>& f)
{
    uint64_t sid; 
    uint64_t bid;
    uint64_t block_index = cmd->block();
    uint64_t len = cmd->length();
    std::auto_ptr<e::buffer> msg(e::buffer::create(cmd->output(), cmd->output_sz()));
    e::unpacker up = msg->unpack_from(0);
    up = up >> sid >> bid;

    if (up.error())
    {
        m_last_error_host = cmd->sent_to().address;
        //XXX: implement proper return value
        return;
    }

    //std::cout << "sid: " << sid << "bid: " << bid << std::endl;

    f->update_blocks(block_index, len, cmd->version(), sid, bid);
}

int64_t
wtf_client :: update_file_cache(const char* path, e::intrusive_ptr<file>& f)
{
    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    int64_t ret = 0;
    hyperdex_client_returncode status;

    ret = m_hyperdex_client.get("wtf", path, strlen(path), &status, &attrs, &attrs_sz);
    if (ret == -1)
    {
        //std::cout << "hyperdex_client_get returned " << status;
        return -1;
    }

    hyperdex_client_returncode res = hyperdex_wait_for_result(ret, status);
    //std::cout << "hyperdex_wait_for_result returned " << res;

    if (res == HYPERDEX_CLIENT_NOTFOUND)
    {
        /* The file does not exist or was deleted, truncate it. */
        f->set_offset(0);
        f->truncate();
    }
    else
    {
        for (size_t i = 0; i < attrs_sz; ++i)
        {
            if (strcmp(attrs[i].attr, "blockmap") == 0)
            {
                e::unpacker up(attrs[i].value, attrs[i].value_sz);

                while (!up.empty())
                {
                    //std::cout << "up as slice hex [" << up.as_slice().hex() << "]" << std::endl;
                    uint32_t idlen;
                    uint64_t id;
                    uint32_t valuelen;
                    up = up >> idlen >> id >> valuelen;
                    e::unpack64be((uint8_t*)&id, &id);
                    e::unpack32be((uint8_t*)&valuelen, &valuelen);
                    cout << "idlen " << idlen << " id " << id << " valuelen " << valuelen << endl;
                    e::intrusive_ptr<wtf::block> b = new wtf::block();
                    up = up >> b;
                    f->update_blocks(id, b);
                }

                break;
            }
        }
    }
    
    return ret;
}

int64_t
wtf_client :: update_hyperdex(e::intrusive_ptr<file>& f)
{
    int64_t ret = -1;
    int i = 0;

    std::vector<struct hyperdex_client_map_attribute> attrs;
    hyperdex_client_returncode status;

    typedef std::map<uint64_t, e::intrusive_ptr<wtf::block> > block_map;

    //XXX; get rid of magic string.
    const char* name = "blockmap";

    /*
     * construct a hyperdex attribute list for all dirty blocks
     */
    for (block_map::const_iterator it = f->blocks_begin();
         it != f->blocks_end(); ++it)
    {
        if (it->second->dirty())
        {
            struct hyperdex_client_map_attribute attr;
            attrs.push_back(attr);
            attrs[i].attr = name;
            attrs[i].map_key = (const char*)malloc(sizeof(uint64_t));
            memmove(const_cast<char*>(attrs[i].map_key), &it->first, sizeof(uint64_t));
            attrs[i].map_key_sz = sizeof(uint64_t);
            attrs[i].map_key_datatype = HYPERDATATYPE_STRING;

            uint64_t sz = it->second->pack_size();
            attrs[i].value = (const char*)malloc(sz);
            it->second->pack(const_cast<char*>(attrs[i].value)); 
            attrs[i].value_datatype = HYPERDATATYPE_STRING;
            attrs[i].value_sz = sz;
            ++i;
        }
    }
    
    //XXX; get rid of magic string.
    //XXX; atomic map add?
    /*
     * Update hyperdex to point to location of new blocks
     */
retry:
    ret = m_hyperdex_client.map_add("wtf", f->path().get(), strlen(f->path().get()), &attrs[0], attrs.size(), &status);

    /*
     * Wait for hyperdex to reply
     */

    hyperdex_client_returncode res = hyperdex_wait_for_result(ret, status);

    if (res == HYPERDEX_CLIENT_NOTFOUND)
    {
        ret = m_hyperdex_client.put_if_not_exist("wtf", f->path().get(), strlen(f->path().get()), NULL, 0, &status);
        res = hyperdex_wait_for_result(ret, status);

        if (res == HYPERDEX_CLIENT_SUCCESS || res == HYPERDEX_CLIENT_CMPFAIL)
        {
            //GUN DID IT.
            goto retry;
        }
        else
        {
            //std::cout << "ERROR: Hyperdex returned " << res << std::endl;
        }
    }
    else
    {
        //std::cout << "Hyperdex returned " << res << std::endl;
    }

    for (std::vector<struct hyperdex_client_map_attribute>::iterator it = attrs.begin();
         it != attrs.end(); ++it)
    {
       free(const_cast<char*>(it->value));
       free(const_cast<char*>(it->map_key));
    }


    return ret;
}

hyperdex_client_returncode
wtf_client::hyperdex_wait_for_result(int64_t reqid, hyperdex_client_returncode& status)
{
    while(1)
    {
        hyperdex_client_returncode lstatus;
        int64_t id = m_hyperdex_client.loop(-1, &lstatus);

        if (id < 0) 
        {
            return lstatus;
        }

        if (id != reqid){
            //std::cout << "ERROR: Hyperdex returned id:" << id << " status:"<< lstatus << std::endl;
            //std::cout << "expected id:" << reqid<< std::endl;
        }
        else
        {
            //std::cout << "Hyperdex returned " << status << std::endl;
            return status;
        }
    } 
}

void
wtf_client :: handle_get(e::intrusive_ptr<command>& cmd,
                         e::intrusive_ptr<file>& f)
{
    //no need to do anything
}

void
wtf_client :: handle_disruption(const wtf_node& from,
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
        po6::io::fd sysrand(::open("/dev/urandom", O_RDONLY));

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
        STRINGIFY(WTF_SUCCESS);
        STRINGIFY(WTF_NOTFOUND);
        STRINGIFY(WTF_READONLY);
        STRINGIFY(WTF_UNKNOWNSPACE);
        STRINGIFY(WTF_COORDFAIL);
        STRINGIFY(WTF_SERVERERROR);
        STRINGIFY(WTF_POLLFAILED);
        STRINGIFY(WTF_OVERFLOW);
        STRINGIFY(WTF_RECONFIGURE);
        STRINGIFY(WTF_TIMEOUT);
        STRINGIFY(WTF_NONEPENDING);
        STRINGIFY(WTF_NOMEM);
        STRINGIFY(WTF_BADCONFIG);
        STRINGIFY(WTF_DUPLICATE);
        STRINGIFY(WTF_INTERRUPTED);
        STRINGIFY(WTF_CLUSTER_JUMP);
        STRINGIFY(WTF_COORD_LOGGED);
        STRINGIFY(WTF_BACKOFF);
        STRINGIFY(WTF_INTERNAL);
        STRINGIFY(WTF_EXCEPTION);
        STRINGIFY(WTF_GARBAGE);
        default:
            lhs << "unknown returncode (" << static_cast<unsigned int>(rhs) << ")";
    }

    return lhs;
}
