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
#include <pwd.h>
#include <grp.h>
#include <ctype.h>

// e
#include <e/endian.h>
#include <e/time.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_st.h>
#include <busybee_utils.h>

// WTF
#include "client/client.h"
#include "client/constants.h"
#include "common/configuration.h"
#include "common/coordinator_returncode.h"
#include "common/macros.h"
#include "common/mapper.h"
#include "common/network_msgtype.h"
#include "common/response_returncode.h"
#include "common/special_objects.h"
#include "client/file.h"
#include "common/coordinator_link.h"
#include "client/pending_getattr.h"
#include "client/pending_truncate.h"
#include "client/pending_chdir.h"
#include "client/pending_write.h"
#include "client/pending_readdir.h"
#include "client/pending_del.h"
#include "client/pending_read.h"
#include "client/pending_rename.h"
#include "client/pending_chmod.h"
#include "client/pending_mkdir.h"
#include "client/pending_creat.h"
#include "client/pending_open.h"
#include "client/buffer_descriptor.h"
#include "visibility.h"

#define ERROR(CODE) \
    *status = WTF_CLIENT_ ## CODE; \
    m_last_error.set_loc(__FILE__, __LINE__); \
    m_last_error.set_msg()

#define _BUSYBEE_ERROR(BBRC) \
    case BUSYBEE_ ## BBRC: \
        ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned " XSTR(BBRC) << ": please file a bug"

#define BUSYBEE_ERROR_CASE(BBRC) \
    _BUSYBEE_ERROR(BBRC); \
    return -1;

#define BUSYBEE_ERROR_CASE_FALSE(BBRC) \
    _BUSYBEE_ERROR(BBRC); \
    return false;

#ifdef LOG_METADATA 
#define LOGMETADATA std::cerr << "MESSAGE: " << up.as_slice().hex() << std::endl
#else
#define LOGMETADATA
#endif

using wtf::client;

client :: client(const char* host, in_port_t port,
                         const char* hyper_host, in_port_t hyper_port)
    : m_coord(host, port)
    , m_gc()
    , m_gc_ts()
    , m_token(busybee_generate_id())
    , m_busybee_mapper(m_coord.config())
    , m_busybee(&m_gc, &m_busybee_mapper, m_token)
    , m_next_client_id(1)
    , m_next_server_nonce(1)
    , m_pending_ops()
    , m_pending_hyperdex_ops()
    , m_failed()
    , m_yielding()
    , m_yielded()
    , m_last_error()
    , m_hyperdex_client(hyper_host, hyper_port)
    , m_next_fileno(1)
    , m_fds()
    , m_cwd("/")
    , m_addr()
{
	TRACE;
    busybee_discover(&m_addr);
    m_gc.register_thread(&m_gc_ts);
}

client :: ~client() throw ()
{
	TRACE;
    m_gc.deregister_thread(&m_gc_ts);
}

bool
client :: maintain_coord_connection(wtf_client_returncode* status)
{
	TRACE;
    replicant_returncode rc;
    uint64_t old_version = m_coord.config()->version();

    if (!m_coord.ensure_configuration(&rc))
    {
        if (rc == REPLICANT_INTERRUPTED)
        {
            ERROR(INTERRUPTED) << "signal received";
        }
        else if (rc == REPLICANT_TIMEOUT)
        {
            ERROR(TIMEOUT) << "operation timed out";
        }
        else
        {
            ERROR(COORDFAIL) << "coordinator failure: " << m_coord.error().msg();
        }

        return false;
    }

    if (m_busybee.set_external_fd(m_coord.poll_fd()) != BUSYBEE_SUCCESS)
    {
        *status = WTF_CLIENT_POLLFAILED;
        return false;
    }

    uint64_t new_version = m_coord.config()->version();

    if (old_version < new_version)
    {
        pending_map_t::iterator it = m_pending_ops.begin();
        for (const server * s = m_coord.config()->servers_begin();
             s != m_coord.config()->servers_end();
             ++s)
        {
            send_nop(server_id(s->id));
        }

        //std::cout << "SLEEPING 5" << std::endl;
        sleep(0);

        while (it != m_pending_ops.end())
        {
            // If the server that was in configuration when the operation 
            // started is no longer in configuration, we fail the operation 
            // with a RECONFIGURE.
            if (!m_coord.config()->exists(it->second.si))
            {
                m_failed.push_back(it->second);
                pending_map_t::iterator tmp = it;
                ++it;
                m_pending_ops.erase(tmp);
            }
            else
            {
                ++it;
            }
        }
    }

    return true;
}

void
client :: prepare_write_op(e::intrusive_ptr<file> f, 
                              size_t& rem, 
                              std::vector<block_location>& bl,
                              size_t& buf_offset,
                              uint32_t& block_offset,
                              uint32_t& block_capacity,
                              uint64_t& file_offset,
                              size_t& slice_len)
{
    f->copy_current_block_locations(bl);
    m_coord.config()->assign_random_block_locations(bl, m_addr);
    block_offset = f->current_block_offset();
    block_capacity = f->current_block_capacity();
    file_offset = f->current_block_start();
    std::cout << "XXX FILE OFFSET: " << file_offset << std::endl;
    slice_len = f->advance_to_end_of_block(rem);
    buf_offset += slice_len;
    rem -= slice_len;
}

int64_t
client :: perform_aggregation(const std::vector<server_id>& servers,
                              e::intrusive_ptr<pending_aggregation> _op,
                              wtf_network_msgtype mt,
                              std::auto_ptr<e::buffer> msg,
                              wtf_client_returncode* status)
{
	TRACE;
    e::intrusive_ptr<pending_aggregation> op(_op.get());
    //std::cout << "SERVER COUNT IS " << servers.size() << std::endl;

    m_next_server_nonce += servers.size();
    uint64_t nonce = m_next_server_nonce;

    for (int i = servers.size()-1; i > -1; --i)
    {
        //std::cout << "SERVER COUNT IS " << servers.size() << std::endl;
        nonce--;
        pending_server_pair psp(servers[i], op);

        if (i == 0)
        {
            //sleep(1);
            //Send the data to the master replica
            //std::cout << "SENDING DATA" << std::endl;
            std::auto_ptr<e::buffer> msg_copy(msg->copy());

            //std::cout << "NONCE WAS " << nonce << std::endl;
            if (!send(mt, psp.si, nonce, msg_copy, op, status))
            {
                //std::cout << "FAILED TO SEND TO " << psp.si << std::endl;
                //std::cout << "NONCE WAS " << nonce << std::endl;
                m_failed.push_back(psp);
            }
        }
        else
        {
            //Send a NOP to set up a channel to the other servers.
            //std::cout << "SENDING NOP" << std::endl;
            size_t sz = WTF_CLIENT_HEADER_SIZE_REQ;
            std::auto_ptr<e::buffer> msg_copy(e::buffer::create(sz));
            wtf_network_msgtype msgtype = PACKET_NOP;

            //std::cout << "NONCE WAS " << nonce << std::endl;
            if (!send(msgtype, psp.si, nonce, msg_copy, op, status))
            {
                //std::cout << "FAILED TO SEND TO " << psp.si << std::endl;
                //std::cout << "NONCE WAS " << nonce << std::endl;
                m_failed.push_back(psp);
            }
        }
    }

    return op->client_visible_id();
}

bool
client :: send_nop(const server_id& to)
{
	TRACE;
    std::auto_ptr<e::buffer> msg(e::buffer::create(WTF_CLIENT_HEADER_SIZE_REQ));
    const uint8_t type = static_cast<uint8_t>(PACKET_NOP);
    const uint8_t flags = 0;
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << type << m_next_server_nonce++;
    m_busybee.set_timeout(-1);
    m_busybee.send(to.get(), msg);
}

bool
client :: send(wtf_network_msgtype mt,
               const server_id& to,
               uint64_t nonce,
               std::auto_ptr<e::buffer> msg,
               e::intrusive_ptr<pending_aggregation> op,
               wtf_client_returncode* status)
{
	TRACE;
    const uint8_t type = static_cast<uint8_t>(mt);
    const uint8_t flags = 0;
    const uint64_t version = m_coord.config()->version();
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << type << nonce;
    m_busybee.set_timeout(-1);
    busybee_returncode rc = m_busybee.send(to.get(), msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            op->handle_sent_to_wtf(to);
            m_pending_ops.insert(std::make_pair(nonce, pending_server_pair(to, op)));
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(to);
            ERROR(SERVERERROR) << "server " << to.get() << " had a communication disruption";
            return false;
        BUSYBEE_ERROR_CASE_FALSE(SHUTDOWN);
        BUSYBEE_ERROR_CASE_FALSE(POLLFAILED);
        BUSYBEE_ERROR_CASE_FALSE(ADDFDFAIL);
        BUSYBEE_ERROR_CASE_FALSE(TIMEOUT);
        BUSYBEE_ERROR_CASE_FALSE(EXTERNAL);
        BUSYBEE_ERROR_CASE_FALSE(INTERRUPTED);
        default:
            ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned "
                                << (unsigned) rc << ": please file a bug";
            return false;
    }
}

int64_t
client :: loop(int64_t client_id, int timeout, wtf_client_returncode* status)
{
	TRACE;
    int64_t out = inner_loop(timeout, status, client_id);
    return out;
}

int64_t
client :: loop(int timeout, wtf_client_returncode* status)
{
	TRACE;
    return inner_loop(timeout, status, -1);
}

int64_t
client :: inner_loop(int timeout, wtf_client_returncode* status, int64_t wait_for)
{
	TRACE;
    /*
     * This is for internal use only.  We loop for any operation to
     * yield.  Client facing functions call op->handle_delivery()
     * so that we can keep track of which op is actually delivered
     * to the clients.  This mainly facilitates waiting for a specific
     * op to yield.  In that case, we need to store the ops we do not
     * care about in m_complete, to be delivered at another time.
     *
     * We could simply record the client_id and status, but in
     * the case of readdir, the same op returns multiple times
     * and assumes the client is storing the return location each
     * time.  If we were to loop multiple times without giving it
     * to the client, we'd miss some of the results.
     * 
     * In the readdir case, we simply make can_yeild() false until
     * one of the client-facing functions has delivered the yielded
     * result to the client, calling op->handle_delivered().
     */

    *status = WTF_CLIENT_SUCCESS;
    m_last_error = e::error();

    while (m_yielding ||
           !m_failed.empty() ||
           !m_pending_ops.empty() ||
           !m_pending_hyperdex_ops.empty() ||
           !m_yieldable.empty())
    {
        if (0)
        {
            std::cerr << "WAIT FOR = " << wait_for << std::endl;

            for (std::map<uint64_t, e::intrusive_ptr<pending_aggregation> >::iterator it = m_yieldable.begin(); 
                    it != m_yieldable.end(); ++it)
            {
                std::cerr << "YIELDABLE " << it->first << std::endl; 
            }

            for (std::map<uint64_t, pending_server_pair>::iterator it = m_pending_hyperdex_ops.begin(); 
                    it != m_pending_hyperdex_ops.end(); ++it)
            {
                std::cerr << "PENDINGHYPERDEX " << it->first << std::endl; 
            }

            for (std::map<uint64_t, pending_server_pair>::iterator it = m_pending_ops.begin(); 
                    it != m_pending_ops.end(); ++it)
            {
                std::cerr << "PENDINGWTF " << it->first << std::endl; 
            }

            for (std::list<pending_server_pair>::iterator it = m_failed.begin(); 
                    it != m_failed.end(); ++it)
            {
                std::cerr << "FAILED " << it->si << std::endl; 
            }
        }

        m_gc.quiescent_state(&m_gc_ts);

        /* Handle currently yielding operation first. */
        if (m_yielding)
        {
            int64_t client_id = m_yielding->client_visible_id();

            if (!m_yielding->can_yield())
            {
                m_yielded = m_yielding;
                m_yielding = NULL;
                TRACE;continue;
            }

            if (wait_for != client_id)
            {
                m_yieldable.insert(std::make_pair(client_id, m_yielding));
                m_yielding = NULL;
                TRACE;continue;
            }

            if (!m_yielding->yield(status, &m_last_error))
            {
                TRACE;
                return -1;
            }

            m_last_error = m_yielding->error();

            if (!m_yielding->can_yield())
            {
                /* m_yielded is never read.  Its purpose is to
                   delay the destruction of the yielding operation
                   until at least the next loop call. 
                   
                   If the op can still yield more, we leave m_yielding
                   alone and process more on the next call to loop
                   */
                m_yielded = m_yielding;
                m_yielding = NULL;
                TRACE;
                return client_id;
            }

            TRACE;
            return client_id;
        }

        /* Handle failed operations second */
        else if (!m_failed.empty())
        {
            const pending_server_pair& psp(m_failed.front());
            e::intrusive_ptr<pending_aggregation> op = psp.op;
            op->handle_wtf_failure(psp.si);
            
            if (wait_for > 0 && wait_for != op->client_visible_id())
            {
                m_yieldable.insert(std::make_pair(op->client_visible_id(), op)); 
            }
            else
            {
                m_yielding = op;
            }

            m_failed.pop_front();
            TRACE;continue;
        }

        /* the previously yielded operation can be destroyed now
           if no one else is referring to it. */
        m_yielded = NULL;


        if (!m_yieldable.empty() && wait_for > 0)
        {
            yieldable_map_t ::iterator it = m_yieldable.find(wait_for);
            if (it != m_yieldable.end())
            {
                m_yielding = it->second;
                m_yieldable.erase(it);
                TRACE;continue;
            }
            /* nothing left to do here.*/
            else if (m_pending_ops.empty() && m_pending_hyperdex_ops.empty())
            {
                TRACE;
                return -1;
            }
        }
        else if(!m_yieldable.empty())
        {
            yieldable_map_t::iterator it = m_yieldable.begin();
            m_yielding = it->second;
            m_yieldable.erase(it);
            TRACE;continue;
        }


        /* Handle a new pending op. */
        assert(!m_pending_ops.empty() || !m_pending_hyperdex_ops.empty());

        if (!maintain_coord_connection(status))
        {
            TRACE;
            return -1;
        }

        uint64_t sid_num;

        m_busybee.set_external_fd(m_hyperdex_client.poll_fd());

        std::auto_ptr<e::buffer> msg;
        m_busybee.set_timeout(timeout);
        busybee_returncode rc = m_busybee.recv(&sid_num, &msg);

        server_id id(sid_num);
        //std::cout << "RECVD " << rc << " FROM " << id << std::endl;

        bool hyperdex_message = false;

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                hyperdex_message = false;
                break;
            case BUSYBEE_INTERRUPTED:
                ERROR(INTERRUPTED) << "signal received";
                TRACE;
                return -1;
            case BUSYBEE_TIMEOUT:
                ERROR(TIMEOUT) << "operation timed out";
                TRACE;
                return -1;
            case BUSYBEE_DISRUPTED:
                handle_disruption(id);
                TRACE;continue;
            case BUSYBEE_EXTERNAL:
                //std::cout << "GOT MESSAGE FROM " << sid_num << " HYPERDEX IS " << m_hyperdex_client.poll_fd() << std::endl;
                if (sid_num == m_hyperdex_client.poll_fd())
                {
                    hyperdex_message = true;
                    break;
                }
                TRACE;continue;
            BUSYBEE_ERROR_CASE(POLLFAILED);
            BUSYBEE_ERROR_CASE(ADDFDFAIL);
            BUSYBEE_ERROR_CASE(SHUTDOWN);
            default:
                ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned "
                                << (unsigned) rc << ": please file a bug";
                TRACE;
                return -1;
        }

        /*
         * If we got a hyperdex message, we look up the pending op in a
         * map indexed by hyperdex request ID.  We fill msg with the 
         * request id returned from hyperdex loop, and the status returned
         * from hyperdex. We can then pass this to op->handle_message just
         * like any other, setting the msgtype to be HYPERDEX_RESPONSE.
         * pending ops are responsible for whatever needs to happen given
         * that information.
         *
         * If there is a failure, we put the pending server pair in the failed_ops
         * with the server ID set to the hyperdex client poll file descriptor.
         */ 

        if (hyperdex_message)
        {
            hyperdex_client_returncode hstatus;
            int64_t reqid = m_hyperdex_client.loop(-1, &hstatus);
            pending_map_t::iterator it = m_pending_hyperdex_ops.find(reqid);

            if (it == m_pending_hyperdex_ops.end())
            {
                //std::cout << hstatus << " FROM HYPERDEX." << std::endl;
                TRACE;continue;
            }

            const pending_server_pair psp(it->second);
            e::intrusive_ptr<pending_aggregation> op = psp.op;
            m_pending_hyperdex_ops.erase(it);

            if (!op->handle_hyperdex_message(this, reqid, hstatus,
                                    status, &m_last_error))
            {
                TRACE;
                return -1;
            }

            if (wait_for > 0 && wait_for != op->client_visible_id())
            {
                m_yieldable.insert(std::make_pair(op->client_visible_id(), op)); 
            }
            else
            {
                m_yielding = op;
            }
 
            TRACE;continue;
        }

        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        uint8_t mt;
        int64_t nonce;
        up = up >> mt >> nonce;

        //std::cout << "NONCE IS " << nonce << std::endl;
        if (up.error())
        {
            ERROR(SERVERERROR) << "communication error: server "
                               << sid_num << " sent message="
                               << msg->as_slice().hex()
                               << " with invalid header";
            TRACE;
            return -1;
        }

        wtf_network_msgtype msg_type = static_cast<wtf_network_msgtype>(mt);
        pending_map_t::iterator it = m_pending_ops.find(nonce);

        if (it == m_pending_ops.end())
        {
            TRACE;continue;
        }

        const pending_server_pair psp(it->second);
        e::intrusive_ptr<pending_aggregation> op = psp.op;
        m_pending_ops.erase(it);

        if (msg_type == CONFIGMISMATCH)
        {
            m_failed.push_back(psp);
            TRACE;continue;
        }

        if (id == psp.si &&
            m_coord.config()->exists(id))
        {
            if (!op->handle_wtf_message(this, id, 
                                    msg, up, status, &m_last_error))
            {
                TRACE;
                return -1;
            }

            if (wait_for > 0 && wait_for != op->client_visible_id())
            {
                m_yieldable.insert(std::make_pair(op->client_visible_id(), op)); 
            }
            else
            {
                m_yielding = op;
            }
                
        }
        else if(id != psp.si)
        {
            ERROR(SERVERERROR) << "wrong server replied for nonce=" << nonce
                               << ": expected it to come from "
                               << psp.si
                               << "; it came from "
                               << id;
            TRACE; 
            return -1;
        }
        else
        {
            ERROR(SERVERERROR) << "Server that replied for nonce=" << nonce
                               << "is not in our configuration.";
        }
    }

    ERROR(NONEPENDING) << "no outstanding operations to process";
    TRACE;
    return -1;
}

int64_t
client :: open(const char* path, int flags, mode_t mode, size_t num_replicas, size_t block_size,
               int64_t* fd, wtf_client_returncode* status)
{
	TRACE;

    /* 
    open will try to get metadata from hyperdex or create a new file object and put to
    hyperdex.  It returns the operation ID, and when the operation is complete, the int64_t
    pointed to by fd will be populated by a valid file descriptor for the file.
    */
    int64_t client_id = m_next_client_id++;
    *fd= m_next_fileno++;

    if (!maintain_coord_connection(status))
    {
        TRACE;
        return -1;
    }

    char abspath[PATH_MAX]; 

    if (canon_path(path, abspath, PATH_MAX) != 0)
    {
        TRACE;
        return -1;
    }


    if (flags & O_CREAT)
    {
        //std::cout << path << std::endl;
        e::intrusive_ptr<file> f = new file(abspath, num_replicas, block_size);
        m_fds[*fd] = f; 
        f->flags = flags;
        f->mode = mode;
        struct passwd *pass = getpwuid(getuid()); 
        struct group *grp = getgrgid(pass->pw_gid);
        f->owner = std::string(pass->pw_name);
        f->group = std::string(grp->gr_name); //XXX: get actual group name
        e::intrusive_ptr<pending_creat> op = new pending_creat(this, client_id, status, f, fd);
        op->try_op();
        return client_id;
    }
    else
    {
        e::intrusive_ptr<file> f = new file(abspath, num_replicas, block_size);
        m_fds[*fd] = f; 
        f->flags = flags;

        e::intrusive_ptr<pending_open> op = new pending_open(this, client_id, status, f, fd);
        op->try_op();
        return client_id;
    }
}

int64_t
client :: unlink(const char* path, wtf_client_returncode* status)
{
	TRACE;

    hyperdex_client_returncode hstatus;
    char abspath[PATH_MAX]; 

    if (canon_path(path, abspath, PATH_MAX) != 0)
    {
        return -1;
    }

    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_del(this, client_id, std::string(abspath), status);

    if (op->try_op())
    {
        return client_id;
    }
    else
    {
        return -1;
    }
}

int64_t
client :: rename(const char* src, const char* dst, wtf_client_returncode* status)
{
	TRACE;

    char src_abspath[PATH_MAX];

    if (canon_path(src, src_abspath, PATH_MAX) != 0)
    {
        return -1;
    }

    char dst_abspath[PATH_MAX]; 

    if (canon_path(dst, dst_abspath, PATH_MAX) != 0)
    {
        return -1;
    }

    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_rename> op;
    op = new pending_rename(this, client_id, status, src_abspath, dst_abspath);

    if (op->try_op())
    {
        return client_id;
    }
    else
    {
        return -1;
    }

}

void
client :: begin_tx()
{
	TRACE;
    //proprietary HyperDex Warp code here.
}

int64_t
client :: end_tx()
{
	TRACE;
    //proprietary HyperDex Warp code here.
}

int64_t
client :: lseek(int64_t fd, uint64_t offset, int whence, wtf_client_returncode* status)
{
	TRACE;

    if (m_fds.find(fd) == m_fds.end())
    {
        ERROR(BADF) << "file descriptor " << fd << " is invalid.";
        return -1;
    }


    e::intrusive_ptr<file> f = m_fds[fd];

    switch (whence)
    {
        case SEEK_SET:
            f->set_offset(offset);
            return f->offset();
        case SEEK_CUR:
            f->set_offset(f->offset() + offset);
            return f->offset();
        case SEEK_END:
            f->set_offset(f->length() + offset);
            return f->offset();
    }
}

int64_t
client :: write(int64_t fd, const char* buf,
                   size_t * buf_sz, 
                   wtf_client_returncode* status)
{
	TRACE;
    if (m_fds.find(fd) == m_fds.end())
    {
        ERROR(BADF) << "file descriptor " << fd << " is invalid.";
        return -1;
    }

    e::intrusive_ptr<file> f = m_fds[fd];

    /* The op object here is created once and a reference to it
     * is inserted into the m_pending list for each send operation,
     * which is called from perform_aggregation.  The pending_aggregation
     * also has an internal list of server_ids it is waiting to hear back
     * from, which is also appended to for each send op. As servers return
     * acknowledgements, items are removed from both lists.  When the last
     * server_id is removed from the list inside the op, it will be marked
     * as can_yield, which will cause the loop() operation to return the
     * client_id of the op. */
    
    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_write> op;

    size_t rem = *buf_sz;
    size_t next_buf_offset = 0;

    e::intrusive_ptr<buffer_descriptor> bd(new buffer_descriptor(buf, 0));

    while(rem > 0)
    {
        std::vector<block_location> bl;
        size_t buf_offset = next_buf_offset;
        uint32_t block_offset;
        uint32_t block_capacity;
        uint64_t file_offset;
        size_t slice_len;
        prepare_write_op(f, rem, bl, next_buf_offset, block_offset, block_capacity, file_offset, slice_len);
        e::slice data = e::slice(buf+ buf_offset, slice_len);
        op = new pending_write(this, client_id, f, data, bl, block_offset, block_capacity, file_offset, bd, status);
        bd->add_op();
        f->add_pending_op(client_id);
        bool result = op->try_op();
    }

    return client_id;
}


int64_t
client :: read(int64_t fd, char* buf,
                   size_t* buf_sz,
                   wtf_client_returncode* status)
{
	TRACE;

    if (!maintain_coord_connection(status))
    {
        return -1;
    }


    if (m_fds.find(fd) == m_fds.end())
    {
        ERROR(BADF) << "file descriptor " << fd << " is invalid.";
        return -1;
    }


    e::intrusive_ptr<file> f = m_fds[fd];

    /* The op object here is created once and a reference to it
     * is inserted into the m_pending list for each send operation,
     * which is called from perform_aggregation.  The pending_aggregation
     * also has an internal list of server_ids it is waiting to hear back
     * from, which is also appended to for each send op. As servers return
     * acknowledgements, items are removed from both lists.  When the last
     * server_id is removed from the list inside the op, it will be marked
     * as can_yield, which will cause the loop() operation to return the
     * client_id of the op. */
    
    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_read(this, client_id, f, buf, buf_sz, status);
    f->add_pending_op(client_id);

    if (op->try_op())
    {
        return client_id;
    }
    else
    {
        return -1;
    }	

}

void
client :: prepare_read_op(e::intrusive_ptr<file> f, 
                              size_t& rem, 
                              size_t& buf_offset,
                              block_location& bl, 
                              uint32_t& block_length,
                              e::intrusive_ptr<pending_read> op, 
                              std::vector<server_id>& servers)
{
	TRACE;
    block_length = f->current_block_length();
    size_t block_offset = f->current_block_offset(); 
    bl = f->current_block_location();
    servers.push_back(server_id(bl.si));

    size_t advance = f->advance_to_end_of_block(rem);
    op->set_offset(bl.si, bl.bi, buf_offset, block_offset, advance);

    buf_offset += advance;
    rem -= advance;
}

int64_t
client :: close(int64_t fd, wtf_client_returncode* status)
{
	TRACE;
    if(m_fds.find(fd) == m_fds.end())
    {
        ERROR(BADF) << "file descriptor " << fd << " is invalid.";
        return -1;
    }

    e::intrusive_ptr<file> f = m_fds[fd];

    int64_t retval = 0;

    while (!f->pending_ops_empty())
    {
        int64_t client_id = f->pending_ops_pop_front();

        /*
         * This will only return a positive number if the
         * op was actually found and yielded successfully.
         *
         * We have to keep looping until we get NONPENDING
         * to ensure that ops that have multiple returns
         * complete.  If we timeout, we assume something
         * has gone horribly wrong and return failure.
         */

        int64_t ret = inner_loop(-1, status, client_id);

        if (ret < 0 && *status == WTF_CLIENT_NONEPENDING) 
        {
            //std::cout << "ret = " << ret << " status = " << status << std::endl;
            continue;
        }
        else if (ret < 0)
        {
            ERROR(IO) << "there was an IO error somewhere."; 
            retval = -1;
        }
        //XXX: return bad status if bad
    }

    if (retval >= 0) 
    {
        *status = WTF_CLIENT_SUCCESS;
    }

    return retval;
}

int64_t
client :: truncate(int fd, off_t length, wtf_client_returncode* status)
{
    //XXX: implement truncate.
    TRACE;

    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    if (m_fds.find(fd) == m_fds.end())
    {
        ERROR(BADF) << "file descriptor " << fd << " is invalid.";
        return -1;
    }

    e::intrusive_ptr<file> f = m_fds[fd];

    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_truncate(this, client_id, f, length, status);

    if (op->try_op())
    {
        return client_id;
    }
    else
    {
        return -1;
    }	
    return 0;
}

int64_t
client :: canon_path(const char* rel, char* abspath, size_t abspath_sz)
{
	TRACE;
    wtf_client_returncode lstatus;
    wtf_client_returncode* status = &lstatus;

    char* start = abspath;
    char* end = abspath + abspath_sz - 2;
    int rel_len = strnlen(rel, PATH_MAX);
    if (rel[rel_len] != '\0')
    {
        ERROR(NAMETOOLONG) << "path name is too long.";
        return -1;
    }
        
    if (*rel != '/')
    {
        getcwd(abspath, abspath_sz);
        abspath+=strlen(abspath);
        if (abspath[-1] != '/')
        {
            *abspath++ = '/';
        }
    }
    else
    {
        *abspath++ = '/';
    }

    //Invariant: abspath contains an abspatholute path
    
    while(rel[0] != '\0')
    {
        switch(rel[0])
        {

            case '/':
                ++rel;
                continue;
            case '.':
                if (rel[1] == '.' && (rel[2] == '/' || rel[2] == '\0'))
                {
                    rel += 2;

                    if (start == abspath - 1)
                    { 
                        //don't go back past the root directory.
                        continue;
                    }

                    while ((--abspath)[-1] != '/')
                    {
                        //move back one in abspath.
                    }

                    continue;
                }

                if (rel[1] == '/' || rel[1] == '\0')
                {
                    ++rel;
                    continue;
                }

            default:
                while (*rel != '/' && *rel != '\0')
                {
                    *abspath++ = *rel++;
                    if (abspath == end)
                    {
                        ERROR(NAMETOOLONG) << "path name is too long.";
                        return -1;
                    }
                }
                *abspath++ = '/';
        }
    }

    if (abspath[-1] == '/' && abspath != start + 1)
    {
        --abspath;
    }
    *abspath = '\0';
    return 0;
}

int64_t
client :: getcwd(char* c, size_t len)
{
	TRACE;
    wtf_client_returncode lstatus;
    wtf_client_returncode* status = &lstatus;

    if (len < m_cwd.size() + 1)
    {
        ERROR(NAMETOOLONG) << "buffer too small for cwd.";
        return -1;
    }

    memcpy(c, m_cwd.data(), m_cwd.size());
    c[m_cwd.size()] = '\0';

    return 0;
}

int64_t
client :: getattr(const char* path, struct wtf_file_attrs* fa, wtf_client_returncode* status)
{
    TRACE;
    int64_t client_id = m_next_client_id++;

    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    char abspath[PATH_MAX]; 

    if (canon_path(path, abspath, PATH_MAX) != 0)
    {
        return -1;
    }

    e::intrusive_ptr<file> f = new file(abspath, 0, 0);
    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_getattr(this, client_id, abspath, status, fa);
    if (op->try_op())
    {
        return client_id;
    }
    else
    {
        return -1;
    }	
}

int64_t
client :: chdir(const char* path, wtf_client_returncode* status)
{
    TRACE;
    int64_t client_id = m_next_client_id++;

    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    char abspath[PATH_MAX]; 

    if (canon_path(path, abspath, PATH_MAX) != 0)
    {
        return -1;
    }

    e::intrusive_ptr<pending_aggregation> op;
    op = new pending_chdir(this, client_id, abspath, status);
    if (op->try_op())
    {
        return client_id;
    }
    else
    {
        return -1;
    }	
}

int64_t
client :: chmod(const char* path, mode_t mode, wtf_client_returncode* status)
{
	TRACE;

    char abspath[PATH_MAX]; 

    if (canon_path(path, abspath, PATH_MAX) != 0)
    {
        return -1;
    }

    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_chmod> op;
    op = new pending_chmod(this, client_id, status, std::string(abspath), mode);

    if (op->try_op())
    {
        return client_id;
    }
    else
    {
        return -1;
    }
}

int64_t
client :: mkdir(const char* path, mode_t mode, wtf_client_returncode* status)
{
	TRACE;

    char abspath[PATH_MAX]; 

    if (canon_path(path, abspath, PATH_MAX) != 0)
    {
        return -1;
    }

    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_mkdir> op;
    op = new pending_mkdir(this, client_id, status, std::string(abspath), mode);

    if (op->try_op())
    {
        return client_id;
    }
    else
    {
        return -1;
    }

}

int64_t 
client :: readdir(const char* path, char** entry, wtf_client_returncode* status) 
{
	TRACE;

    char abspath[PATH_MAX]; 
    canon_path(path, abspath, PATH_MAX);
   
    int64_t client_id = m_next_client_id++;
    e::intrusive_ptr<pending_readdir> op;
    op = new pending_readdir(this, client_id, status, abspath, entry);

    if (op->try_op())
    {
        return client_id;
    }
    else
    {
        return -1;
    }

    return 0;
}

std::vector<std::string>
client :: ls(const char* path)
{
    TRACE;

    hyperdex_client_returncode hstatus;

    char abspath[PATH_MAX]; 

    if (canon_path(path, abspath, PATH_MAX) != 0)
    {
        return std::vector<std::string>();
    }

    std::string query("^");
    query += std::string(abspath);

    struct hyperdex_client_attribute_check check;
    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz = 0;

    check.attr = "path";
    check.value = query.c_str();
    check.value_sz = query.size();
    check.datatype = HYPERDATATYPE_STRING;
    check.predicate = HYPERPREDICATE_REGEX;
    int64_t retval = m_hyperdex_client.search("wtf", &check, 1, &hstatus, &attrs, &attrs_sz);

    std::vector<std::string> output;

    while (hstatus != HYPERDEX_CLIENT_SEARCHDONE &&
           hstatus != HYPERDEX_CLIENT_NONEPENDING)
    {
        hyperdex_client_returncode lstatus;
        retval = m_hyperdex_client.loop(-1, &lstatus);
        if (retval > 0)
        {
            for (size_t i = 0; i < attrs_sz; ++i)
            {
                if (strcmp(attrs[i].attr, "path") == 0)
                {
                    if (attrs[i].value_sz == 0)
                    {
                        continue;
                    }

                    output.push_back(std::string(attrs[i].value, attrs[i].value_sz));
                    attrs_sz = 0;
                }
           }
        }
    }

    return output;
}

/* BOILERPLATE */
void
client :: handle_disruption(const server_id& si)
{
	TRACE;
    pending_map_t::iterator it = m_pending_ops.begin();

    while (it != m_pending_ops.end())
    {
        if (it->second.si == si)
        {
            m_failed.push_back(it->second);
            pending_map_t::iterator tmp = it;
            ++it;
            m_pending_ops.erase(tmp);
        }
        else
        {
            ++it;
        }
    }

    m_busybee.drop(si.get());
}

const char*
client :: error_message()
{
	TRACE;
    return m_last_error.msg();
}

const char*
client :: error_location()
{
	TRACE;
    return m_last_error.loc();
}

void
client :: set_error_message(const char* msg)
{
	TRACE;
    m_last_error = e::error();
    m_last_error.set_loc(__FILE__, __LINE__);
    m_last_error.set_msg() << msg;
}

WTF_API std::ostream&
operator << (std::ostream& lhs, wtf_client_returncode rhs)
{
	TRACE;
    switch (rhs)
    {
        STRINGIFY(WTF_CLIENT_SUCCESS);
        STRINGIFY(WTF_CLIENT_NOTFOUND);
        STRINGIFY(WTF_CLIENT_INVALID);
        STRINGIFY(WTF_CLIENT_BADF);
        STRINGIFY(WTF_CLIENT_READONLY);
        STRINGIFY(WTF_CLIENT_NAMETOOLONG);
        STRINGIFY(WTF_CLIENT_EXIST);
        STRINGIFY(WTF_CLIENT_ISDIR);
        STRINGIFY(WTF_CLIENT_NOTDIR);
        STRINGIFY(WTF_CLIENT_UNKNOWNSPACE);
        STRINGIFY(WTF_CLIENT_COORDFAIL);
        STRINGIFY(WTF_CLIENT_SERVERERROR);
        STRINGIFY(WTF_CLIENT_POLLFAILED);
        STRINGIFY(WTF_CLIENT_OVERFLOW);
        STRINGIFY(WTF_CLIENT_RECONFIGURE);
        STRINGIFY(WTF_CLIENT_TIMEOUT);
        STRINGIFY(WTF_CLIENT_NONEPENDING);
        STRINGIFY(WTF_CLIENT_NOMEM);
        STRINGIFY(WTF_CLIENT_BADCONFIG);
        STRINGIFY(WTF_CLIENT_DUPLICATE);
        STRINGIFY(WTF_CLIENT_INTERRUPTED);
        STRINGIFY(WTF_CLIENT_CLUSTER_JUMP);
        STRINGIFY(WTF_CLIENT_COORD_LOGGED);
        STRINGIFY(WTF_CLIENT_BACKOFF);
        STRINGIFY(WTF_CLIENT_IO);
        STRINGIFY(WTF_CLIENT_INTERNAL);
        STRINGIFY(WTF_CLIENT_EXCEPTION);
        STRINGIFY(WTF_CLIENT_GARBAGE);
        default:
            lhs << "unknown wtf_client_returncode";
            return lhs;
    }
}


/* Sync ops */
int64_t
client :: getattr_sync(const char* path, struct wtf_file_attrs* fa, wtf_client_returncode* status)
{
    int64_t reqid = getattr(path, fa, status);
    if (reqid < 0)
    {
        return reqid;
    }

    int64_t lreqid = loop(reqid, -1, status);
    if (lreqid < 0)
    {
        return lreqid;
    }

    return lreqid;

}

int64_t
client :: write_sync(int64_t fd, const char* buf,
                   size_t * buf_sz, 
                   wtf_client_returncode* status)
{
    if (*buf_sz == 0)
    {
        return 0;
    }

    int64_t reqid = write(fd, buf, buf_sz, status);
    if (reqid < 0)
    {
        return reqid;
    }

    int64_t lreqid = loop(reqid, -1, status);
    if (lreqid < 0)
    {
        return lreqid;
    }

    return lreqid;
}

int64_t
client :: read_sync(int64_t fd, char* buf,
                   size_t* buf_sz,
                   wtf_client_returncode* status)
{
    if (*buf_sz == 0)
    {
        return 0;
    }

    int64_t reqid = read(fd, buf, buf_sz, status);
    if (reqid < 0)
    {
        return reqid;
    }

    int64_t lreqid = loop(reqid, -1, status);
    if (lreqid < 0)
    {
        return lreqid;
    }

    return reqid;
}

void
client :: add_hyperdex_op(int64_t reqid, pending_aggregation* pending_op)
{
    server_id HYPERDEX = server_id(m_hyperdex_client.poll_fd());
    e::intrusive_ptr<pending_aggregation> op = pending_op;
    pending_server_pair psp(HYPERDEX, op);
    m_pending_hyperdex_ops.insert(std::make_pair(reqid, psp));
}
