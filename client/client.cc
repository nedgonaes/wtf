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
#include "client/pending_write.h"
#include "client/pending_read.h"
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

using wtf::client;

client :: client(const char* host, in_port_t port,
                         const char* hyper_host, in_port_t hyper_port)
    : m_coord(host, port)
    , m_busybee_mapper(m_coord.config())
    , m_busybee(&m_busybee_mapper, busybee_generate_id())
    , m_next_client_id(1)
    , m_next_server_nonce(1)
    , m_pending_ops()
    , m_failed()
    , m_yielding()
    , m_yielded()
    , m_last_error()
    , m_hyperdex_client(hyper_host, hyper_port)
    , m_next_fileno(1)
    , m_fds()
    , m_cwd("/")
{
}

client :: ~client() throw ()
{
}

bool
client :: maintain_coord_connection(wtf_client_returncode* status)
{
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


bool
client :: send(wtf_network_msgtype mt,
               const server_id& to,
               uint64_t nonce,
               std::auto_ptr<e::buffer> msg,
               e::intrusive_ptr<pending> op,
               wtf_client_returncode* status)
{
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
            op->handle_sent_to(to);
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
    return inner_loop(timeout, status, client_id);
}

int64_t
client :: loop(int timeout, wtf_client_returncode* status)
{
    return inner_loop(timeout, status, -1);
}

int64_t
client :: inner_loop(int timeout, wtf_client_returncode* status, int64_t wait_for)
{
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
           !m_yieldable.empty())
    {
        /* Handle currently yielding operation first. */
        if (m_yielding)
        {
            if (!m_yielding->can_yield())
            {
                m_yielding = NULL;
                continue;
            }

            if (!m_yielding->yield(status, &m_last_error))
            {
                return -1;
            }

            int64_t client_id = m_yielding->client_visible_id();
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
            }

            return client_id;
        }

        /* Handle failed operations second */
        else if (!m_failed.empty())
        {
            const pending_server_pair& psp(m_failed.front());
            e::intrusive_ptr<pending> op = psp.op;
            op->handle_failure(psp.si);
            
            if (wait_for > 0 && wait_for != op->client_visible_id())
            {
                m_yieldable.insert(std::make_pair(op->client_visible_id(), op)); 
            }
            else
            {
                m_yielding = op;
            }

            m_failed.pop_front();
            continue;
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
                continue;
            }
        }
        else if(!m_yieldable.empty())
        {
            yieldable_map_t::iterator it = m_yieldable.begin();
            m_yielding = it->second;
            m_yieldable.erase(it);
            continue;
        }


        /* Handle a new pending op. */
        assert(!m_pending_ops.empty());

        if (!maintain_coord_connection(status))
        {
            return -1;
        }

        uint64_t sid_num;
        std::auto_ptr<e::buffer> msg;
        m_busybee.set_timeout(timeout);
        busybee_returncode rc = m_busybee.recv(&sid_num, &msg);
        server_id id(sid_num);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_INTERRUPTED:
                ERROR(INTERRUPTED) << "signal received";
                return -1;
            case BUSYBEE_TIMEOUT:
                ERROR(TIMEOUT) << "operation timed out";
                return -1;
            case BUSYBEE_DISRUPTED:
                handle_disruption(id);
                continue;
            case BUSYBEE_EXTERNAL:
                continue;
            BUSYBEE_ERROR_CASE(POLLFAILED);
            BUSYBEE_ERROR_CASE(ADDFDFAIL);
            BUSYBEE_ERROR_CASE(SHUTDOWN);
            default:
                ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned "
                                << (unsigned) rc << ": please file a bug";
                return -1;
        }

        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        uint8_t mt;
        int64_t nonce;
        up = up >> mt >> nonce;

        if (up.error())
        {
            ERROR(SERVERERROR) << "communication error: server "
                               << sid_num << " sent message="
                               << msg->as_slice().hex()
                               << " with invalid header";
            return -1;
        }

        wtf_network_msgtype msg_type = static_cast<wtf_network_msgtype>(mt);
        pending_map_t::iterator it = m_pending_ops.find(nonce);

        if (it == m_pending_ops.end())
        {
            continue;
        }

        const pending_server_pair psp(it->second);
        e::intrusive_ptr<pending> op = psp.op;
        m_pending_ops.erase(it);

        if (msg_type == CONFIGMISMATCH)
        {
            m_failed.push_back(psp);
            continue;
        }

        if (id == psp.si &&
            m_coord.config()->exists(id))
        {
            if (!op->handle_message(this, id, msg_type, 
                                    msg, up, status, &m_last_error))
            {
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
            return -1;
        }
        else
        {
            ERROR(SERVERERROR) << "Server that replied for nonce=" << nonce
                               << "is not in our configuration.";
        }
    }

    ERROR(NONEPENDING) << "no outstanding operations to process";
    return -1;
}

int64_t
client :: open(const char* path, int flags, mode_t mode, size_t num_replicas)
{
    wtf_client_returncode lstatus;
    wtf_client_returncode* status = &lstatus;

    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    e::intrusive_ptr<file> f = new file(path);
    m_fds[m_next_fileno] = f; 
    
    f->flags = flags;
    f->mode = mode;
    std::cout << "OPEN MODE: " << mode << std::endl;

    if (flags & O_CREAT)
    {
        f->set_replicas(num_replicas);
        update_file_cache(path, f, true);
        update_hyperdex(f);
    }
    else
    {
        update_file_cache(path, f, false);
    }


    return m_next_fileno++;
}

void
client :: begin_tx()
{
    //proprietary HyperDex Warp code here.
}

int64_t
client :: end_tx()
{
    //proprietary HyperDex Warp code here.
}

void
client :: lseek(int64_t fd, uint64_t offset)
{
    m_fds[fd]->set_offset(offset);
}

int64_t
client :: perform_aggregation(const std::vector<server_id>& servers,
                              e::intrusive_ptr<pending_aggregation> _op,
                              wtf_network_msgtype mt,
                              std::auto_ptr<e::buffer> msg,
                              wtf_client_returncode* status)
{
    e::intrusive_ptr<pending> op(_op.get());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        uint64_t nonce = m_next_server_nonce++;
        pending_server_pair psp(servers[i], op);
        std::auto_ptr<e::buffer> msg_copy(msg->copy());

        if (!send(mt, psp.si, nonce, msg_copy, op, status))
        {
            m_failed.push_back(psp);
        }
    }

    return op->client_visible_id();
}

int64_t
client :: write(int64_t fd, const char* buf,
                   size_t * buf_sz, 
                   wtf_client_returncode* status)
{
    if (m_fds.find(fd) == m_fds.end())
    {
        ERROR(BADF) << "file descriptor " << fd << " is invalid.";
        return -1;
    }

    e::intrusive_ptr<file> f = m_fds[fd];

    //XXX
    update_file_cache(f->path().get(), f, false);

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
    op = new pending_write(client_id, status);

    size_t rem = *buf_sz;
    size_t next_buf_offset = 0;

    while(rem > 0)
    {
        std::vector<block_location> bl;
        size_t buf_offset = next_buf_offset;
        uint32_t block_offset;
        size_t slice_len;
        prepare_write_op(f, rem, bl, next_buf_offset, block_offset, slice_len);
        e::slice data = e::slice(buf + buf_offset, slice_len);

        for (size_t i = 0; i < bl.size(); ++i)
        {
            size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
                + sizeof(uint64_t) // bl.bi (remote block number) 
                + sizeof(uint32_t) // block_offset (remote block offset) 
                + data.size();     // user data 
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            e::buffer::packer pa = msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ);
                pa = pa << bl[i].bi << block_offset;
            pa.copy(data);

            if (!maintain_coord_connection(status))
            {
                return -1;
            }

            std::vector<server_id> servers;
            servers.push_back(server_id(bl[i].si));
            perform_aggregation(servers, op, REQ_UPDATE, msg, status);
        }
    }

    return client_id;
}

void
client :: prepare_write_op(e::intrusive_ptr<file> f, 
                              size_t& rem, 
                              std::vector<block_location>& bl,
                              size_t& buf_offset,
                              uint32_t& block_offset,
                              size_t& slice_len)
{
    f->copy_current_block_locations(bl);
    block_offset = f->current_block_offset();
    slice_len = f->advance_to_end_of_block(rem);
    buf_offset += slice_len;
    rem -= slice_len;
}


int64_t
client :: read(int64_t fd, char* buf,
                   size_t* buf_sz,
                   wtf_client_returncode* status)
{
    if (m_fds.find(fd) == m_fds.end())
    {
        ERROR(BADF) << "file descriptor " << fd << " is invalid.";
        return -1;
    }

    e::intrusive_ptr<file> f = m_fds[fd];

    //XXX
    update_file_cache(f->path().get(), f, false);

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
    e::intrusive_ptr<pending_read> op;
    op = new pending_read(client_id, status, buf, buf_sz);

    size_t rem = std::min(*buf_sz, f->bytes_left_in_file());
    size_t buf_offset = 0;

    while(rem > 0)
    {
        block_location bl;
        uint32_t block_length;
        std::vector<server_id> servers;
        prepare_read_op(f, rem, buf_offset, bl, block_length, op, servers);
        size_t sz = WTF_CLIENT_HEADER_SIZE_REQ
                  + sizeof(uint64_t); // bl.bi (local block number) 
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(WTF_CLIENT_HEADER_SIZE_REQ) << bl.bi << block_length;

        if (!maintain_coord_connection(status))
        {
            return -1;
        }

        perform_aggregation(servers, op.get(), REQ_GET, msg, status);
    }

    return client_id;
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

        while (true)
        {
            int64_t ret = inner_loop(-1, status, client_id);

            if (ret < 0 && *status == WTF_CLIENT_NONEPENDING) 
            {
                break;
            }
            else if (ret < 0)
            {
                ERROR(IO) << "there was an IO error somewhere.";
                retval = -1;
            }
        }
    }

    return retval;
}

int64_t
client :: update_file_cache(const char* path, e::intrusive_ptr<file>& f, bool create)
{
    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    int64_t ret = 0;
    hyperdex_client_returncode hstatus;
    wtf_client_returncode lstatus;
    wtf_client_returncode* status = &lstatus;

    ret = m_hyperdex_client.get("wtf", path, strlen(path), &hstatus, &attrs, &attrs_sz);
    if (ret == -1)
    {
        ERROR(INTERNAL) << " failed to update file cache.  HyperDex get failed with " << ret;
        return -1;
    }

    hyperdex_client_returncode res = hyperdex_wait_for_result(ret, hstatus);

    if (res == HYPERDEX_CLIENT_NOTFOUND)
    {
        if (!create)
        {
            ERROR(NOTFOUND) << "path not found in HyperDex.";
            return -1;
        }

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
                    uint32_t idlen;
                    uint64_t id;
                    uint32_t valuelen;
                    up = up >> idlen >> id >> valuelen;
                    e::unpack64be((uint8_t*)&id, &id);
                    e::unpack32be((uint8_t*)&valuelen, &valuelen);
                    e::intrusive_ptr<wtf::block> b = new wtf::block();
                    up = up >> b;
                    f->update_blocks(id, b);
                }
            }
            else if (strcmp(attrs[i].attr, "directory") == 0)
            {
                std::cout << "unpacking directory" << std::endl;
                uint64_t is_dir;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> is_dir;
                e::unpack64be((uint8_t*)&is_dir, &is_dir);

                if (is_dir == 0)
                {
                    std::cout << path << " is not a dir " << std::endl;
                    f->is_directory = false;
                }
                else
                {
                    std::cout << path << " is a dir " << std::endl;
                    f->is_directory = true;
                }
            }
            else if (strcmp(attrs[i].attr, "mode") == 0)
            {
                std::cout << "unpacking mode" << std::endl;
                uint64_t mode;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> mode;
                e::unpack64be((uint8_t*)&mode, &mode);

                std::cout << "MODE: " << mode << std::endl;

                f->mode = mode;
            }
        }
    }
    
    return ret;
}

int64_t
client :: truncate(int fd, off_t length)
{
    /*
    size_t sz;
    e::intrusive_ptr<file> f = m_fds[fd];

    if (length == f->length())
    {
        return 0;
    }

    if (length > f->length())
    {
        sz = length - f->length();
        //cout << "expanding to sz " << sz << endl;
        wtf_client_returncode status;
        char* data = new char[sz];
        memset(data, 0, sz);
        write(fd, data, sz, 3, &status);
        flush(fd, &status);
    }
    else
    {
        //cout << "truncating" << endl;
        f->truncate(length); 
    }

    update_hyperdex(f);
    cout << "updated" << endl;
*/
    return 0;
}

int64_t
client :: canon_path(char* rel, char* abspath, size_t abspath_sz)
{
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
client :: getattr(const char* path, struct wtf_file_attrs* fa)
{
    wtf_client_returncode status;

    int64_t fd = open(path, O_RDONLY, 0, 0);
    if (fd < 0)
    {
        return -1;
    }

    e::intrusive_ptr<file> f = m_fds[fd];
    fa->is_dir = f->is_directory ? 1 : 0;
    fa->size = f->length();
    fa->mode = f->mode;
    fa->flags = f->flags;
    close(fd, &status);
    return 0;
}

int64_t
client :: chdir(char* path)
{
    char *abspath = new char[PATH_MAX]; 
    if (canon_path(path, abspath, PATH_MAX) != 0)
    {
        return -1;
    }

    const struct hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    int64_t ret = 0;
    hyperdex_client_returncode hstatus;
    wtf_client_returncode lstatus;
    wtf_client_returncode* status = &lstatus;

    ret = m_hyperdex_client.get("wtf", abspath, strlen(abspath), &hstatus, &attrs, &attrs_sz);
    if (ret == -1)
    {
        return -1;
    }

    hyperdex_client_returncode res = hyperdex_wait_for_result(ret, hstatus);

    if (res == HYPERDEX_CLIENT_NOTFOUND)
    {
        ERROR(NOTFOUND) << "path " << abspath << " not found in HyperDex.";
        return -1;
    }
    else
    {
        for (size_t i = 0; i < attrs_sz; ++i)
        {
            if (strcmp(attrs[i].attr, "directory") == 0)
            {
                uint64_t is_dir;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> is_dir;

                if (is_dir == 0)
                {
                    errno = ENOTDIR;
                    return -1;
                }
            }
            else if (strcmp(attrs[i].attr, "mode") == 0)
            {
                uint64_t mode;

                e::unpacker up(attrs[i].value, attrs[i].value_sz);
                up = up >> mode;
                //XXX: implement owner, group, etc and deny if no
                //     read permissions.
            }
        }
    }
    
    m_cwd = abspath;
    return 0;
}

int64_t
client :: chmod(const char* path, mode_t mode)
{
    hyperdex_client_returncode status;
    int64_t ret = -1;
    uint64_t val = mode;
    struct hyperdex_client_attribute attr;
    attr.attr = "mode";
    attr.value = (const char*)&val;
    attr.value_sz = sizeof(val);
    attr.datatype = HYPERDATATYPE_INT64;

 
    ret = m_hyperdex_client.put("wtf", path, strlen(path), &attr, 1, &status);
    hyperdex_client_returncode res = hyperdex_wait_for_result(ret, status);

    if (res != HYPERDEX_CLIENT_SUCCESS)
    {
        return -1;
    }
    else
    {
        return 0;
    }

}

int64_t
client :: mkdir(const char* path, mode_t mode)
{
    hyperdex_client_returncode status;
    int64_t ret = -1;
    struct hyperdex_client_attribute attr[2];
    uint64_t val1 = 1;
    attr[0].attr = "directory";
    attr[0].value = (const char*)&val1;
    attr[0].value_sz = sizeof(val1);
    attr[0].datatype = HYPERDATATYPE_INT64;

    uint64_t val2 = mode;
    attr[1].attr = "mode";
    attr[1].value = (const char*)&val2;
    attr[1].value_sz = sizeof(val2);
    attr[1].datatype = HYPERDATATYPE_INT64;

 
    ret = m_hyperdex_client.put_if_not_exist("wtf", path, strlen(path), attr, 2, &status);
    hyperdex_client_returncode res = hyperdex_wait_for_result(ret, status);
    if (res != HYPERDEX_CLIENT_SUCCESS)
    {
        std::cerr << "mkdir returned " << res << std::endl;
        return -1;
    }
    else
    {
        return 0;
    }
}

int64_t 
client :: opendir(const char* path)
{
    int64_t fd = m_next_fileno;
    m_next_fileno;
    m_next_fileno++;
}

int64_t 
client :: closedir(int fd)
{
}

int64_t 
client :: readdir(int fd, char* entry)
{
    return 0;
}


int64_t
client :: update_hyperdex(e::intrusive_ptr<file>& f)
{
    std::cout << "updating hyperdex for file " << f->path().get() << std::endl;
    int64_t ret = -1;
    int i = 0;

    std::vector<struct hyperdex_client_map_attribute> attrs;
    hyperdex_client_returncode status;

    typedef std::map<uint64_t, e::intrusive_ptr<wtf::block> > block_map;

    //XXX; get rid of magic string.
    const char* name = "blockmap";

    uint64_t mode = f->mode;
    uint64_t directory = f->is_directory;
    struct hyperdex_client_attribute attr[2];

    attr[0].attr = "mode";
    attr[0].value = (const char*)&mode;
    attr[0].value_sz = sizeof(mode);
    attr[0].datatype = HYPERDATATYPE_INT64;

    attr[1].attr = "directory";
    attr[1].value = (const char*)&directory;
    attr[1].value_sz = sizeof(directory);
    attr[1].datatype = HYPERDATATYPE_INT64;

    ret = m_hyperdex_client.put("wtf", f->path().get(), strlen(f->path().get()), attr, 2, &status);
    if (ret < 0)
    {
        std::cout << "PUT FAILED: " << status << std::endl;
        return -1;
    }

    hyperdex_client_returncode res = hyperdex_wait_for_result(ret, status);

    if (res != HYPERDEX_CLIENT_SUCCESS)
    {
        std::cout << "UPDATE FAILED: " << res << std::endl;
        return -1;
    }


    /*
     * construct a hyperdex attribute list for all dirty blocks
     */
    for (block_map::const_iterator it = f->blocks_begin();
            it != f->blocks_end(); ++it)
    {
        if (it->second->dirty())
        {
            std::cout << "UPDATING BLOCK " << it->first << " of " << f->path().get() << std::endl;
            std::cout << "LEN: " << it->second->length() << std::endl;
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
        else
        {
            std::cout << "BLOCK " << it->first << " of " << f->path().get() << "NOT DIRTY" << std::endl;
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

    res = hyperdex_wait_for_result(ret, status);

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
client::hyperdex_wait_for_result(int64_t reqid, hyperdex_client_returncode& status)
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
client :: handle_disruption(const server_id& si)
{
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
    return m_last_error.msg();
}

const char*
client :: error_location()
{
    return m_last_error.loc();
}

void
client :: set_error_message(const char* msg)
{
    m_last_error = e::error();
    m_last_error.set_loc(__FILE__, __LINE__);
    m_last_error.set_msg() << msg;
}

WTF_API std::ostream&
operator << (std::ostream& lhs, wtf_client_returncode rhs)
{
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
