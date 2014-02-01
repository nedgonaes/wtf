// Copyright (c) 2013, Cornell University
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

// STL
#include <sstream>

// e
#include <e/endian.h>

// BusyBee
#include <busybee_constants.h>

// WTF 
#include "common/macros.h"
#include "common/serialization.h"
#include "admin/admin.h"
#include "admin/constants.h"
#include "admin/coord_rpc_generic.h"
#include "admin/pending_string.h"
#include "admin/yieldable.h"

#define ERROR(CODE) \
    *status = WTF_ADMIN_ ## CODE; \
    m_last_error.set_loc(__FILE__, __LINE__); \
    m_last_error.set_msg()

using wtf::admin;

admin :: admin(const char* coordinator, uint16_t port)
    : m_coord(coordinator, port)
    , m_busybee_mapper(m_coord.config())
    , m_busybee(&m_busybee_mapper, 0)
    , m_next_admin_id(1)
    , m_next_server_nonce(1)
    , m_handle_coord_ops(false)
    , m_coord_ops()
    , m_server_ops()
    , m_multi_ops()
    , m_failed()
    , m_yieldable()
    , m_yielding()
    , m_yielded()
    , m_last_error()
{
}

admin :: ~admin() throw ()
{
}

int64_t
admin :: dump_config(wtf_admin_returncode* status,
                     const char** config)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    std::string tmp = m_coord.config()->dump();
    e::intrusive_ptr<pending> op = new pending_string(id, status, WTF_ADMIN_SUCCESS, tmp, config);
    m_yieldable.push_back(op.get());
    return op->admin_visible_id();
}

int64_t
admin :: read_only(int ro, wtf_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    bool set = ro != 0;
    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, (set ? "set read-only" : "set read-write"));
    char buf[sizeof(uint8_t)];
    buf[0] = set ? 1 : 0;
    int64_t cid = m_coord.rpc("read_only", buf, sizeof(uint8_t),
                              &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_rpc_request_failure(op->repl_status, status);
        return -1;
    }
}

int64_t
admin :: server_register(uint64_t token, const char* address,
                         enum wtf_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    server_id sid(token);
    po6::net::location loc;

    try
    {
        loc = po6::net::location(address);
    }
    catch (po6::error& e)
    {
        ERROR(TIMEOUT) << "could not parse address";
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "register server");
    std::auto_ptr<e::buffer> msg(e::buffer::create(sizeof(uint64_t) + pack_size(loc)));
    *msg << sid << loc;
    int64_t cid = m_coord.rpc("server_register", reinterpret_cast<const char*>(msg->data()), msg->size(),
                              &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_rpc_request_failure(op->repl_status, status);
        return -1;
    }
}

int64_t
admin :: server_online(uint64_t token, enum wtf_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "bring server online");
    char buf[sizeof(uint64_t)];
    e::pack64be(token, buf);
    int64_t cid = m_coord.rpc("server_online", buf, sizeof(uint64_t),
                              &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_rpc_request_failure(op->repl_status, status);
        return -1;
    }
}

int64_t
admin :: server_offline(uint64_t token, enum wtf_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "bring server offline");
    char buf[sizeof(uint64_t)];
    e::pack64be(token, buf);
    int64_t cid = m_coord.rpc("server_offline", buf, sizeof(uint64_t),
                              &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_rpc_request_failure(op->repl_status, status);
        return -1;
    }
}

int64_t
admin :: server_forget(uint64_t token, enum wtf_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "forget server");
    char buf[sizeof(uint64_t)];
    e::pack64be(token, buf);
    int64_t cid = m_coord.rpc("server_forget", buf, sizeof(uint64_t),
                              &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_rpc_request_failure(op->repl_status, status);
        return -1;
    }
}

int64_t
admin :: server_kill(uint64_t token, enum wtf_admin_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t id = m_next_admin_id;
    ++m_next_admin_id;
    e::intrusive_ptr<coord_rpc> op = new coord_rpc_generic(id, status, "kill server");
    char buf[sizeof(uint64_t)];
    e::pack64be(token, buf);
    int64_t cid = m_coord.rpc("server_kill", buf, sizeof(uint64_t),
                              &op->repl_status, &op->repl_output, &op->repl_output_sz);

    if (cid >= 0)
    {
        m_coord_ops[cid] = op;
        return op->admin_visible_id();
    }
    else
    {
        interpret_rpc_request_failure(op->repl_status, status);
        return -1;
    }
}

int64_t
admin :: loop(int timeout, wtf_admin_returncode* status)
{
    *status = WTF_ADMIN_SUCCESS;

    while (m_yielding ||
           !m_failed.empty() ||
           !m_yieldable.empty() ||
           !m_coord_ops.empty() ||
           !m_server_ops.empty())
    {
        if (m_yielding)
        {
            if (!m_yielding->can_yield())
            {
                m_yielding = NULL;
                continue;
            }

            if (!m_yielding->yield(status))
            {
                return -1;
            }

            int64_t admin_id = m_yielding->admin_visible_id();
            m_last_error = m_yielding->error();

            if (!m_yielding->can_yield())
            {
                m_yielded = m_yielding;
                m_yielding = NULL;
            }

            multi_yieldable_map_t::iterator it = m_multi_ops.find(admin_id);

            if (it != m_multi_ops.end())
            {
                e::intrusive_ptr<multi_yieldable> op = it->second;
                m_multi_ops.erase(it);

                if (!op->callback(this, admin_id, status))
                {
                    return -1;
                }

                m_yielding = op.get();
                continue;
            }

            return admin_id;
        }
        else if (!m_yieldable.empty())
        {
            m_yielding = m_yieldable.front();
            m_yieldable.pop_front();
            continue;
        }
        else if (!m_failed.empty())
        {
            const pending_server_pair& psp(m_failed.front());
            psp.op->handle_failure(psp.si);
            m_yielding = psp.op.get();
            m_failed.pop_front();
            continue;
        }

        m_yielded = NULL;

        if (!maintain_coord_connection(status))
        {
            return -1;
        }

        assert(!m_coord_ops.empty() || !m_server_ops.empty());

        m_busybee.set_timeout(timeout);

        if (m_handle_coord_ops)
        {
            m_handle_coord_ops = false;
            replicant_returncode lrc = REPLICANT_GARBAGE;
            int64_t lid = m_coord.loop(0, &lrc);

            if (lid < 0 && lrc != REPLICANT_TIMEOUT)
            {
                interpret_rpc_loop_failure(lrc, status);
                return -1;
            }

            coord_rpc_map_t::iterator it = m_coord_ops.find(lid);

            if (it == m_coord_ops.end())
            {
                continue;
            }

            e::intrusive_ptr<coord_rpc> op = it->second;
            m_coord_ops.erase(it);

            if (!op->handle_response(this, status))
            {
                return -1;
            }

            m_yielding = op.get();
            continue;
        }

        uint64_t sid_num;
        std::auto_ptr<e::buffer> msg;
        busybee_returncode rc = m_busybee.recv(&sid_num, &msg);
        server_id id(sid_num);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
                ERROR(POLLFAILED) << "poll failed";
                return -1;
            case BUSYBEE_DISRUPTED:
                handle_disruption(id);
                continue;
            case BUSYBEE_TIMEOUT:
                ERROR(TIMEOUT) << "operation timed out";
                return -1;
            case BUSYBEE_INTERRUPTED:
                ERROR(INTERRUPTED) << "signal received";
                return -1;
            case BUSYBEE_EXTERNAL:
                m_handle_coord_ops = true;
                continue;
            case BUSYBEE_SHUTDOWN:
            default:
                abort();
        }

        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        uint8_t mt;
        server_id vfrom;
        int64_t nonce;
        up = up >> mt >> vfrom >> nonce;

        if (up.error())
        {
            ERROR(SERVERERROR) << "communication error: server "
                               << sid_num << " sent message="
                               << msg->as_slice().hex()
                               << " with invalid header";
            return -1;
        }

        wtf_network_msgtype msg_type = static_cast<wtf_network_msgtype>(mt);
        pending_map_t::iterator it = m_server_ops.find(nonce);

        if (it == m_server_ops.end())
        {
            continue;
        }

        const pending_server_pair psp(it->second);
        e::intrusive_ptr<pending> op = psp.op;

        if (msg_type == CONFIGMISMATCH)
        {
            m_failed.push_back(psp);
            continue;
        }

        if (id == it->second.si)
        {
            m_server_ops.erase(it);

            if (!op->handle_message(this, id, msg_type, msg, up, status))
            {
                return -1;
            }

            m_yielding = psp.op.get();
        }
        else
        {
            ERROR(SERVERERROR) << "server " << sid_num
                               << " responded for nonce " << nonce
                               << " which belongs to server " << it->second.si.get();
            return -1;
        }
    }

    ERROR(NONEPENDING) << "no outstanding operations to process";
    return -1;
}

const char*
admin :: error_message()
{
    return m_last_error.msg();
}

const char*
admin :: error_location()
{
    return m_last_error.loc();
}

void
admin :: set_error_message(const char* msg)
{
    m_last_error = e::error();
    m_last_error.set_loc(__FILE__, __LINE__);
    m_last_error.set_msg() << msg;
}

void
admin :: interpret_rpc_request_failure(replicant_returncode rstatus,
                                       wtf_admin_returncode* status)
{
    e::error err;

    switch (rstatus)
    {
        case REPLICANT_TIMEOUT:
            ERROR(TIMEOUT) << "operation timed out";
            break;
        case REPLICANT_INTERRUPTED:
            ERROR(INTERRUPTED) << "signal received";
            break;
        case REPLICANT_NAME_TOO_LONG:
        case REPLICANT_OBJ_NOT_FOUND:
        case REPLICANT_FUNC_NOT_FOUND:
        case REPLICANT_CLUSTER_JUMP:
            err = m_coord.error();
            ERROR(COORDFAIL) << "persistent coordinator error: " << err.msg() << " @ " << err.loc();
            break;
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_NEED_BOOTSTRAP:
        case REPLICANT_BACKOFF:
            err = m_coord.error();
            ERROR(COORDFAIL) << "transient coordinator error: " << err.msg() << " @ " << err.loc();
            break;
        case REPLICANT_SUCCESS:
        case REPLICANT_NONE_PENDING:
        case REPLICANT_INTERNAL_ERROR:
        case REPLICANT_MISBEHAVING_SERVER:
        case REPLICANT_BAD_LIBRARY:
        case REPLICANT_COND_DESTROYED:
        case REPLICANT_COND_NOT_FOUND:
        case REPLICANT_OBJ_EXIST:
        case REPLICANT_CTOR_FAILED:
        case REPLICANT_GARBAGE:
        default:
            err = m_coord.error();
            ERROR(COORDFAIL) << "internal library error: " << err.msg() << " @ " << err.loc();
            break;
    }
}

void
admin :: interpret_rpc_loop_failure(replicant_returncode rstatus,
                                    wtf_admin_returncode* status)
{
    e::error err;

    switch (rstatus)
    {
        case REPLICANT_TIMEOUT:
            ERROR(TIMEOUT) << "operation timed out";
            break;
        case REPLICANT_INTERRUPTED:
            ERROR(INTERRUPTED) << "signal received";
            break;
        case REPLICANT_NAME_TOO_LONG:
        case REPLICANT_OBJ_NOT_FOUND:
        case REPLICANT_FUNC_NOT_FOUND:
        case REPLICANT_CLUSTER_JUMP:
            err = m_coord.error();
            ERROR(COORDFAIL) << "persistent coordinator error: " << err.msg() << " @ " << err.loc();
            break;
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_NEED_BOOTSTRAP:
        case REPLICANT_BACKOFF:
            err = m_coord.error();
            ERROR(COORDFAIL) << "transient coordinator error: " << err.msg() << " @ " << err.loc();
            break;
        case REPLICANT_NONE_PENDING:
            ERROR(NONEPENDING) << "no outstanding operations to process";
            break;
        case REPLICANT_SUCCESS:
        case REPLICANT_INTERNAL_ERROR:
        case REPLICANT_MISBEHAVING_SERVER:
        case REPLICANT_BAD_LIBRARY:
        case REPLICANT_COND_DESTROYED:
        case REPLICANT_COND_NOT_FOUND:
        case REPLICANT_OBJ_EXIST:
        case REPLICANT_CTOR_FAILED:
        case REPLICANT_GARBAGE:
        default:
            err = m_coord.error();
            ERROR(COORDFAIL) << "internal library error: " << err.msg() << " @ " << err.loc();
            break;
    }
}

void
admin :: interpret_rpc_response_failure(replicant_returncode rstatus,
                                        wtf_admin_returncode* status,
                                        e::error* ret_err)
{
    e::error err;
    e::error tmp = m_last_error;
    m_last_error = e::error();

    switch (rstatus)
    {
        case REPLICANT_SUCCESS:
            *status = WTF_ADMIN_SUCCESS;
            break;
        case REPLICANT_TIMEOUT:
            ERROR(TIMEOUT) << "operation timed out";
            break;
        case REPLICANT_INTERRUPTED:
            ERROR(INTERRUPTED) << "signal received";
            break;
        case REPLICANT_NAME_TOO_LONG:
        case REPLICANT_OBJ_NOT_FOUND:
        case REPLICANT_FUNC_NOT_FOUND:
        case REPLICANT_CLUSTER_JUMP:
            err = m_coord.error();
            ERROR(COORDFAIL) << "persistent coordinator error: " << err.msg() << " @ " << err.loc();
            break;
        case REPLICANT_SERVER_ERROR:
        case REPLICANT_NEED_BOOTSTRAP:
        case REPLICANT_BACKOFF:
            err = m_coord.error();
            ERROR(COORDFAIL) << "transient coordinator error: " << err.msg() << " @ " << err.loc();
            break;
        case REPLICANT_NONE_PENDING:
            ERROR(NONEPENDING) << "no outstanding operations to process";
            break;
        case REPLICANT_INTERNAL_ERROR:
        case REPLICANT_MISBEHAVING_SERVER:
        case REPLICANT_BAD_LIBRARY:
        case REPLICANT_COND_DESTROYED:
        case REPLICANT_COND_NOT_FOUND:
        case REPLICANT_OBJ_EXIST:
        case REPLICANT_CTOR_FAILED:
        case REPLICANT_GARBAGE:
        default:
            err = m_coord.error();
            ERROR(COORDFAIL) << "internal library error: " << err.msg() << " @ " << err.loc();
            break;
    }

    *ret_err = m_last_error;
    m_last_error = tmp;
}

bool
admin :: maintain_coord_connection(wtf_admin_returncode* status)
{
    replicant_returncode rc;
    e::error err;

    if (!m_coord.ensure_configuration(&rc))
    {
        switch (rc)
        {
            case REPLICANT_TIMEOUT:
                ERROR(TIMEOUT) << "operation timed out";
                return false;
            case REPLICANT_INTERRUPTED:
                ERROR(INTERRUPTED) << "signal received";
                return false;
            case REPLICANT_NAME_TOO_LONG:
            case REPLICANT_OBJ_NOT_FOUND:
            case REPLICANT_FUNC_NOT_FOUND:
            case REPLICANT_CLUSTER_JUMP:
                err = m_coord.error();
                ERROR(COORDFAIL) << "persistent coordinator error: " << err.msg() << " @ " << err.loc();
                break;
            case REPLICANT_MISBEHAVING_SERVER:
            case REPLICANT_SERVER_ERROR:
            case REPLICANT_NEED_BOOTSTRAP:
            case REPLICANT_BACKOFF:
                err = m_coord.error();
                ERROR(COORDFAIL) << "transient coordinator error: " << err.msg() << " @ " << err.loc();
                return false;
            case REPLICANT_SUCCESS:
            case REPLICANT_OBJ_EXIST:
            case REPLICANT_COND_NOT_FOUND:
            case REPLICANT_COND_DESTROYED:
            case REPLICANT_BAD_LIBRARY:
            case REPLICANT_INTERNAL_ERROR:
            case REPLICANT_NONE_PENDING:
            case REPLICANT_CTOR_FAILED:
            case REPLICANT_GARBAGE:
            default:
                *status = WTF_ADMIN_INTERNAL;
                return false;
        }
    }

    if (m_busybee.set_external_fd(m_coord.poll_fd()) != BUSYBEE_SUCCESS)
    {
        ERROR(POLLFAILED) << "poll failed";
        return false;
    }

    if (m_coord.queued_responses() > 0)
    {
        m_handle_coord_ops = true;
    }

    return true;
}

bool
admin :: send(wtf_network_msgtype mt,
              server_id id,
              uint64_t nonce,
              std::auto_ptr<e::buffer> msg,
              e::intrusive_ptr<pending> op,
              wtf_admin_returncode* status)
{
    const uint8_t type = static_cast<uint8_t>(mt);
    const uint8_t flags = 0;
    const uint64_t version = m_coord.config()->version();
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << type << flags << version << uint64_t(UINT64_MAX) << nonce;
    m_busybee.set_timeout(-1);

    switch (m_busybee.send(id.get(), msg))
    {
        case BUSYBEE_SUCCESS:
            op->handle_sent_to(id);
            m_server_ops.insert(std::make_pair(nonce, pending_server_pair(id, op)));
            return true;
        case BUSYBEE_DISRUPTED:
            handle_disruption(id);
            ERROR(SERVERERROR) << "server " << id.get() << " had a communication disruption";
            return false;
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
            ERROR(POLLFAILED) << "poll failed";
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
        default:
            abort();
    }
}

void
admin :: handle_disruption(const server_id& si)
{
    pending_map_t::iterator it = m_server_ops.begin();

    while (it != m_server_ops.end())
    {
        if (it->second.si == si)
        {
            m_failed.push_back(it->second);
            pending_map_t::iterator tmp = it;
            ++it;
            m_server_ops.erase(tmp);
        }
        else
        {
            ++it;
        }
    }

    m_busybee.drop(si.get());
}

std::ostream&
operator << (std::ostream& lhs, wtf_admin_returncode rhs)
{
    switch (rhs)
    {
        STRINGIFY(WTF_ADMIN_SUCCESS);
        STRINGIFY(WTF_ADMIN_NOMEM);
        STRINGIFY(WTF_ADMIN_NONEPENDING);
        STRINGIFY(WTF_ADMIN_POLLFAILED);
        STRINGIFY(WTF_ADMIN_TIMEOUT);
        STRINGIFY(WTF_ADMIN_INTERRUPTED);
        STRINGIFY(WTF_ADMIN_SERVERERROR);
        STRINGIFY(WTF_ADMIN_COORDFAIL);
        STRINGIFY(WTF_ADMIN_BADSPACE);
        STRINGIFY(WTF_ADMIN_DUPLICATE);
        STRINGIFY(WTF_ADMIN_NOTFOUND);
        STRINGIFY(WTF_ADMIN_LOCALERROR);
        STRINGIFY(WTF_ADMIN_INTERNAL);
        STRINGIFY(WTF_ADMIN_EXCEPTION);
        STRINGIFY(WTF_ADMIN_GARBAGE);
        default:
            lhs << "unknown wtf_admin_returncode";
    }

    return lhs;
}
