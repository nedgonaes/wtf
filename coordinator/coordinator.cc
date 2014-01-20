// Copyright (c) 2012-2013, Cornell University
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
//     * Neither the name of HyperDex nor the names of its contributors may be
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
#include <sstream>

// e
#include <e/endian.h>

// Replicant
#include <replicant_state_machine.h>

// WTF 
#include "common/serialization.h"
#include "coordinator/coordinator.h"
#include "coordinator/transitions.h"
#include "coordinator/util.h"

#define ALARM_INTERVAL 30

using wtf::coordinator;

// ASSUME:  I'm assuming only one server ever changes state at a time for a
//          given transition.  If you violate this assumption, fixup
//          converge_intent.

namespace
{

std::string
to_string(const po6::net::location& loc)
{
    std::ostringstream oss;
    oss << loc;
    return oss.str();
}

template <typename T>
void
shift_and_pop(size_t idx, std::vector<T>* v)
{
    for (size_t i = idx + 1; i < v->size(); ++i)
    {
        (*v)[i - 1] = (*v)[i];
    }

    v->pop_back();
}

template <typename T>
void
remove(const T& t, std::vector<T>* v)
{
    for (size_t i = 0; i < v->size(); )
    {
        if ((*v)[i] == t)
        {
            shift_and_pop(i, v);
        }
        else
        {
            ++i;
        }
    }
}

template <class T, class TID>
bool
find_id(const TID& id, std::vector<T>& v, T** found)
{
    for (size_t i = 0; i < v.size(); ++i)
    {
        if (v[i].id == id)
        {
            *found = &v[i];
            return true;
        }
    }

    return false;
}

template <class T, class TID>
void
remove_id(const TID& id, std::vector<T>* v)
{
    for (size_t i = 0; i < v->size(); )
    {
        if ((*v)[i].id == id)
        {
            shift_and_pop(i, v);
        }
        else
        {
            ++i;
        }
    }
}

} // namespace

coordinator :: coordinator()
    : m_cluster(0)
    , m_counter(1)
    , m_version(0)
    , m_flags(0)
    , m_servers()
    , m_offline()
    , m_config_ack_through(0)
    , m_config_ack_barrier()
    , m_config_stable_through(0)
    , m_config_stable_barrier()
    , m_checkpoint(0)
    , m_checkpoint_stable_through(0)
    , m_checkpoint_gc_through(0)
    , m_checkpoint_stable_barrier()
    , m_latest_config()
{
    assert(m_config_ack_through == m_config_ack_barrier.min_version());
    assert(m_config_stable_through == m_config_stable_barrier.min_version());
    assert(m_checkpoint_stable_through == m_checkpoint_stable_barrier.min_version());
}

coordinator :: ~coordinator() throw ()
{
}

void
coordinator :: init(replicant_state_machine_context* ctx, uint64_t token)
{
    FILE* log = replicant_state_machine_log_stream(ctx);

    if (m_cluster != 0)
    {
        fprintf(log, "cannot initialize HyperDex cluster with id %lu "
                     "because it is already initialized to %lu\n", token, m_cluster);
        // we lie to the client and pretend all is well
        return generate_response(ctx, COORD_SUCCESS);
    }

    replicant_state_machine_alarm(ctx, "alarm", ALARM_INTERVAL);
    fprintf(log, "initializing HyperDex cluster with id %lu\n", token);
    m_cluster = token;
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: read_only(replicant_state_machine_context* ctx, bool ro)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    uint64_t old_flags = m_flags;

    if (ro)
    {
        if ((m_flags & HYPERDEX_CONFIG_READ_ONLY))
        {
            fprintf(log, "cluster already in read-only mode\n");
        }
        else
        {
            fprintf(log, "putting cluster into read-only mode\n");
        }

        m_flags |= HYPERDEX_CONFIG_READ_ONLY;
    }
    else
    {
        if ((m_flags & HYPERDEX_CONFIG_READ_ONLY))
        {
            fprintf(log, "putting cluster into read-write mode\n");
        }
        else
        {
            fprintf(log, "cluster already in read-write mode\n");
        }

        uint64_t mask = HYPERDEX_CONFIG_READ_ONLY;
        mask = ~mask;
        m_flags &= mask;
    }

    if (old_flags != m_flags)
    {
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_register(replicant_state_machine_context* ctx,
                               const server_id& sid,
                               const po6::net::location& bind_to)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (srv)
    {
        std::string str(to_string(srv->bind_to));
        fprintf(log, "cannot register server(%lu) because the id belongs to "
                     "server(%lu, %s)\n", sid.get(), srv->id.get(), str.c_str());
        return generate_response(ctx, hyperdex::COORD_DUPLICATE);
    }

    srv = new_server(sid);
    srv->state = server::ASSIGNED;
    srv->bind_to = bind_to;
    fprintf(log, "registered server(%lu)\n", sid.get());
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_online(replicant_state_machine_context* ctx,
                             const server_id& sid,
                             const po6::net::location* bind_to)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot bring server(%lu) online because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, hyperdex::COORD_NOT_FOUND);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::SHUTDOWN &&
        srv->state != server::AVAILABLE)
    {
        fprintf(log, "cannot bring server(%lu) online because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, hyperdex::COORD_NO_CAN_DO);
    }

    bool changed = false;

    if (bind_to && srv->bind_to != *bind_to)
    {
        std::string from(to_string(srv->bind_to));
        std::string to(to_string(*bind_to));

        for (size_t i = 0; i < m_servers.size(); ++i)
        {
            if (m_servers[i].id != sid &&
                m_servers[i].bind_to == *bind_to)
            {
                fprintf(log, "cannot change server(%lu) to %s "
                             "because that address is in use by "
                             "server(%lu)\n", sid.get(), to.c_str(),
                             m_servers[i].id.get());
                return generate_response(ctx, hyperdex::COORD_DUPLICATE);
            }
        }

        fprintf(log, "changing server(%lu)'s address from %s to %s\n",
                     sid.get(), from.c_str(), to.c_str());
        srv->bind_to = *bind_to;
        changed = true;
    }

    if (srv->state != server::AVAILABLE)
    {
        fprintf(log, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::AVAILABLE));
        srv->state = server::AVAILABLE;

        if (!in_permutation(sid))
        {
            add_permutation(sid);
        }

        rebalance_replica_sets(ctx);
        changed = true;
    }

    if (changed)
    {
        generate_next_configuration(ctx);
    }

    char buf[sizeof(uint64_t)];
    e::pack64be(sid.get(), buf);
    uint64_t client = replicant_state_machine_get_client(ctx);
    replicant_state_machine_suspect(ctx, client, "server_suspect",  buf, sizeof(uint64_t));
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_offline(replicant_state_machine_context* ctx,
                              const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot bring server(%lu) offline because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, hyperdex::COORD_NOT_FOUND);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::AVAILABLE &&
        srv->state != server::SHUTDOWN)
    {
        fprintf(log, "cannot bring server(%lu) offline because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, hyperdex::COORD_NO_CAN_DO);
    }

    if (srv->state != server::NOT_AVAILABLE && srv->state != server::SHUTDOWN)
    {
        fprintf(log, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::NOT_AVAILABLE));
        srv->state = server::NOT_AVAILABLE;
        remove_permutation(sid);
        rebalance_replica_sets(ctx);
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_shutdown(replicant_state_machine_context* ctx,
                               const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot shutdown server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, hyperdex::COORD_NOT_FOUND);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::AVAILABLE &&
        srv->state != server::SHUTDOWN)
    {
        fprintf(log, "cannot shutdown server(%lu) because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, hyperdex::COORD_NO_CAN_DO);
    }

    if (srv->state != server::SHUTDOWN)
    {
        fprintf(log, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::SHUTDOWN));
        srv->state = server::SHUTDOWN;
        rebalance_replica_sets(ctx);
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_kill(replicant_state_machine_context* ctx,
                           const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot kill server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, hyperdex::COORD_NOT_FOUND);
    }

    if (srv->state != server::KILLED)
    {
        fprintf(log, "changing server(%lu) from %s to %s\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::KILLED));
        srv->state = server::KILLED;
        remove_permutation(sid);
        remove_offline(sid);
        rebalance_replica_sets(ctx);
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_forget(replicant_state_machine_context* ctx,
                             const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot forget server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, hyperdex::COORD_NOT_FOUND);
    }

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == sid)
        {
            std::swap(m_servers[i], m_servers[m_servers.size() - 1]);
            m_servers.pop_back();
        }
    }

    std::stable_sort(m_servers.begin(), m_servers.end());
    remove_permutation(sid);
    remove_offline(sid);
    rebalance_replica_sets(ctx);
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_suspect(replicant_state_machine_context* ctx,
                              const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server* srv = get_server(sid);

    if (!srv)
    {
        fprintf(log, "cannot suspect server(%lu) because "
                     "the server doesn't exist\n", sid.get());
        return generate_response(ctx, hyperdex::COORD_NOT_FOUND);
    }

    if (srv->state == server::SHUTDOWN)
    {
        return generate_response(ctx, COORD_SUCCESS);
    }

    if (srv->state != server::ASSIGNED &&
        srv->state != server::NOT_AVAILABLE &&
        srv->state != server::AVAILABLE)
    {
        fprintf(log, "cannot suspect server(%lu) because the server is "
                     "%s\n", sid.get(), server::to_string(srv->state));
        return generate_response(ctx, hyperdex::COORD_NO_CAN_DO);
    }

    if (srv->state != server::NOT_AVAILABLE && srv->state != server::SHUTDOWN)
    {
        fprintf(log, "changing server(%lu) from %s to %s because we suspect it failed\n",
                     sid.get(), server::to_string(srv->state),
                     server::to_string(server::NOT_AVAILABLE));
        srv->state = server::NOT_AVAILABLE;
        remove_permutation(sid);
        rebalance_replica_sets(ctx);
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: report_disconnect(replicant_state_machine_context*,
                                 const server_id& sid, uint64_t version)
{
//XXX: figure out what this is for.
}

void
coordinator :: config_get(replicant_state_machine_context* ctx)
{
    assert(m_cluster != 0 && m_version != 0);
    assert(m_latest_config.get());
    const char* output = reinterpret_cast<const char*>(m_latest_config->data());
    size_t output_sz = m_latest_config->size();
    replicant_state_machine_set_response(ctx, output, output_sz);
}

void
coordinator :: config_ack(replicant_state_machine_context* ctx,
                          const server_id& sid, uint64_t version)
{
    m_config_ack_barrier.pass(version, sid);
    check_ack_condition(ctx);
}

void
coordinator :: config_stable(replicant_state_machine_context* ctx,
                             const server_id& sid, uint64_t version)
{
    m_config_stable_barrier.pass(version, sid);
    check_stable_condition(ctx);
}

void
coordinator :: checkpoint(replicant_state_machine_context* ctx)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    uint64_t cond_state = 0;

    if (replicant_state_machine_condition_broadcast(ctx, "checkp", &cond_state) < 0)
    {
        fprintf(log, "could not broadcast on \"checkp\" condition\n");
    }

    ++m_checkpoint;
    fprintf(log, "establishing checkpoint %lu\n", m_checkpoint);
    assert(cond_state == m_checkpoint);
    assert(m_checkpoint_stable_through <= m_checkpoint);
    std::vector<server_id> sids;
    servers_in_configuration(&sids);
    m_checkpoint_stable_barrier.new_version(m_checkpoint, sids);
    check_checkpoint_stable_condition(ctx);
}

void
coordinator :: checkpoint_stable(replicant_state_machine_context* ctx,
                                 const server_id& sid,
                                 uint64_t config,
                                 uint64_t number)
{
    if (config < m_version)
    {
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    m_checkpoint_stable_barrier.pass(number, sid);
    check_checkpoint_stable_condition(ctx);
    generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: alarm(replicant_state_machine_context* ctx)
{
    replicant_state_machine_alarm(ctx, "alarm", ALARM_INTERVAL);
    checkpoint(ctx);
}

void
coordinator :: debug_dump(replicant_state_machine_context* ctx)
{
}

coordinator*
coordinator :: recreate(replicant_state_machine_context* ctx,
                        const char* data, size_t data_sz)
{
    std::auto_ptr<coordinator> c(new coordinator());

    if (!c.get())
    {
        fprintf(replicant_state_machine_log_stream(ctx), "memory allocation failed\n");
        return NULL;
    }

    e::unpacker up(data, data_sz);
    up = up >> c->m_cluster >> c->m_counter >> c->m_version >> c->m_flags >> c->m_servers
            >> c->m_offline 
            >> c->m_config_ack_through >> c->m_config_ack_barrier
            >> c->m_config_stable_through >> c->m_config_stable_barrier
            >> c->m_checkpoint >> c->m_checkpoint_stable_through
            >> c->m_checkpoint_gc_through >> c->m_checkpoint_stable_barrier;

    if (up.error())
    {
        fprintf(replicant_state_machine_log_stream(ctx), "unpacking failed\n");
        return NULL;
    }

    c->generate_cached_configuration(ctx);
    replicant_state_machine_alarm(ctx, "alarm", ALARM_INTERVAL);
    return c.release();
}

void
coordinator :: snapshot(replicant_state_machine_context* /*ctx*/,
                        const char** data, size_t* data_sz)
{
    size_t sz = sizeof(m_cluster)
              + sizeof(m_counter)
              + sizeof(m_version)
              + sizeof(m_flags)
              + pack_size(m_servers)
              + pack_size(m_offline)
              + sizeof(m_config_ack_through)
              + pack_size(m_config_ack_barrier)
              + sizeof(m_config_stable_through)
              + pack_size(m_config_stable_barrier)
              + sizeof(m_checkpoint)
              + sizeof(m_checkpoint_stable_through)
              + sizeof(m_checkpoint_gc_through)
              + pack_size(m_checkpoint_stable_barrier);

    std::auto_ptr<e::buffer> buf(e::buffer::create(sz));
    e::buffer::packer pa = buf->pack_at(0);
    pa = pa << m_cluster << m_counter << m_version << m_flags << m_servers
            << m_offline 
            << m_config_ack_through << m_config_ack_barrier
            << m_config_stable_through << m_config_stable_barrier
            << m_checkpoint << m_checkpoint_stable_through
            << m_checkpoint_gc_through << m_checkpoint_stable_barrier;

    char* ptr = static_cast<char*>(malloc(buf->size()));
    *data = ptr;
    *data_sz = buf->size();

    if (*data)
    {
        memmove(ptr, buf->data(), buf->size());
    }
}

server*
coordinator :: new_server(const server_id& sid)
{
    size_t idx = m_servers.size();
    m_servers.push_back(server(sid));

    for (; idx > 0; --idx)
    {
        if (m_servers[idx - 1].id < m_servers[idx].id)
        {
            break;
        }

        std::swap(m_servers[idx - 1], m_servers[idx]);
    }

    return &m_servers[idx];
}

server*
coordinator :: get_server(const server_id& sid)
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].id == sid)
        {
            return &m_servers[i];
        }
    }

    return NULL;
}

bool
coordinator :: in_permutation(const server_id& sid)
{
    for (size_t i = 0; i < m_permutation.size(); ++i)
    {
        if (m_permutation[i] == sid)
        {
            return true;
        }
    }

    for (size_t i = 0; i < m_spares.size(); ++i)
    {
        if (m_spares[i] == sid)
        {
            return true;
        }
    }

    return false;
}

void
coordinator :: add_permutation(const server_id& sid)
{
    if (m_spares.size() < m_desired_spares)
    {
        m_spares.push_back(sid);
    }
    else
    {
        m_permutation.push_back(sid);
    }
}

void
coordinator :: remove_permutation(const server_id& sid)
{
    remove(sid, &m_spares);

    for (size_t i = 0; i < m_permutation.size(); ++i)
    {
        if (m_permutation[i] != sid)
        {
            continue;
        }

        if (m_spares.empty())
        {
            shift_and_pop(i, &m_permutation);
        }
        else
        {
            m_permutation[i] = m_spares.back();
            m_spares.pop_back();
        }

        break;
    }
}

void
coordinator :: remove_offline(const server_id& sid)
{
    for (size_t i = 0; i < m_offline.size(); )
    {
        if (m_offline[i].sid == sid)
        {
            shift_and_pop(i, &m_offline);
        }
        else
        {
            ++i;
        }
    }
}

void
coordinator :: check_ack_condition(replicant_state_machine_context* ctx)
{
    if (m_config_ack_through < m_config_ack_barrier.min_version())
    {
        FILE* log = replicant_state_machine_log_stream(ctx);
        fprintf(log, "acked through version %lu\n", m_config_ack_barrier.min_version());
    }

    while (m_config_ack_through < m_config_ack_barrier.min_version())
    {
        replicant_state_machine_condition_broadcast(ctx, "ack", &m_config_ack_through);
    }
}

void
coordinator :: check_stable_condition(replicant_state_machine_context* ctx)
{
    if (m_config_stable_through < m_config_stable_barrier.min_version())
    {
        FILE* log = replicant_state_machine_log_stream(ctx);
        fprintf(log, "stable through version %lu\n", m_config_stable_barrier.min_version());
    }

    while (m_config_stable_through < m_config_stable_barrier.min_version())
    {
        replicant_state_machine_condition_broadcast(ctx, "stable", &m_config_stable_through);
    }
}

void
coordinator :: generate_next_configuration(replicant_state_machine_context* ctx)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    uint64_t cond_state;

    if (replicant_state_machine_condition_broadcast(ctx, "config", &cond_state) < 0)
    {
        fprintf(log, "could not broadcast on \"config\" condition\n");
    }

    ++m_version;
    fprintf(log, "issuing new configuration version %lu\n", m_version);
    assert(cond_state == m_version);
    std::vector<server_id> sids;
    servers_in_configuration(&sids);
    m_config_ack_barrier.new_version(m_version, sids);
    m_config_stable_barrier.new_version(m_version, sids);
    check_ack_condition(ctx);
    check_stable_condition(ctx);
    generate_cached_configuration(ctx);
}

void
coordinator :: generate_cached_configuration(replicant_state_machine_context*)
{
    m_latest_config.reset();
    size_t sz = 7 * sizeof(uint64_t);

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        sz += pack_size(m_servers[i]);
    }

    std::auto_ptr<e::buffer> new_config(e::buffer::create(sz));
    e::buffer::packer pa = new_config->pack_at(0);
    pa = pa << m_cluster << m_version << m_flags
            << uint64_t(m_servers.size());

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        pa = pa << m_servers[i];
    }

    m_latest_config = new_config;
}

void
coordinator :: servers_in_configuration(std::vector<server_id>* sids)
{
    for (std::vector<server_id>::iterator it = m_servers.begin();
            it != m_servers.end(); ++it)
    {
        sids->push_back(it->id);
    }

    std::sort(sids->begin(), sids->end());
    std::vector<server_id>::iterator sit;
    sit = std::unique(sids->begin(), sids->end());
    sids->resize(sit - sids->begin());
}

#define OUTSTANDING_CHECKPOINTS 120

void
coordinator :: check_checkpoint_stable_condition(replicant_state_machine_context* ctx)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    assert(m_checkpoint_stable_through <= m_checkpoint);

    if (m_checkpoint_stable_through < m_checkpoint_stable_barrier.min_version())
    {
        fprintf(log, "checkpoint %lu done\n", m_checkpoint_stable_barrier.min_version());
    }

    bool stabilized = false;

    while (m_checkpoint_stable_through < m_checkpoint_stable_barrier.min_version())
    {
        stabilized = true;
        replicant_state_machine_condition_broadcast(ctx, "checkps", &m_checkpoint_stable_through);
    }

    bool gc = false;

    while (m_checkpoint_gc_through + OUTSTANDING_CHECKPOINTS < m_checkpoint_stable_barrier.min_version())
    {
        gc = true;
        replicant_state_machine_condition_broadcast(ctx, "checkpgc", &m_checkpoint_gc_through);
    }

    if (gc && m_checkpoint_gc_through > 0)
    {
        fprintf(log, "garbage collect <= checkpoint %lu\n", m_checkpoint_gc_through);
    }

    assert(m_checkpoint_stable_through <= m_checkpoint);
    assert(m_checkpoint_gc_through + OUTSTANDING_CHECKPOINTS <= m_checkpoint ||
           m_checkpoint <= OUTSTANDING_CHECKPOINTS);

    if (stabilized)
    {
	//XXX: do something.
    }
}
