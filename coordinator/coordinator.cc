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

// C++
#include <sstream>

// STL
#include <algorithm>
#include <tr1/memory>

// po6
#include <po6/net/location.h>

// WTF
#include "common/coordinator_returncode.h"
#include "common/serialization.h"
#include "coordinator/coordinator.h"
#include "coordinator/server_state.h"

//////////////////////////// C Transition Functions ////////////////////////////

using namespace wtf;

static inline void
generate_response(replicant_state_machine_context* ctx, coordinator_returncode x)
{
    const char* ptr = NULL;

    switch (x)
    {
        case COORD_SUCCESS:
            ptr = "\x22\x80";
            break;
        case COORD_MALFORMED:
            ptr = "\x22\x81";
            break;
        case COORD_DUPLICATE:
            ptr = "\x22\x82";
            break;
        case COORD_NOT_FOUND:
            ptr = "\x22\x83";
            break;
        case COORD_INITIALIZED:
            ptr = "\x22\x84";
            break;
        case COORD_UNINITIALIZED:
            ptr = "\x22\x85";
            break;
        default:
            ptr = "\x00\x00";
            break;
    }

    replicant_state_machine_set_response(ctx, ptr, 2);
}

#define PROTECT_UNINITIALIZED \
    do \
    { \
        if (!obj) \
        { \
            fprintf(replicant_state_machine_log_stream(ctx), "cannot operate on NULL object\n"); \
            return generate_response(ctx, COORD_UNINITIALIZED); \
        } \
        coordinator* c = static_cast<coordinator*>(obj); \
        if (c->cluster() == 0) \
        { \
            fprintf(replicant_state_machine_log_stream(ctx), "cluster not initialized\n"); \
            return generate_response(ctx, COORD_UNINITIALIZED); \
        } \
    } \
    while (0)

#define CHECK_UNPACK(MSGTYPE) \
    do \
    { \
        if (up.error() || up.remain()) \
        { \
            fprintf(log, "received malformed \"" #MSGTYPE "\" message\n"); \
            return generate_response(ctx, COORD_MALFORMED); \
        } \
    } while (0)

#define INVARIANT_BROKEN(X) \
    fprintf(log, "invariant broken at " __FILE__ ":%d:  %s\n", __LINE__, (X))

extern "C"
{

void*
wtf_coordinator_create(struct replicant_state_machine_context* ctx)
{
    if (replicant_state_machine_condition_create(ctx, "config") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"config\"\n");
        return NULL;
    }

    if (replicant_state_machine_condition_create(ctx, "acked") < 0)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "could not create condition \"acked\"\n");
        return NULL;
    }

    coordinator* c = new (std::nothrow) coordinator();

    if (!c)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "memory allocation failed\n");
    }

    return c;
}

void*
wtf_coordinator_recreate(struct replicant_state_machine_context* ctx,
                              const char* /*data*/, size_t /*data_sz*/)
{
    coordinator* c = new (std::nothrow) coordinator();

    if (!c)
    {
        fprintf(replicant_state_machine_log_stream(ctx), "memory allocation failed\n");
    }

    fprintf(replicant_state_machine_log_stream(ctx), "recreate is not implemented\n");
    // XXX recreate from (data,data_sz)
    return c;
}

void
wtf_coordinator_destroy(struct replicant_state_machine_context*, void* obj)
{
    if (obj)
    {
        delete static_cast<coordinator*>(obj);
    }
}

void
wtf_coordinator_snapshot(struct replicant_state_machine_context* ctx,
                              void* /*obj*/, const char** data, size_t* sz)
{
    fprintf(replicant_state_machine_log_stream(ctx), "snapshot is not implemented\n");
    // XXX snapshot to (data,data_sz)
    *data = NULL;
    *sz = 0;
}

void
wtf_coordinator_initialize(struct replicant_state_machine_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);

    if (!c)
    {
        fprintf(log, "cannot operate on NULL object\n");
        return generate_response(ctx, COORD_UNINITIALIZED);
    }

    uint64_t cluster;
    e::unpacker up(data, data_sz);
    up = up >> cluster;
    CHECK_UNPACK(initialize);
    c->initialize(ctx, cluster);
}

void
wtf_coordinator_get_config(struct replicant_state_machine_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    e::unpacker up(data, data_sz);
    CHECK_UNPACK(get_config);
    c->get_config(ctx);
}

void
wtf_coordinator_ack_config(struct replicant_state_machine_context* ctx,
                                void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> _sid >> version;
    CHECK_UNPACK(ack_config);
    server_id sid(_sid);
    c->ack_config(ctx, sid, version);
}

void
wtf_coordinator_server_register(struct replicant_state_machine_context* ctx,
                                     void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> _sid >> bind_to;
    CHECK_UNPACK(server_register);
    server_id sid(_sid);
    c->server_register(ctx, sid, bind_to);
}

void
wtf_coordinator_server_reregister(struct replicant_state_machine_context* ctx,
                                       void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> _sid >> bind_to;
    CHECK_UNPACK(server_reregister);
    server_id sid(_sid);
    c->server_reregister(ctx, sid, bind_to);
}

void
wtf_coordinator_server_suspect(struct replicant_state_machine_context* ctx,
                                    void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    uint64_t version;
    e::unpacker up(data, data_sz);
    up = up >> _sid >> version;
    CHECK_UNPACK(server_suspect);
    server_id sid(_sid);
    c->server_suspect(ctx, sid, version);
}

void
wtf_coordinator_server_shutdown1(struct replicant_state_machine_context* ctx,
                                      void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    e::unpacker up(data, data_sz);
    up = up >> _sid;
    CHECK_UNPACK(server_shutdown1);
    server_id sid(_sid);
    c->server_shutdown1(ctx, sid);
}

void
wtf_coordinator_server_shutdown2(struct replicant_state_machine_context* ctx,
                                      void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    FILE* log = replicant_state_machine_log_stream(ctx);
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _sid;
    e::unpacker up(data, data_sz);
    up = up >> _sid;
    CHECK_UNPACK(server_shutdown2);
    server_id sid(_sid);
    c->server_shutdown2(ctx, sid);
}

} // extern "C"

/////////////////////////////// Coordinator Class //////////////////////////////

coordinator :: coordinator()
    : m_cluster(0)
    , m_version(0)
    , m_counter(1)
    , m_acked(0)
    , m_servers()
    , m_missing_acks()
    , m_latest_config()
    , m_resp()
    , m_seed()
{
}

coordinator :: ~coordinator() throw ()
{
}

uint64_t
coordinator :: cluster() const
{
    return m_cluster;
}

void
coordinator :: initialize(replicant_state_machine_context* ctx, uint64_t token)
{
    FILE* log = replicant_state_machine_log_stream(ctx);

    if (m_cluster == 0)
    {
        fprintf(log, "initializing WTF cluster with id %lu\n", token);
        m_cluster = token;
        memset(&m_seed, 0, sizeof(m_seed));
#ifdef __APPLE__
        srand(m_seed);
#else
        srand48_r(m_cluster, &m_seed);
#endif
        issue_new_config(ctx);
        return generate_response(ctx, COORD_SUCCESS);
    }
    else
    {
        return generate_response(ctx, COORD_INITIALIZED);
    }
}

void
coordinator :: get_config(replicant_state_machine_context* ctx)
{
    assert(m_cluster != 0 && m_version != 0);

    if (!m_latest_config.get())
    {
        regenerate_cached(ctx);
    }

    const char* output = reinterpret_cast<const char*>(m_latest_config->data());
    size_t output_sz = m_latest_config->size();
    replicant_state_machine_set_response(ctx, output, output_sz);
}

void
coordinator :: ack_config(replicant_state_machine_context* ctx,
                          const server_id& sid,
                          uint64_t version)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server_state* ss = get_state(sid);

    if (ss)
    {
        ss->acked = std::max(ss->acked, version);
        fprintf(log, "server_id(%lu) acks config %lu\n", sid.get(), version);

        if (ss->state == server_state::NOT_AVAILABLE &&
            ss->acked > ss->version)
        {
            ss->state = server_state::AVAILABLE;
        }

        uint64_t remove_up_to = 0;

        for (std::list<missing_acks>::iterator it = m_missing_acks.begin();
                it != m_missing_acks.end(); ++it)
        {
            if (it->version() > ss->acked)
            {
                break;
            }

            it->ack(ss->id);

            if (it->empty())
            {
                assert(remove_up_to < it->version());
                remove_up_to = it->version();
            }
        }

        while (!m_missing_acks.empty() && m_missing_acks.front().version() <= remove_up_to)
        {
            m_missing_acks.pop_front();
        }

        while (m_acked < remove_up_to)
        {
            if (replicant_state_machine_condition_broadcast(ctx, "acked", &m_acked) < 0)
            {
                fprintf(log, "could not broadcast on \"acked\" condition\n");
                break;
            }
        }

        fprintf(log, "servers have acked through version %lu\n", m_acked);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_register(replicant_state_machine_context* ctx,
                               const server_id& sid,
                               const po6::net::location& bind_to)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    std::ostringstream oss;
    oss << bind_to;

    if (is_registered(sid))
    {
        fprintf(log, "cannot register server_id(%lu) on address %s because the id is in use\n", sid.get(), oss.str().c_str());
        return generate_response(ctx, wtf::COORD_DUPLICATE);
    }

    if (is_registered(bind_to))
    {
        fprintf(log, "cannot register server_id(%lu) on address %s because the address is in use\n", sid.get(), oss.str().c_str());
        return generate_response(ctx, wtf::COORD_DUPLICATE);
    }

    m_servers.push_back(server_state(sid, bind_to));
    m_servers.back().state = server_state::AVAILABLE;
    std::stable_sort(m_servers.begin(), m_servers.end());
    fprintf(log, "registered server_id(%lu) on address %s\n", sid.get(), oss.str().c_str());
    return generate_response(ctx, wtf::COORD_SUCCESS);
}

void
coordinator :: server_reregister(replicant_state_machine_context* ctx,
                                 const server_id& sid,
                                 const po6::net::location& bind_to)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    std::ostringstream oss;
    oss << bind_to;
    server_state* ss = get_state(sid);

    if (!ss)
    {
        fprintf(log, "cannot re-register server_id(%lu) on address %s because the server doesn't exist\n", sid.get(), oss.str().c_str());
        return generate_response(ctx, wtf::COORD_DUPLICATE);
    }

    ss->bind_to = po6::net::location();

    if (is_registered(bind_to))
    {
        fprintf(log, "cannot register server_id(%lu) on address %s because the address is in use\n", sid.get(), oss.str().c_str());
        return generate_response(ctx, wtf::COORD_DUPLICATE);
    }

    ss->bind_to = bind_to;
    ss->state = server_state::AVAILABLE;
    fprintf(log, "re-registered server_id(%lu) on address %s\n", sid.get(), oss.str().c_str());
    return generate_response(ctx, wtf::COORD_SUCCESS);
}

void
coordinator :: server_suspect(replicant_state_machine_context* ctx,
                              const server_id& sid, uint64_t version)
{
    FILE* log = replicant_state_machine_log_stream(ctx);

    if (version < m_version)
    {
        return generate_response(ctx, COORD_SUCCESS);
    }

    fprintf(log, "server_id(%lu) suspected\n", sid.get());

    server_state* state = get_state(sid);

    if (state && state->state == server_state::AVAILABLE)
    {
        state->state = server_state::NOT_AVAILABLE;
        state->version = m_version;
    }

    issue_new_config(ctx);

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: server_shutdown1(replicant_state_machine_context* ctx,
                                const server_id& sid)
{
    m_resp.reset(e::buffer::create(sizeof(uint16_t) + sizeof(uint64_t)));
    *m_resp << static_cast<uint16_t>(COORD_SUCCESS) << m_version;
    replicant_state_machine_set_response(ctx, reinterpret_cast<const char*>(m_resp->data()), m_resp->size());
}

void
coordinator :: server_shutdown2(replicant_state_machine_context* ctx,
                                const server_id& sid)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    server_state* ss = get_state(sid);

    if (ss)
    {
        ss->state = server_state::SHUTDOWN;
    }

    fprintf(log, "server_id(%lu) shutdown cleanly\n", sid.get());
    issue_new_config(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

server_state*
coordinator :: get_state(const server_id& sid)
{
    std::vector<server_state>::iterator it;
    it = std::lower_bound(m_servers.begin(), m_servers.end(), sid);

    if (it != m_servers.end() && it->id == sid)
    {
        return &(*it);
    }

    return NULL;
}

bool
coordinator :: is_registered(const server_id& sid)
{
    std::vector<server_state>::iterator it;
    it = std::lower_bound(m_servers.begin(), m_servers.end(), sid);
    return it != m_servers.end() && it->id == sid;
}

bool
coordinator :: is_registered(const po6::net::location& bind_to)
{
    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].bind_to == bind_to)
        {
            return true;
        }
    }

    return false;
}

void
coordinator :: issue_new_config(struct replicant_state_machine_context* ctx)
{
    FILE* log = replicant_state_machine_log_stream(ctx);
    uint64_t cond_state;

    if (replicant_state_machine_condition_broadcast(ctx, "config", &cond_state) < 0)
    {
        fprintf(log, "could not broadcast on \"config\" condition\n");
    }

    ++m_version;
    std::vector<server_id> sids;

    for (std::vector<server_state>::iterator it = m_servers.begin();
            it != m_servers.end(); ++it)
     {
         sids.push_back(it->id);
     }

    m_missing_acks.push_back(missing_acks(m_version, sids));
    fprintf(log, "issuing new configuration version %lu\n", m_version);
    m_latest_config.reset();
}

template <class T>
static void
uniquify/*not a word*/(std::vector<T>* v)
{
    std::sort(v->begin(), v->end());
    typename std::vector<T>::iterator it;
    it = std::unique(v->begin(), v->end());
    v->resize(it - v->begin());
}

template <class T, class U>
static bool
contains(const std::vector<std::pair<T, U> >& v, const T& e)
{
    typename std::vector<std::pair<T, U> >::const_iterator it;
    it = std::lower_bound(v.begin(), v.end(), std::make_pair(e, U()));
    return it != v.end() && it->first == e;
}

template <class T, class TID>
static void
remove(std::vector<T>* v, const TID& id)
{
    for (size_t i = 0; i < v->size(); ++i)
    {
        if ((*v)[i].id == id)
        {
            for (size_t j = i; j + 1 < v->size(); ++j)
            {
                (*v)[j] = (*v)[j + 1];
            }

            v->pop_back();
            return;
        }
    }
}

void
coordinator :: regenerate_cached(struct replicant_state_machine_context*)
{
    size_t sz = 3 * sizeof(uint64_t);
    uint64_t num_servers = 0;

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].state == server_state::AVAILABLE)
        {
            sz += sizeof(uint64_t) + pack_size(m_servers[i].bind_to);
            ++num_servers;
        }
    }

    std::auto_ptr<e::buffer> new_config(e::buffer::create(sz));
    e::buffer::packer pa = new_config->pack_at(0);
    pa = pa << m_cluster << m_version
            << num_servers;

    for (size_t i = 0; i < m_servers.size(); ++i)
    {
        if (m_servers[i].state == server_state::AVAILABLE)
        {
            pa = pa << m_servers[i].id.get() << m_servers[i].bind_to;
        }
    }

    m_latest_config = new_config;
}
