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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// C
#include <cmath>

// POSIX
#include <dlfcn.h>
#include <signal.h>
#include <unistd.h>

// STL
#include <algorithm>

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

// po6
#include <po6/pathname.h>

// e
#include <e/endian.h>
#include <e/envconfig.h>
#include <e/time.h>

// BusyBee
#include <busybee_constants.h>
#include <busybee_mta.h>
#include <busybee_single.h>

// WTF
#include "common/macros.h"
#include "common/network_msgtype.h"
#include "common/special_objects.h"
#include "common/wtf_node.h"
#include "daemon/daemon.h"
#include "daemon/request_response.h"

using wtf::daemon;

int s_interrupts = 0;
bool s_alarm = false;

#define CHECK_UNPACK(MSGTYPE, UNPACKER) \
    do \
    { \
        if (UNPACKER.error()) \
        { \
            wtf_network_msgtype CONCAT(_anon, __LINE__)(WTFNET_ ## MSGTYPE); \
            LOG(WARNING) << "received corrupt \"" \
                         << CONCAT(_anon, __LINE__) << "\" message"; \
            return; \
        } \
    } while (0)

static bool s_continue = true;

static void
exit_on_signal(int /*signum*/)
{
    RAW_LOG(ERROR, "signal received; triggering exit");
    s_continue = false;
}

static uint64_t
monotonic_time()
{
    return e::time();
}

daemon :: ~daemon() throw ()
{
}

daemon :: daemon()
    : m_s() 
    , m_busybee_mapper()
    , m_busybee()
    , m_us()
    , m_coord(this)
    , m_periodic()
    , m_temporary_servers()
{
}

static bool
install_signal_handler(int signum)
{
    struct sigaction handle;
    handle.sa_handler = exit_on_signal;
    sigfillset(&handle.sa_mask);
    handle.sa_flags = SA_RESTART;
    return sigaction(signum, &handle, NULL) >= 0;
}

int
daemon :: run(bool daemonize,
              po6::pathname data,
              bool set_bind_to,
              po6::net::location bind_to,
              bool set_coordinator,
              po6::net::hostname coordinator)
{
    if (!install_signal_handler(SIGHUP))
    {
        std::cerr << "could not install SIGHUP handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGINT))
    {
        std::cerr << "could not install SIGINT handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGTERM))
    {
        std::cerr << "could not install SIGTERM handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return EXIT_FAILURE;
    }

    int err = pthread_sigmask(SIG_BLOCK, &ss, NULL);

    if (err < 0)
    {
        errno = err;
        PLOG(ERROR) << "could not block signals";
        return EXIT_FAILURE;
    }

    google::LogToStderr();

    if (daemonize)
    {
        LOG(INFO) << "forking off to the background";
        LOG(INFO) << "you can find the log at wtf-daemon-YYYYMMDD-HHMMSS.sssss";
        LOG(INFO) << "provide \"--foreground\" on the command-line if you want to run in the foreground";
        google::SetLogSymlink(google::INFO, "");
        google::SetLogSymlink(google::WARNING, "");
        google::SetLogSymlink(google::ERROR, "");
        google::SetLogSymlink(google::FATAL, "");
        google::SetLogDestination(google::INFO, "wtf-daemon-");

        if (::daemon(1, 0) < 0)
        {
            PLOG(ERROR) << "could not daemonize";
            return EXIT_FAILURE;
        }
    }
    else
    {
        LOG(INFO) << "running in the foreground";
        LOG(INFO) << "no log will be generated; instead, the log messages will print to the terminal";
        LOG(INFO) << "provide \"--daemon\" on the command-line if you want to run in the background";
    }

    bool restored = false;

    m_coord.set_coordinator_address(coordinator.address.c_str(), coordinator.port);

    m_us.address = bind_to;

    if (!generate_token(&m_us.token))
    {
        PLOG(ERROR) << "could not read random tokens from /dev/urandom";
        return EXIT_FAILURE;
    }

    m_us.address = bind_to;

    m_busybee.reset(new busybee_mta(&m_busybee_mapper, m_us.address, m_us.token, 0/*we don't use pause/unpause*/));
    LOG(INFO) << "token " << m_us.token;
    m_busybee->set_timeout(1000);

    wtf::connection conn;
    std::auto_ptr<e::buffer> msg;

    LOG(INFO) << "recv";
    while (recv(&conn, &msg))
    {
        LOG(INFO) << "recved";
        assert(msg.get());
        wtf_network_msgtype mt = WTFNET_NOP;
        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        up = up >> mt;

        switch (mt)
        {
            case WTFNET_NOP:
                break;
            case WTFNET_GET:
                process_get(conn, msg, up);
                break;
            case WTFNET_PUT:
                process_put(conn, msg, up);
                break;
            default:
                LOG(WARNING) << "unknown message type; here's some hex:  " << msg->hex();
                break;
        }
    }

    LOG(INFO) << "wtf is gracefully shutting down";
    LOG(INFO) << "wtf will now terminate";
    return EXIT_SUCCESS;
}

bool
daemon :: recv(wtf::connection* conn, std::auto_ptr<e::buffer>* msg)
{
    while (s_continue)
    {
        busybee_returncode rc = m_busybee->recv(&conn->token, msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_INTERRUPTED:
                continue;
            case BUSYBEE_DISRUPTED:
                continue;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "BusyBee returned " << rc << " during a \"recv\" call";
                return false;
        }

        return true;
    }

    return false;
}

bool
daemon :: send(const wtf::connection& conn, std::auto_ptr<e::buffer> msg)
{
    switch (m_busybee->send(conn.token, msg))
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
        default:
            return false;
    }
}

bool
daemon :: send(const wtf_node& node, std::auto_ptr<e::buffer> msg)
{
    m_busybee_mapper.set(node);

    switch (m_busybee->send(node.token, msg))
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
        default:
            return false;
    }
}

bool
daemon :: send_no_disruption(uint64_t token, std::auto_ptr<e::buffer> msg)
{
    switch (m_busybee->send(token, msg))
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        case BUSYBEE_INTERRUPTED:
        default:
            return false;
    }
}

void
daemon :: process_put(const wtf::connection& conn,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up)
{
    e::slice data = up.as_slice();
    LOG(INFO) << "PUT: " << data.hex();
}

void
daemon :: process_get(const wtf::connection& conn, 
                      std::auto_ptr<e::buffer> msg, 
                      e::unpacker up)
{
    e::slice data = up.as_slice();
    LOG(INFO) << "GET: " << data.hex();
}

typedef void (daemon::*_periodic_fptr)(uint64_t now);
typedef std::pair<uint64_t, _periodic_fptr> _periodic;

static bool
compare_periodic(const _periodic& lhs, const _periodic& rhs)
{
    return lhs.first > rhs.first;
}

void
daemon :: trip_periodic(uint64_t when, periodic_fptr fp)
{
    for (size_t i = 0; i < m_periodic.size(); ++i)
    {
        if (m_periodic[i].second == fp)
        {
            m_periodic[i].second = &daemon::periodic_nop;
        }
    }

    // Clean up dead functions from the front
    while (!m_periodic.empty() && m_periodic[0].second == &daemon::periodic_nop)
    {
        std::pop_heap(m_periodic.begin(), m_periodic.end(), compare_periodic);
        m_periodic.pop_back();
    }

    // And from the back
    while (!m_periodic.empty() && m_periodic.back().second == &daemon::periodic_nop)
    {
        m_periodic.pop_back();
    }

    m_periodic.push_back(std::make_pair(when, fp));
    std::push_heap(m_periodic.begin(), m_periodic.end(), compare_periodic);
}

void
daemon :: run_periodic()
{
    uint64_t now = monotonic_time();

    while (!m_periodic.empty() && m_periodic[0].first <= now)
    {
        if (m_periodic.size() > m_s.PERIODIC_SIZE_WARNING)
        {
            LOG(WARNING) << "there are " << m_periodic.size()
                         << " functions scheduled which exceeds the threshold of "
                         << m_s.PERIODIC_SIZE_WARNING << " functions";
        }

        periodic_fptr fp;
        std::pop_heap(m_periodic.begin(), m_periodic.end(), compare_periodic);
        fp = m_periodic.back().second;
        m_periodic.pop_back();
        (this->*fp)(now);
    }
}

void
daemon :: periodic_nop(uint64_t)
{
}

bool
daemon :: generate_token(uint64_t* token)
{
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0)
    {
        return false;
    }

    *token = 0;

    while (*token < (1ULL << 32))
    {
        if (sysrand.read(token, sizeof(*token)) != sizeof(*token))
        {
            return false;
        }
    }

    return true;
}
