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

#define ALARM_INTERVAL 30

// C
#include <cmath>
#include <stdio.h>

// POSIX
#include <signal.h>
#include <unistd.h>
#include <sys/statvfs.h>

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
#include "common/response_returncode.h"
#include "common/special_objects.h"
#include "daemon/daemon.h"

using wtf::daemon;

int s_interrupts = 0;
bool s_alarm = false;
bool s_debug = false;


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

#define COMMAND_HEADER_SIZE (BUSYBEE_HEADER_SIZE + \
    pack_size(wtf::RESP_GET) + \
    pack_size(wtf::RESPONSE_SUCCESS) + sizeof(uint64_t))

static void
exit_on_signal(int /*signum*/)
{
    if (__sync_fetch_and_add(&s_interrupts, 1) == 0)
    {
        RAW_LOG(ERROR, "interrupted: initiating shutdown; we'll try for up to 10 seconds (interrupt again to exit immediately)");
    }
    else
    {
        RAW_LOG(ERROR, "interrupted again: exiting immediately");
    }
}

static void
exit_after_timeout(int /*signum*/)
{
    __sync_fetch_and_add(&s_interrupts, 1);
    RAW_LOG(ERROR, "took too long to shutdown; just exiting");
}

static void
handle_alarm(int /*signum*/)
{
    s_alarm = true;
}

static void
handle_debug(int /*signum*/)
{
    s_debug = true;
}

static void
dummy(int /*signum*/)
{
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
    , m_us()
    , m_bind_to()
    , m_threads()
    , m_coord(this)
    , m_busybee_mapper(&m_config)
    , m_busybee()
    , m_blockman()
    , m_periodic()
    , m_config()
{
    trip_periodic(0, &daemon::periodic_stat);
}

static bool
install_signal_handler(int signum, void (*f)(int))
{
    struct sigaction handle;
    handle.sa_handler = f;
    sigfillset(&handle.sa_mask);
    handle.sa_flags = SA_RESTART;
    return sigaction(signum, &handle, NULL) >= 0;
}

int
daemon :: run(bool daemonize,
              po6::pathname data,
              po6::pathname backing_path,
              bool set_bind_to,
              po6::net::location bind_to,
              bool set_coordinator,
              po6::net::hostname coordinator,
              unsigned threads)
{
    if (!install_signal_handler(SIGHUP, exit_on_signal))
    {
        std::cerr << "could not install SIGHUP handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGINT, exit_on_signal))
    {
        std::cerr << "could not install SIGINT handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGTERM, exit_on_signal))
    {
        std::cerr << "could not install SIGTERM handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGALRM, handle_alarm))
    {
        std::cerr << "could not install SIGUSR1 handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!install_signal_handler(SIGUSR1, dummy))
    {
        std::cerr << "could not install SIGUSR1 handler; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return EXIT_FAILURE;
    }

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
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
    m_bind_to = bind_to;

    uint64_t sid;

    if (!generate_token(&sid))
    {
        PLOG(ERROR) << "could not read random token from /dev/urandom";
        return EXIT_FAILURE;
    }

    LOG(INFO) << "generated new random token:  " << sid;

    if (!m_coord.register_id(server_id(sid), bind_to))
    {
        return EXIT_FAILURE;
    }

    m_us = server_id(sid);

    LOG(INFO) << "token " << m_us.get();

    m_busybee.reset(new busybee_mta(&m_busybee_mapper, bind_to, m_us.get(), threads));
    m_busybee->set_ignore_signals();
    m_blockman.setup(m_us.get(), data, backing_path);

    for (size_t i = 0; i < threads; ++i)
    {
        std::tr1::shared_ptr<po6::threads::thread> t(new po6::threads::thread(std::tr1::bind(&daemon::loop, this, i)));
        m_threads.push_back(t);
        t->start();
    }

    //m_stat_collector.start();
    alarm(ALARM_INTERVAL);
    bool cluster_jump = false;
    bool requested_exit = false;
    uint64_t checkpoint = 0;
    uint64_t checkpoint_stable = 0;
    uint64_t checkpoint_gc = 0;

    while (__sync_fetch_and_add(&s_interrupts, 0) < 2 &&
           !m_coord.should_exit())
    {
        if (s_alarm)
        {
            s_alarm = false;
            alarm(ALARM_INTERVAL);
        }

        if (s_debug)
        {
            s_debug = false;
            LOG(INFO) << "recieved SIGUSR2; dumping internal tables";
            //XXX: debug dumps of various subsystems
            LOG(INFO) << "end debug dump";
        }

        if (s_interrupts > 0 && !requested_exit)
        {
            if (!install_signal_handler(SIGALRM, exit_after_timeout))
            {
                __sync_fetch_and_add(&s_interrupts, 2);
                break;
            }

            alarm(10);
            m_coord.request_shutdown();
            requested_exit = true;
        }

        if (m_config.version() > 0 &&
            checkpoint < m_coord.checkpoint())
        {
            checkpoint = m_coord.checkpoint();
            //m_repl.begin_checkpoint(checkpoint);
        }

        if (m_config.version() > 0 &&
            checkpoint_stable < m_coord.checkpoint_stable())
        {
            checkpoint_stable = m_coord.checkpoint_stable();
            //m_repl.end_checkpoint(checkpoint_stable);
        }

        if (m_config.version() > 0 &&
            checkpoint_gc < m_coord.checkpoint_gc())
        {
            checkpoint_gc = m_coord.checkpoint_gc();
            //m_data.set_checkpoint_lower_gc(checkpoint_gc);
        }

        if (!m_coord.maintain_link())
        {
            continue;
        }

        const configuration& old_config(m_config);
        const configuration& new_config(m_coord.config());

        if (old_config.cluster() != 0 &&
            old_config.cluster() != new_config.cluster())
        {
            cluster_jump = true;
            break;
        }

        if (old_config.version() > new_config.version())
        {
            LOG(ERROR) << "received new configuration version=" << new_config.version()
                       << " that's older than our current configuration version="
                       << old_config.version();
            continue;
        }
        else if (old_config.version() > new_config.version())
        {
            continue;
        }

        LOG(INFO) << "moving to configuration version=" << new_config.version()
                  << "; pausing all activity while we reconfigure";

        //XXX: pause stuff.
        m_config = new_config;
        //XXX: unpause stuff
        LOG(INFO) << "reconfiguration complete; resuming normal operation";
        LOG(INFO) << "s_interrupts = " << s_interrupts;
        // let the coordinator know we've moved to this config
        m_coord.config_ack(new_config.version());
    }

    if (cluster_jump)
    {
        LOG(INFO) << "\n================================================================================\n"
                  << "Exiting because the coordinator changed on us.\n"
                  << "This is most likely an operations error.  Did you deploy a new HyperDex\n"
                  << "cluster at the same address as the old cluster?\n"
                  << "================================================================================";
    }
    else if (m_coord.should_exit() && !m_coord.config().exists(m_us))
    {
        LOG(INFO) << "\n================================================================================\n"
                  << "Exiting because the coordinator says it doesn't know about this node.\n"
                  << "Check the coordinator logs for details, but it's most likely the case that\n"
                  << "this server was killed, or this server tried reconnecting to a different\n"
                  << "coordinator.  You may just have to restart the daemon with a different \n"
                  << "coordinator address or this node may be dead and you can simply erase it.\n"
                  << "================================================================================";
    }

    __sync_fetch_and_add(&s_interrupts, 2);
    //m_stat_collector.join();

    m_blockman.shutdown();
    m_busybee->shutdown();

    for (size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i]->join();
    }

    LOG(INFO) << "wtf-daemon will now terminate";
    return EXIT_SUCCESS;
}

void
daemon :: loop(size_t thread)
{
    sigset_t ss;

    size_t core = thread % sysconf(_SC_NPROCESSORS_ONLN);
#ifdef __LINUX__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_t cur = pthread_self();
    int x = pthread_setaffinity_np(cur, sizeof(cpu_set_t), &cpuset);
    assert(x == 0);
#elif defined(__APPLE__)
    thread_affinity_policy_data_t policy;
    policy.affinity_tag = 0;
    thread_policy_set(mach_thread_self(),
                      THREAD_AFFINITY_POLICY,
                      (thread_policy_t)&policy,
                      THREAD_AFFINITY_POLICY_COUNT);
#endif

    LOG(INFO) << "network thread " << thread << " started on core " << core;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return;
    }

    sigdelset(&ss, SIGPROF);

    if (pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return;
    }

    wtf::connection conn;
    std::auto_ptr<e::buffer> msg;

    while (recv(&conn, &msg))
    {
        assert(msg.get());
        uint64_t nonce = 0;
        wtf_network_msgtype mt = PACKET_NOP;
        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        up = up >> mt >> nonce;

        switch (mt)
        {
            case PACKET_NOP:
                break;
            case REQ_GET:
                process_get(conn, nonce, msg, up);
                break;
            case REQ_PUT:
                process_put(conn, nonce, msg, up);
                break;
            case REQ_UPDATE:
                process_update(conn, nonce, msg, up);
                break;
            default:
                LOG(WARNING) << "unknown message type; here's some hex:  " << msg->hex();
                break;
        }
    }

    LOG(INFO) << "network thread shutting down";
}

bool
daemon :: recv(wtf::connection* conn, std::auto_ptr<e::buffer>* msg)
{
    while (true)
    {
        busybee_returncode rc = m_busybee->recv(&conn->token, msg);

        LOG(INFO) << "recv'd " << rc;

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_SHUTDOWN:
                return false;
            case BUSYBEE_DISRUPTED:
                continue;
            case BUSYBEE_INTERRUPTED:
                continue;
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "busybee unexpectedly returned " << rc;
                continue;
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
daemon :: send(const server& node, std::auto_ptr<e::buffer> msg)
{
    switch (m_busybee->send(node.id.get(), msg))
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
                            uint64_t nonce,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up)
{
    wtf::response_returncode rc;
    uint64_t bid;
    uint32_t block_offset;
    ssize_t ret = 0;

    up = up >> bid >> block_offset;  // only used for UPDATE.. should be 0 for PUT.
    e::slice data = up.as_slice();

    uint64_t us = m_us.get();
    ret = m_blockman.write_block(data, us, bid); 

    if (ret < data.size())
    {
        rc = RESPONSE_SERVER_ERROR;
    }
    else
    {
        rc = RESPONSE_SUCCESS;
    }

    size_t sz = COMMAND_HEADER_SIZE + 
                sizeof(uint64_t) + /* token */
                sizeof(uint64_t);  /* block id */
    std::auto_ptr<e::buffer> resp(e::buffer::create(sz));
    e::buffer::packer pa = resp->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << RESP_PUT << nonce << rc << bid;
    send(conn, resp);
}

void
daemon :: process_get(const wtf::connection& conn, 
                      uint64_t nonce,
                      std::auto_ptr<e::buffer> msg, 
                      e::unpacker up)
{
    wtf::response_returncode rc;
    uint64_t bid;
    uint32_t len;
    ssize_t ret;

    up = up >> bid >> len;
    uint8_t* data = new uint8_t[len];
    ret = m_blockman.read_block(m_us.get(), bid, data, len);

    if (ret < len)
    {
        rc = RESPONSE_SERVER_ERROR;
        LOG(WARNING) << "block manager returned " << ret << " which is less than expected length " << len;
        len = 0;
    }
    else
    {
        rc = RESPONSE_SUCCESS;
    }

    size_t sz = COMMAND_HEADER_SIZE + 
                len;

    std::auto_ptr<e::buffer> resp(e::buffer::create(sz));
    e::buffer::packer pa = resp->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << RESP_GET << nonce << rc;

    if (len > 0) 
    {
        pa = pa.copy(e::slice(data,len));
    }

    delete [] data;

    send(conn, resp);
}


void
daemon :: process_update(const wtf::connection& conn,
                            uint64_t nonce,
                            std::auto_ptr<e::buffer> msg,
                            e::unpacker up)
{
    wtf::response_returncode rc;
    uint64_t sid;
    uint64_t bid;
    uint32_t offset;
    ssize_t ret = 0;


    up = up >> bid >> offset;
    e::slice data = up.as_slice();
    sid = m_us.get();

    ret = m_blockman.update_block(data, offset, sid, bid); 

    if (ret < data.size())
    {
        rc = wtf::RESPONSE_SERVER_ERROR;
    }
    else
    {
        rc = wtf::RESPONSE_SUCCESS;
    }

    LOG(INFO) << "Returning " << rc << " to client.";


    size_t sz = COMMAND_HEADER_SIZE + 
                sizeof(uint64_t) + /* token */
                sizeof(uint64_t);  /* block id */
    std::auto_ptr<e::buffer> resp(e::buffer::create(sz));
    e::buffer::packer pa = resp->pack_at(BUSYBEE_HEADER_SIZE);
    pa = pa << RESP_UPDATE << nonce << rc 
            << bid;
    send(conn, resp);
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

void
daemon :: periodic_stat(uint64_t)
{
    m_blockman.stat();
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
