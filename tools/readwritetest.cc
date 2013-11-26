// Copyright (c) 2013, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met: //
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Replicant nor the names of its contributors may be
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

// POSIX
#include <errno.h>

// STL
#include <tr1/unordered_map>
#include <tr1/memory>

// po6
#include <po6/error.h>
#include <po6/threads/thread.h>

// e
#include <e/guard.h>
#include <e/time.h>
#include <numbers.h>
#include <armnod.h>

// WTF 
#include "common/network_msgtype.h"
#include "client/wtf.h"

static long _done = 0;
static long _number = 1000;
static long _threads = 1;
static long _backup = 0;
static long _connect_port = 1982;
static long _hyper_port = 1981;
static long _concurrent = 50;
static const char* _output = "wtf-sync-benchmark.log";
static const char* _dir = ".";
static const char* _connect_host = "127.0.0.1";
static const char* _hyper_host = "127.0.0.1";

#define BILLION (1000ULL * 1000ULL * 1000ULL)

static uint64_t
get_random()
{
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0)
    {
        abort();
        return 0xcafebabe;
    }

    uint64_t ret;

    if (sysrand.read(&ret, sizeof(ret)) != sizeof(ret))
    {
        abort();
        return 0xdeadbeef;
    }

    return ret;
}

void 
worker_thread( numbers::throughput_latency_logger* tll,
        const armnod::argparser& _f,
        const armnod::argparser& _v)
{
    armnod::generator file(armnod::argparser(_f).config());
    armnod::generator val(armnod::argparser(_v).config());
    file.seed(get_random());
    val.seed(get_random());
    numbers::throughput_latency_logger::thread_state ts;
    tll->initialize_thread(&ts);

    try
    {
        std::string v = val();

        wtf_client cl(_connect_host, _connect_port, _hyper_host, _hyper_port);
        while (__sync_fetch_and_add(&_done, 1) < _number)
        {
            wtf_returncode status = WTF_GARBAGE;
            std::string f = file();
            std::cout << "File: " << f << std::endl;
            int64_t fd = cl.open(f.data());

            tll->start(&ts, 1);
            int64_t reqid = cl.write(fd, v.data(), v.size(), 1, &status);

            if (reqid < 0)
            {
                std::cerr << "wtf_client->write encountered" << status << std::endl;
                return;
            }

            wtf_returncode rc = WTF_GARBAGE;

            reqid = cl.flush(fd, &rc);
            cl.close(fd, &rc);

            tll->finish(&ts);

            if (reqid < 0)
            {
                std::cerr << "wtf_loop encountered " << rc << std::endl;
                return; 
            }

            //std::string d(v.size(), '0');
            char* dd = new char[v.size()];
            std::cout << "output pointer = " << (void*)dd << std::endl;
            fd = cl.open(f.data());
            reqid = cl.read(fd, dd, v.size(), &status);


            if (reqid < 0)
            {
                std::cerr << "wtf_client->read encountered " << rc << std::endl;
                return; 
            }

            reqid = cl.flush(fd, &rc);

            if (reqid < 0)
            {
                std::cerr << "wtf_loop encountered " << rc << std::endl;
                return; 
            }

            std::string d(dd, v.size());

            if (v.compare(d) != 0)
            {
                std::cerr << "Strings don't match" << std::endl;
                e::slice slc1(v.data(), v.size());
                e::slice slc2(d.data(), d.size());
                std::cerr << slc1.hex() << std::endl;
                std::cerr << " != " << std::endl;
                std::cerr  << slc2.hex() << std::endl;
                abort();
            }

            cl.close(fd, &rc);
            
        }
    }
    catch (po6::error& e)
    {
        std::cerr << "system error: " << e.what() << std::endl;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
    }

    tll->terminate_thread(&ts);
}

int
main(int argc, const char* argv[])
{
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('p', "port")
        .description("port on the wtf coordinator")
        .metavar("p")
        .as_long(&_connect_port);
    ap.arg().name('P', "Port")
        .description("port on the hyperdex coordinator")
        .metavar("P")
        .as_long(&_hyper_port);
    ap.arg().name('h', "host")
        .description("address of wtf coordinator")
        .metavar("h")
        .as_string(&_connect_host);
    ap.arg().name('H', "host")
        .description("address of hyperdex coordinator")
        .metavar("H")
        .as_string(&_hyper_host);
    ap.arg().name('n', "number")
        .description("perform N operations against the database (default: 1000000)")
        .metavar("N")
        .as_long(&_number);
    ap.arg().name('t', "threads")
        .description("run the test with T concurrent threads (default: 1)")
        .metavar("T")
        .as_long(&_threads);
    ap.arg().name('o', "output")
        .description("output file for benchmark results (default: benchmark.log)")
        .as_string(&_output);
    ap.arg().name('c', "concurrent")
        .description("number of concurrent ops (default=50)")
        .as_long(&_concurrent);
    armnod::argparser file_parser("file-");
    armnod::argparser value_parser("value-");
    ap.add("Filename Generation:", file_parser.parser());
    ap.add("Value Generation:", value_parser.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;
    std::vector<thread_ptr> threads;

    numbers::throughput_latency_logger tll;
    if (!tll.open(_output))
    {
        std::cerr << "could not open log: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    for (size_t i = 0; i < _threads; ++i)
    {
        thread_ptr t(new po6::threads::thread(std::tr1::bind(worker_thread, &tll, file_parser, value_parser)));
        threads.push_back(t);
        t->start();
    }

    for (size_t i = 0; i < threads.size(); ++i)
    {
        threads[i]->join();
    }

    if (!tll.close())
    {
        std::cerr << "could not close log: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
