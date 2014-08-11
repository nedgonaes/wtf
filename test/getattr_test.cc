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
#include <tr1/random>

// po6
#include <po6/error.h>
#include <po6/threads/thread.h>

// e
#include <e/guard.h>
#include <e/time.h>
#include <e/slice.h>
#include <armnod.h>

// WTF 
#include <wtf/client.hpp>

static bool _quiet = false;
static long _done = 0;
static long _number = 1000;
static long _threads = 1;
static long _backup = 0;
static long _connect_port = 1981;
static long _hyper_port = 1982;
static long _concurrent = 50;
static long _block_size = 4096;
static long _min_read = 4096;
static long _max_read = 4096;
static long _min_write = 4096;
static long _max_write = 4096;
static const char* _output = "wtf-sync-benchmark.log";
static const char* _dir = ".";
static const char* _connect_host = "127.0.0.1";
static const char* _hyper_host = "127.0.0.1";

#define BILLION (1000ULL * 1000ULL * 1000ULL)

#define WTF_TEST_SUCCESS(TESTNO) \
    do { \
        if (!_quiet) std::cout << "Test " << TESTNO << ":  [\x1b[32mOK\x1b[0m]\n"; \
    } while (0)

#define WTF_TEST_FAIL(TESTNO, REASON) \
    do { \
        if (!_quiet) std::cout << "Test " << TESTNO << ":  [\x1b[31mFAIL\x1b[0m]\n" \
                  << "location: " << __FILE__ << ":" << __LINE__ << "\n" \
                  << "reason:  " << REASON << "\n"; \
    abort(); \
    } while (0)

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

using std::tr1::uniform_int;
using std::tr1::variate_generator;
using std::tr1::mt19937;

void 
worker_thread(const armnod::argparser& _f)
{
    armnod::generator file(armnod::argparser(_f).config());
    file.seed(get_random());

    try
    {

        wtf::Client cl(_connect_host, _connect_port, _hyper_host, _hyper_port);
        wtf_client_returncode status = WTF_CLIENT_GARBAGE;
        wtf_client_returncode lstatus = WTF_CLIENT_GARBAGE;
        std::string f = file();
        int64_t fd;
        int64_t reqid = -1;

        std::cout << "creating directory " << std::string(f.data()) << std::endl;

        /* create a random file */
        reqid = cl.mkdir(f.data(), mode_t(0777), &status);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to open file");
        }

        reqid = cl.loop(reqid, -1, &lstatus);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to open file");
        }

        struct wtf_file_attrs fa;
        reqid = cl.getattr(f.data(), &fa, &status);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to open file");
        }

        reqid = cl.loop(reqid, -1, &lstatus);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to open file");
        }

        if (fa.is_dir != 1)
        {
            WTF_TEST_FAIL(0, "directory is not marked as a directory.");
        }

        /* create a random file */
        reqid = cl.open("/foo", O_CREAT | O_RDWR, mode_t(0777), 3, _block_size, &fd, &status);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to open file");
        }

        reqid = cl.loop(reqid, -1, &lstatus);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to open file");
        }

        size_t sz = 12;
        reqid = cl.write(fd, "hello world", &sz, 1, &status);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to write file");
        }

        reqid = cl.loop(reqid, -1, &lstatus);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to open file");
        }

        reqid = cl.close(fd, &status);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "XXX");;

        }

        reqid = cl.getattr("/foo", &fa, &status);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to open file");
        }

        reqid = cl.loop(reqid, -1, &lstatus);
        if (reqid < 0)
        {
            WTF_TEST_FAIL(0, "failed to open file");
        }
 
        if (fa.size != 12)
        {
            WTF_TEST_FAIL(0, "file length is incorrect.");
        }

        WTF_TEST_SUCCESS(0);      
    }
    catch (po6::error& e)
    {
        WTF_TEST_FAIL(0, "system error: " << e.what());
    }
    catch (std::exception& e)
    {
        WTF_TEST_FAIL(0, "error: " << e.what());
    }
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
    ap.arg().name('q', "quiet")
            .description("silence all output")
            .set_true(&_quiet);
    armnod::argparser file_parser("file-");
    ap.add("Filename Generation:", file_parser.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;
    std::vector<thread_ptr> threads;

    for (size_t i = 0; i < _threads; ++i)
    {
        thread_ptr t(new po6::threads::thread(std::tr1::bind(worker_thread, file_parser)));
        threads.push_back(t);
        t->start();
    }

    for (size_t i = 0; i < threads.size(); ++i)
    {
        threads[i]->join();
    }

    return EXIT_SUCCESS;
}
