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

void 
worker_thread(const armnod::argparser& _f,
        const armnod::argparser& _v)
{
    try
    {

        wtf::Client cl(_connect_host, _connect_port, _hyper_host, _hyper_port);
            std::string v = "HELLO WORLD";
            wtf_client_returncode status = WTF_CLIENT_GARBAGE;
            wtf_client_returncode lstatus = WTF_CLIENT_GARBAGE;
            std::string f = "helloworld";
            int64_t fd;
            int64_t reqid = -1;
            
            //CREATE AND WRITE

            /* create a random file */
            reqid = cl.open(f.data(), O_CREAT | O_RDWR, mode_t(0777), 1, _block_size, &fd, &status);
            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "failed to open file");
            }

            reqid = cl.loop(reqid, -1, &lstatus);
            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "failed to open file");
            }
            
            /* Write some stuff to the random file, in random size chunks. */
            size_t sz = v.size();
            reqid = cl.write(fd, v.data(), &sz, 1, &status);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;

            }

            reqid = cl.loop(reqid, -1, &status);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;

            }


            wtf_client_returncode rc = WTF_CLIENT_GARBAGE;

            /* close the random file */
            reqid = cl.close(fd, &rc);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;
                 
            }

            //LSEEK

            /* create a random file */
            reqid = cl.open(f.data(), O_RDWR, mode_t(0777), 1, _block_size, &fd, &status);
            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "failed to open file");
            }

            reqid = cl.loop(reqid, -1, &lstatus);
            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "failed to open file");
            }

            cl.lseek(fd, 2, SEEK_SET, &status);
            
            /* Write some stuff to the random file, in random size chunks. */
            sz = 1;
            reqid = cl.write(fd, "X", &sz, 1, &status);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;

            }

            reqid = cl.loop(reqid, -1, &status);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;

            }


            rc = WTF_CLIENT_GARBAGE;

            /* close the random file */
            reqid = cl.close(fd, &rc);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;
                 
            }


            //READ BACK

            reqid = cl.open(f.data(), O_RDWR, mode_t(0777), 1, _block_size, &fd, &status);
            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "failed to open file");
            }

            reqid = cl.loop(reqid, -1, &lstatus);
            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "failed to open file");
            }

            /* Write some stuff to the random file, in random size chunks. */
            char buf[11];
            size_t buf_sz = 11;
            reqid = cl.read(fd, buf, &buf_sz, &status);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");
            }

            reqid = cl.loop(reqid, -1, &status);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;

            }

            if(std::string(buf, buf_sz) != std::string("HEXLO WORLD"))
            {
                WTF_TEST_FAIL(0, std::string(buf) << "!=" << std::string("HEXLO WORLD"));
            }

            rc = WTF_CLIENT_GARBAGE;

            /* close the random file */
            reqid = cl.close(fd, &rc);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;
                 
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
    ap.arg().name('b', "block-size")
        .description("size of blocks")
        .as_long(&_block_size);
    ap.arg().long_name("max-write-length")
        .description("maximum size of write ops")
        .as_long(&_max_write);
    ap.arg().long_name("min-write-length")
        .description("minimum size of write ops")
        .as_long(&_min_write);
    ap.arg().long_name("max-read-length")
        .description("maximum size of read ops")
        .as_long(&_max_read);
    ap.arg().long_name("min-read-length")
        .description("minimum size of read ops")
        .as_long(&_min_read);
    ap.arg().name('q', "quiet")
            .description("silence all output")
            .set_true(&_quiet);
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

    for (size_t i = 0; i < _threads; ++i)
    {
        thread_ptr t(new po6::threads::thread(std::tr1::bind(worker_thread, file_parser, value_parser)));
        threads.push_back(t);
        t->start();
    }

    for (size_t i = 0; i < threads.size(); ++i)
    {
        threads[i]->join();
    }

    return EXIT_SUCCESS;
}
