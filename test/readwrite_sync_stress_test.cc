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
worker_thread(const armnod::argparser& _f,
        const armnod::argparser& _v)
{
    armnod::generator file(armnod::argparser(_f).config());
    armnod::generator val(armnod::argparser(_v).config());
    file.seed(get_random());
    val.seed(get_random());

    try
    {

        wtf::Client cl(_connect_host, _connect_port, _hyper_host, _hyper_port);
        while (__sync_fetch_and_add(&_done, 1) < _number)
        {
            std::string v = val();
            wtf_client_returncode status = WTF_CLIENT_GARBAGE;
            std::string f = file();
            
            /* create a random file */
            int64_t fd = cl.open(f.data(), O_CREAT | O_RDWR, mode_t(0777), 3, _block_size, &status);
            if (fd < 0)
            {
                WTF_TEST_FAIL(0, "failed to open file");
            }

            
            mt19937 eng(get_random()); 
            uniform_int<size_t> write_distribution(_min_write, _max_write);
            uniform_int<size_t> read_distribution(_min_read, _max_read);
            variate_generator<mt19937, uniform_int<size_t> > 
                read_size(eng, read_distribution);
            variate_generator<mt19937, uniform_int<size_t> > 
                write_size(eng, write_distribution);

            /* Write some stuff to the random file, in random size chunks. */
            size_t rem = v.size();
            int64_t reqid = -1;
            size_t sz = 0;
            while (rem > 0)
            {
                sz = write_size();
                sz = std::min(sz, rem);
                reqid = cl.write(fd, v.data() + v.size() - rem, &sz, 1, &status);

                if (reqid < 0)
                {
                    WTF_TEST_FAIL(0, "XXX");;

                }

                reqid = cl.loop(reqid, -1, &status);

                if (reqid < 0)
                {
                    WTF_TEST_FAIL(0, "XXX");;

                }

                rem -= sz;
            }

            wtf_client_returncode rc = WTF_CLIENT_GARBAGE;

            /* close the random file */
            cl.close(fd, &rc);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;
                 
            }

            /* open the file */
            char* dd = new char[v.size()];
            fd = cl.open(f.data(), O_RDWR, 0777, 3, _block_size, &status);

            if (fd < 0)
            {
                WTF_TEST_FAIL(0, "unable to open file " << f << cl.error_location() << ":" << cl.error_message());
            }

            /* read the file in random size chunks. */
            rem = v.size();
            reqid = -1;
            sz = 0;
            while (rem > 0)
            {
                sz = read_size();
                sz = std::min(sz, rem);

                reqid = cl.read(fd, dd + v.size() - rem, &sz, &status);

                if (reqid < 0)
                {
                    WTF_TEST_FAIL(0, "XXX");;

                }

                reqid = cl.loop(reqid, -1, &status);

                if (reqid < 0)
                {
                    WTF_TEST_FAIL(0, reqid);;

                }

                rem -= sz;
            }

            /* compare the contents of the write with the contents returned from read */
            std::string d(dd, v.size());

            if (v.compare(d) != 0)
            {
                e::slice slc1(v.data(), v.size());
                e::slice slc2(d.data(), d.size());
                WTF_TEST_FAIL(0, "Strings don't match"
                                  << slc1.hex() << std::endl
                                  << " != " << std::endl
                                  << slc2.hex());
            }

            /* close the file */
            cl.close(fd, &rc);

            delete [] dd;
            
            WTF_TEST_SUCCESS(0);      
        }
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
