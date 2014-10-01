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
static long _number = 1000;
static long _connect_port = 1981;
static long _hyper_port = 1982;
static long _concurrent = 50;
static long _block_size = 65536;
static long _replication = 3;
static long _size = 10;
static const char* _file = "foo";
static const char* _connect_host = "127.0.0.1";
static const char* _hyper_host = "127.0.0.1";
static const char* _benchmark = "write";

#define BILLION (1000ULL * 1000ULL * 1000ULL)

#define WTF_TEST_FAIL(TESTNO, REASON) \
    do { \
        if (!_quiet) std::cout << "Test " << TESTNO << ":  [\x1b[31mFAIL\x1b[0m]\n" \
        << "location: " << __FILE__ << ":" << __LINE__ << "\n" \
        << "reason:  " << REASON << "\n"; \
        abort(); \
    } while (0)

    void 
do_write_benchmark()
{
    try
    {

        wtf::Client cl(_connect_host, _connect_port, _hyper_host, _hyper_port);
        char* buf = new char[_size*sizeof(uint32_t)];
        int64_t reqid = -1;
        int64_t fd;
        wtf_client_returncode status = WTF_CLIENT_GARBAGE;
        wtf_client_returncode lstatus = WTF_CLIENT_GARBAGE;

        reqid = cl.open(_file, O_CREAT | O_RDWR, mode_t(0777), _replication, _block_size, &fd, &status);
        if (reqid < 0)
        {
            std::cout << "Can't open file: " << status << std::endl;
            abort();
        }

        reqid = cl.loop(reqid, -1, &lstatus);
        if (reqid < 0)
        {
            std::cout << "Can't open file: " << lstatus << std::endl;
            abort();
        }

        uint32_t j = 0;

        for (int i = 0; i < _number; ++i)
        {

            size_t sz = _size*sizeof(uint32_t);
            uint32_t* tempbuf = (uint32_t*)buf;

            if (1){
            for (int k = 0; k < _size; ++k)
            {
                *tempbuf = j++ % 257;
                tempbuf++;
            }
            }

            /* Write some stuff */
            reqid = cl.write(fd, buf, &sz, 1, &status);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;

            }

            if (0)
            {
                reqid = cl.loop(reqid, -1, &status);
                if (reqid < 0)
                {
                    WTF_TEST_FAIL(0, "XXX");;

                }
            }

        }

        /* close the random file */
        cl.close(fd, &status);
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


    void 
do_read_benchmark()
{
    //XXX: Change this to do reads.
    try
    {

        wtf::Client cl(_connect_host, _connect_port, _hyper_host, _hyper_port);
        char* buf = new char[_size];
        int64_t reqid = -1;
        int64_t fd;
        wtf_client_returncode status = WTF_CLIENT_GARBAGE;
        wtf_client_returncode lstatus = WTF_CLIENT_GARBAGE;

        reqid = cl.open(_file, O_CREAT | O_RDWR, mode_t(0777), _replication, _block_size, &fd, &status);
        if (reqid < 0)
        {
            std::cout << "Can't open file: " << status << std::endl;
            abort();
        }

        reqid = cl.loop(reqid, -1, &lstatus);
        if (reqid < 0)
        {
            std::cout << "Can't open file: " << lstatus << std::endl;
            abort();
        }

        char j = 0;

        for (int i = 0; i < _number; ++i)
        {

            size_t sz = _size;
            char* tempbuf = buf;

            for (int k = 0; k < sz; ++k)
            {
                *tempbuf = j++;
                tempbuf++;
            }

            /* Write some stuff */
            reqid = cl.write(fd, buf, &sz, 1, &status);

            if (reqid < 0)
            {
                WTF_TEST_FAIL(0, "XXX");;

            }

            if (0)
            {
                reqid = cl.loop(reqid, -1, &status);
                if (reqid < 0)
                {
                    WTF_TEST_FAIL(0, "XXX");;

                }
            }

        }

        /* close the random file */
        cl.close(fd, &status);
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
    ap.arg().name('b', "block-size")
        .description("size of blocks")
        .as_long(&_block_size);
    ap.arg().long_name("size")
        .description("size of each write")
        .as_long(&_size);
    ap.arg().long_name("replication")
        .description("replication of each write")
        .as_long(&_replication);
     ap.arg().name('q', "quiet")
        .description("silence all output")
        .set_true(&_quiet);
    ap.arg().name('f', "file")
        .description("file to write to in WTF")
        .metavar("f")
        .as_string(&_file);
    ap.arg().long_name("benchmark")
        .description("benchmark to run")
        .as_string(&_benchmark);



    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (strcmp(_benchmark, "read") == 0)
    {
        do_read_benchmark();
    }
    else
    {
        do_write_benchmark();
    }

    return EXIT_SUCCESS;
}
