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

// C
#include <cstdlib>

// STL
#include <string>
#include <sstream>
#include <fstream>
#include <vector>

// po6
#include <po6/error.h>

// e
#include <e/guard.h>

// WTF 
#include "common/network_msgtype.h"
#include "client/client.h"
#include "tools/common.h"

static const char* _file = "readtestdata";

int
main(int argc, const char* argv[])
{
    wtf::connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS]");
    int rc;

    ap.add("Connect to a cluster:", conn.parser());

    ap.arg().name('f', "file")
        .description("input data for test (default: writetestdata)")
        .metavar("file")
        .as_string(&_file);

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    try
    {
        wtf_client r(conn.coord_host(), conn.coord_port(), conn.hyper_host(), conn.hyper_port());
        std::string s;
        std::ifstream f(_file);

        while (std::getline(f, s))
        {
            wtf_returncode re = WTF_GARBAGE;
            wtf_returncode le = WTF_GARBAGE;
            int64_t rid = 0;
            int64_t lid = 0;

            wtf::wtf_network_msgtype msgtype = wtf::WTFNET_NOP;
            
            uint32_t item_sz;
            std::string path;

            std::stringstream ss(s);

            if (!std::getline(ss, path, ' '))
            {
                std::cerr << "Invalid input file.  Aborting." << std::endl;
            }

            ss >> item_sz;
            char* item = new char[item_sz];

            int64_t fd = r.open(path.c_str(), O_CREAT | O_RDWR, 777);

            std::cout << "FD is " << fd << std::endl;

            rid = r.read(fd, item, &item_sz, &re);

            if (rid < 0)
            {
                std::cerr << "could not send request: " << r.last_error_desc()
                          << " (" << re << ")" << std::endl;
                return EXIT_FAILURE;
            }

            std::cout << "Flushing " << fd << std::endl;

            lid = r.flush(fd, &re);

            if (lid < 0)
            {
                std::cerr << "could not loop: " << r.last_error_desc()
                          << " (" << le << ")" << std::endl;
                return EXIT_FAILURE;
            }

            if (re != WTF_SUCCESS)
            {
                std::cerr << "could not process request: " << r.last_error_desc()
                          << " (" << re << ")" << std::endl;
                return EXIT_FAILURE;
            }

            std::cout << "rid: " << rid << " lid: " << lid << " " << re << std::endl;

            std::cout << std::string(item) << std::endl;
            delete [] item;
        }

        std::cout << "Done flushing." << std::endl;
        wtf_returncode e = WTF_SUCCESS; 

        return EXIT_SUCCESS;
    }
    catch (po6::error& e)
    {
        std::cerr << "system error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
