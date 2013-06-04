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
#include <vector>

// po6
#include <po6/error.h>

// e
#include <e/guard.h>

// WTF 
#include "common/network_msgtype.h"
#include "client/wtf.h"
#include "tools/common.h"

static struct poptOption popts[] = {
    POPT_AUTOHELP
    CONNECT_TABLE
    POPT_TABLEEND
};

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon); g.use_variable();
    poptSetOtherOptionHelp(poptcon, "[OPTIONS]");
    int rc;
    bool h = false;
    bool p = false;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            case 'h':
                if (!check_host())
                {
                    return EXIT_FAILURE;
                }
                h = true;
                break;
            case 'p':
                if (!check_port())
                {
                    return EXIT_FAILURE;
                }
                p = true;
                break;
            case POPT_ERROR_NOARG:
            case POPT_ERROR_BADOPT:
            case POPT_ERROR_BADNUMBER:
            case POPT_ERROR_OVERFLOW:
                std::cerr << poptStrerror(rc) << " " << poptBadOption(poptcon, 0) << std::endl;
                return EXIT_FAILURE;
            case POPT_ERROR_OPTSTOODEEP:
            case POPT_ERROR_BADQUOTE:
            case POPT_ERROR_ERRNO:
            default:
                std::cerr << "logic error in argument parsing" << std::endl;
                return EXIT_FAILURE;
        }
    }

    if (!h)
    {
        _connect_host = "127.0.0.1";
    }
    
    if (!p)
    {
        _connect_port = 1982;
    }

    const char** args = poptGetArgs(poptcon);
    size_t num_args = 0;

    while (args && args[num_args])
    {
        ++num_args;
    }

    if (num_args != 0)
    {
        std::cerr << "extra arguments provided\n" << std::endl;
        poptPrintUsage(poptcon, stderr, 0);
        return EXIT_FAILURE;
    }

    try
    {
        wtf_client r(_connect_host, _connect_port);
        std::string s;

        while (std::getline(std::cin, s))
        {
            wtf_returncode re = WTF_GARBAGE;
            wtf_returncode le = WTF_GARBAGE;
            int64_t rid = 0;
            int64_t lid = 0;
            const char* output;
            size_t output_sz;

            wtf::wtf_network_msgtype msgtype = wtf::WTFNET_NOP;
            
            std::string item;
            std::string path;

            std::stringstream ss(s);
            if (std::getline(ss, item, ' '))
            {
                if (item.compare("write")==0)
                {
                    msgtype = wtf::WTFNET_PUT;
                }
                if (item.compare("read")==0)
                {
                    msgtype = wtf::WTFNET_GET;
                }
            }
            else
            {
                std::cerr << "Invalid input file.  Aborting." << std::endl;
            }

            if (!std::getline(ss, path, ' '))
            {
                std::cerr << "Invalid input file.  Aborting." << std::endl;
            }

            if (!std::getline(ss, item, ' '))
            {
                std::cerr << "Invalid input file.  Aborting." << std::endl;
            }

            int64_t fd = r.open(path.c_str());

            std::cout << "FD is " << fd << std::endl;

            rid = r.write(fd,item.c_str(), item.size()+1, &re);

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

            //e::slice out(output, output_sz);
            //std::cout << "RESPONSE: " << out.hex() << std::endl;
            //wtf_destroy_output(output, output_sz);
        }

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
