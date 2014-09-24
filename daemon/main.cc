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

// Popt
#include <popt.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/hostname.h>

// e
#include <e/guard.h>

// BusyBee
#include <busybee_utils.h>

// WTF
#include "daemon/daemon.h"

static bool _daemonize = false;
static const char* _data = "./data";
static const char* _metadata = "./metadata";
static const char* _log = NULL;
static const char* _listen_host = "auto";
static unsigned long _listen_port = 2013;
static po6::net::ipaddr _listen_ip;
static bool _listen = false;
static const char* _coordinator_host = "127.0.0.1";
static unsigned long _coordinator_port = 1982;
static bool _coordinator = false;
static long _threads = 1;

extern "C"
{

static struct poptOption popts[] = {
    POPT_AUTOHELP
    {"daemon", 'd', POPT_ARG_NONE, NULL, 'd',
     "run wtf in the background", 0},
    {"foreground", 'f', POPT_ARG_NONE, NULL, 'f',
     "run wtf in the foreground", 0},
    {"data", 'M', POPT_ARG_STRING, &_metadata, 'M',
     "store persistent state in this directory (default: ./metadata)",
     "dir"},
    {"data", 'D', POPT_ARG_STRING, &_data, 'D',
     "store persistent state in this directory (default: ./data)",
     "dir"},
    {"log", 'L', POPT_ARG_STRING, &_log, 'O',
     "store persistent state in this directory (default: --data)",
     "dir"},
    {"listen", 'l', POPT_ARG_STRING, &_listen_host, 'l',
     "listen on a specific IP address (default: auto)",
     "IP"},
    {"listen-port", 'p', POPT_ARG_LONG, &_listen_port, 'L',
     "listen on an alternative port (default: 1983)",
     "port"},
    {"coordinator", 'c', POPT_ARG_STRING, &_coordinator_host, 'c',
     "join an existing WTF cluster through IP address or hostname",
     "addr"},
    {"coordinator-port", 'P', POPT_ARG_LONG, &_coordinator_port, 'C',
     "connect to an alternative port on the coordinator (default: 2013)",
     "port"},
    {"threads", 't', POPT_ARG_LONG, &_threads, 't',
     "the number of threads which will handle network traffic (default: 1)",
     "N"},
    POPT_TABLEEND
};

} // extern "C"

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon); g.use_variable();
    int rc;
    po6::net::location listen;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            case 'd':
                _daemonize = true;
                break;
            case 'f':
                _daemonize = false;
                break;
            case 'D':
                break;
            case 'M':
                break;
            case 'l':
                try
                {
                    _listen = true;

                    if (strcmp(_listen_host, "auto") == 0)
                    {
                        break;
                    }

                    _listen_ip = po6::net::ipaddr(_listen_host);
                    break;
                }
                catch (po6::error& e)
                {
                }
                catch (std::invalid_argument& e)
                {
                }

                listen = po6::net::hostname(_listen_host, 0).lookup(AF_UNSPEC, IPPROTO_TCP);

                if (listen == po6::net::location())
                {
                    std::cerr << "cannot interpret listen address as hostname or IP address" << std::endl;
                    return EXIT_FAILURE;
                }

                _listen_ip = listen.address;
                break;
            case 'L':
                if (_listen_port >= (1 << 16))
                {
                    std::cerr << "port number to listen on is out of range" << std::endl;
                    return EXIT_FAILURE;
                }

                _listen = true;
                break;
            case 'c':
                _coordinator = true;
                break;
            case 'C':
                if (_coordinator_port >= (1 << 16))
                {
                    std::cerr << "port number to coordinator to is out of range" << std::endl;
                    return EXIT_FAILURE;
                }

                _coordinator = true;
                break;
            case 't':
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

    //FLAGS_logbufsecs = 0;
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    try
    {
        wtf::daemon d;

        if (strcmp(_listen_host, "auto") == 0)
        {
            if (!busybee_discover(&_listen_ip))
            {
                std::cerr << "cannot automatically discover local address; specify one manually" << std::endl;
                return EXIT_FAILURE;
            }
        }

        po6::pathname data(_data);
        po6::pathname log(_log ? _log : _data);
        po6::pathname metadata(_metadata);
        po6::net::location bind_to(_listen_ip, _listen_port);
        po6::net::hostname coord(_coordinator_host, _coordinator_port);

        return d.run(_daemonize, data, log, metadata, _listen, bind_to, _coordinator, coord, _threads);
    }
    catch (po6::error& e)
    {
        std::cerr << "system error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
