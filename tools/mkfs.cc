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
#include <pwd.h>

// E
#include <e/popt.h>

// HyperDex
#include <hyperdex/client.hpp>

//WTF
#include "tools/common.h"

hyperdex_client_returncode
hyperdex_wait_for_result(hyperdex::Client& h, int64_t reqid, hyperdex_client_returncode& status)
{
    while(1)
    {
        hyperdex_client_returncode lstatus;
        int64_t id = h.loop(-1, &lstatus);

        if (id < 0) 
        {
            return lstatus;
        }

        if (id != reqid){
            std::cout << "ERROR: Hyperdex returned id:" << id << " status:"<< lstatus << std::endl;
            std::cout << "expected id:" << reqid<< std::endl;
        }
        else
        {
            //std::cout << "Hyperdex returned " << status << std::endl;
            return status;
        }
    } 
}


int
main(int argc, const char* argv[])
{   
    wtf::connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS]");

    ap.add("Connect to a cluster:", conn.parser());


    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    hyperdex::Client h(conn.hyper_host(), conn.hyper_port());

    hyperdex_client_returncode status;
    int64_t ret = -1;

    struct hyperdex_client_attribute attr[5];

    uint64_t mode = 0777;
    uint64_t t = time(NULL);
    uint64_t dir = 1;

    attr[0].attr = "mode";
    attr[0].value = (const char*)&mode;
    attr[0].value_sz = sizeof(mode);
    attr[0].datatype = HYPERDATATYPE_INT64;

    attr[1].attr = "directory";
    attr[1].value = (const char*)&dir;
    attr[1].value_sz = sizeof(dir);
    attr[1].datatype = HYPERDATATYPE_INT64;

    attr[2].attr = "time";
    attr[2].value = (const char*)&t;
    attr[2].value_sz = sizeof(t);
    attr[2].datatype = HYPERDATATYPE_INT64;
    
    struct passwd* p = getpwuid(geteuid());
    attr[3].attr = "owner";
    attr[3].value = p->pw_name,
    attr[3].value_sz = 5;
    attr[3].datatype = HYPERDATATYPE_STRING;
     
    attr[4].attr = "group";
    attr[4].value = p->pw_name;
    attr[4].value_sz = 6;
    attr[4].datatype = HYPERDATATYPE_STRING;
    
    ret = h.put("wtf", "/", strlen("/"), attr, 5, &status);
    if (ret < 0)
    {
        std::cerr << "mkfs failed with " << status << std::endl;
        return -1;
    }

    hyperdex_client_returncode res = hyperdex_wait_for_result(h, ret, status);

    if (res != HYPERDEX_CLIENT_SUCCESS)
    {
        std::cerr << "mkfs failed with " << res << std::endl;
        return -1;
    }
    else
    {
        return 0;
    }
}
