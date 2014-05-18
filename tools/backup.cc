// Copyright (c) 2014, Cornell University
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

// C/C++
#include <stdio.h>
#include <string.h>

// E
#include <e/popt.h>

// HyperDex
#include <hyperdex/client.hpp>

// WTF 
#include "client/rereplicate.h"

using namespace std;

static long _connect_port = 1981;
static const char* _connect_host = "127.0.0.1";
static long _hyper_port = 1982;
static const char* _hyper_host = "127.0.0.1";

static const char* _sid = NULL;
static const char* _path = NULL;

int
main(int argc, const char* argv[])
{
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('h', "host")
        .description("address of wtf coordinator")
        .metavar("h")
        .as_string(&_connect_host);
    ap.arg().name('p', "port")
        .description("port on the wtf coordinator")
        .metavar("p")
        .as_long(&_connect_port);
    ap.arg().name('H', "Host")
        .description("address of hyperdex coordinator")
        .metavar("H")
        .as_string(&_hyper_host);
    ap.arg().name('P', "Port")
        .description("port on the hyperdex coordinator")
        .metavar("P")
        .as_long(&_hyper_port);
    ap.arg().name('s', "server_id")
        .description("server ID of daemon to check")
        .metavar("s")
        .as_string(&_sid);
    ap.arg().name('f', "file")
        .description("file path to backup")
        .metavar("F")
        .as_string(&_path);

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (_sid == NULL)
    {
        cerr << "Must specify a server ID" << endl;
        return EXIT_FAILURE;
    }

    wtf::rereplicate re(_connect_host, _connect_port, _hyper_host, _hyper_port);
    uint64_t server_id = strtoull(_sid, NULL, 10);

    int64_t ret;
    if (_path != NULL)
    {
        ret = re.replicate(_path, server_id);
    }
    else
    {
        hyperdex::Client* h = new hyperdex::Client(_hyper_host, _hyper_port);
        hyperdex_client_returncode status;
        const struct hyperdex_client_attribute* attrs;
        size_t attrs_sz;
        int64_t retval;

        struct hyperdex_client_attribute_check check;
        check.attr = "path";
        check.value = "^";
        check.value_sz = strlen(check.value);
        check.datatype = HYPERDATATYPE_STRING;
        check.predicate = HYPERPREDICATE_REGEX;

        retval = h->search("wtf", &check, 1, &status, &attrs, &attrs_sz);
        while (status != HYPERDEX_CLIENT_SEARCHDONE && status != HYPERDEX_CLIENT_NONEPENDING)
        {
            retval = h->loop(-1, &status);
            for (size_t i = 0; i < attrs_sz; ++i)
            {
                if (strcmp(attrs[i].attr, "path") == 0)
                {
                    string path(attrs[i].value, attrs[i].value_sz);
                    re.replicate(path.c_str(), server_id);
                }
            }
        }
        cout << "\nSearch finished" << endl;

        return 0;
    }

    return ret;
}
