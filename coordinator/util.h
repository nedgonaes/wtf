// Copyright (c) 2012-2013, Cornell University
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

#ifndef wtf_coordinator_util_h_
#define wtf_coordinator_util_h_

// WTF 
#include "common/coordinator_returncode.h"

namespace wtf __attribute__ ((visibility("hidden")))
{
static inline void
generate_response(replicant_state_machine_context* ctx, coordinator_returncode x)
{
    const char* ptr = NULL;

    switch (x)
    {
        case COORD_SUCCESS:
            ptr = "\x22\x80";
            break;
        case COORD_MALFORMED:
            ptr = "\x22\x81";
            break;
        case COORD_DUPLICATE:
            ptr = "\x22\x82";
            break;
        case COORD_NOT_FOUND:
            ptr = "\x22\x83";
            break;
        case COORD_UNINITIALIZED:
            ptr = "\x22\x85";
            break;
        case COORD_NO_CAN_DO:
            ptr = "\x22\x87";
            break;
        default:
            ptr = "\xff\xff";
            break;
    }

    replicant_state_machine_set_response(ctx, ptr, 2);
}

#define INVARIANT_BROKEN(X) \
    fprintf(log, "invariant broken at " __FILE__ ":%d:  %s\n", __LINE__, X "\n")
}
#endif // wtf_coordinator_util_h_
