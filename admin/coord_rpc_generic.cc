// Copyright (c) 2013, Cornell University
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

// e
#include <e/endian.h>

// WTF
#include "common/coordinator_returncode.h"
#include "admin/admin.h"
#include "admin/coord_rpc_generic.h"

using wtf::coord_rpc_generic;

coord_rpc_generic :: coord_rpc_generic(uint64_t id,
                                       wtf_admin_returncode* s,
                                       const char* opname)
    : coord_rpc(id, s)
    , m_opname(opname)
    , m_done(false)
{
}

coord_rpc_generic :: ~coord_rpc_generic() throw ()
{
}

bool
coord_rpc_generic :: can_yield()
{
    return !m_done;
}

bool
coord_rpc_generic :: yield(wtf_admin_returncode* status)
{
    assert(this->can_yield());
    m_done = true;
    *status = WTF_ADMIN_SUCCESS;
    return true;
}

bool
coord_rpc_generic :: handle_response(admin* adm,
                                     wtf_admin_returncode* status)
{
    *status = WTF_ADMIN_SUCCESS;
    wtf_admin_returncode resp_status;
    e::error err;
    adm->interpret_rpc_response_failure(repl_status, &resp_status, &err);
    set_status(resp_status);
    set_error(err);

    if (resp_status != WTF_ADMIN_SUCCESS)
    {
        return true;
    }

    if (repl_output_sz >= 2)
    {
        uint16_t x;
        e::unpack16be(repl_output, &x);
        coordinator_returncode rc = static_cast<coordinator_returncode>(x);

        switch (rc)
        {
            case wtf::COORD_SUCCESS:
                set_status(WTF_ADMIN_SUCCESS);
                break;
            case wtf::COORD_NOT_FOUND:
                YIELDING_ERROR(NOTFOUND) << "cannot " << m_opname << ": does not exist";
                break;
            case wtf::COORD_DUPLICATE:
                YIELDING_ERROR(DUPLICATE) << "cannot " << m_opname << ": already exists";
                break;
            case wtf::COORD_UNINITIALIZED:
                YIELDING_ERROR(COORDFAIL) << "cannot " << m_opname << ": coordinator is uninitialized";
                break;
            case wtf::COORD_NO_CAN_DO:
                YIELDING_ERROR(COORDFAIL) << "cannot " << m_opname << ": see coordinator log for details";
                break;
            case wtf::COORD_MALFORMED:
            default:
                YIELDING_ERROR(INTERNAL) << "internal error interfacing with coordinator";
                break;
        }
    }

    return true;
}
