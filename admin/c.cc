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

#define __STDC_LIMIT_MACROS

// WTF 
#include "include/wtf/admin.h"
#include "common/macros.h"
#include "admin/admin.h"

#define C_WRAP_EXCEPT(X) \
    try \
    { \
        X \
    } \
    catch (po6::error& e) \
    { \
        errno = e; \
        *status = WTF_ADMIN_EXCEPTION; \
        return -1; \
    } \
    catch (std::bad_alloc& ba) \
    { \
        errno = ENOMEM; \
        *status = WTF_ADMIN_NOMEM; \
        return -1; \
    } \
    catch (...) \
    { \
        *status = WTF_ADMIN_EXCEPTION; \
        return -1; \
    }

extern "C"
{

 struct wtf_admin*
wtf_admin_create(const char* coordinator, uint16_t port)
{
    try
    {
        return reinterpret_cast<struct wtf_admin*>(new wtf::admin(coordinator, port));
    }
    catch (po6::error& e)
    {
        errno = e;
        return NULL;
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        return NULL;
    }
}

 void
wtf_admin_destroy(struct wtf_admin* admin)
{
    delete reinterpret_cast<wtf::admin*>(admin);
}

 int64_t
wtf_admin_dump_config(struct wtf_admin* _adm,
                           wtf_admin_returncode* status,
                           const char** config)
{
    C_WRAP_EXCEPT(
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->dump_config(status, config);
    );
}

 int64_t
wtf_admin_read_only(struct wtf_admin* _adm, int ro,
                         wtf_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->read_only(ro, status);
    );
}

 int64_t
wtf_admin_server_register(struct wtf_admin* _adm,
                               uint64_t token, const char* address,
                               wtf_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->server_register(token, address, status);
    );
}

 int64_t
wtf_admin_server_online(struct wtf_admin* _adm,
                             uint64_t token,
                             wtf_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->server_online(token, status);
    );
}

 int64_t
wtf_admin_server_offline(struct wtf_admin* _adm,
                              uint64_t token,
                              wtf_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->server_offline(token, status);
    );
}

 int64_t
wtf_admin_server_forget(struct wtf_admin* _adm,
                             uint64_t token,
                             wtf_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->server_forget(token, status);
    );
}

 int64_t
wtf_admin_server_kill(struct wtf_admin* _adm,
                           uint64_t token,
                           wtf_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->server_kill(token, status);
    );
}

 int64_t
wtf_admin_loop(struct wtf_admin* _adm, int timeout,
                    enum wtf_admin_returncode* status)
{
    C_WRAP_EXCEPT(
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->loop(timeout, status);
    );
}

 const char*
wtf_admin_error_message(struct wtf_admin* _adm)
{
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->error_message();
}

 const char*
wtf_admin_error_location(struct wtf_admin* _adm)
{
    wtf::admin* adm = reinterpret_cast<wtf::admin*>(_adm);
    return adm->error_location();
}

 const char*
wtf_admin_returncode_to_string(enum wtf_admin_returncode status)
{
    switch (status)
    {
        CSTRINGIFY(WTF_ADMIN_SUCCESS);
        CSTRINGIFY(WTF_ADMIN_NOMEM);
        CSTRINGIFY(WTF_ADMIN_NONEPENDING);
        CSTRINGIFY(WTF_ADMIN_POLLFAILED);
        CSTRINGIFY(WTF_ADMIN_TIMEOUT);
        CSTRINGIFY(WTF_ADMIN_INTERRUPTED);
        CSTRINGIFY(WTF_ADMIN_SERVERERROR);
        CSTRINGIFY(WTF_ADMIN_COORDFAIL);
        CSTRINGIFY(WTF_ADMIN_BADSPACE);
        CSTRINGIFY(WTF_ADMIN_DUPLICATE);
        CSTRINGIFY(WTF_ADMIN_NOTFOUND);
        CSTRINGIFY(WTF_ADMIN_LOCALERROR);
        CSTRINGIFY(WTF_ADMIN_INTERNAL);
        CSTRINGIFY(WTF_ADMIN_EXCEPTION);
        CSTRINGIFY(WTF_ADMIN_GARBAGE);
        default:
            return "unknown wtf_admin_returncode";
    }
}

} // extern "C"
