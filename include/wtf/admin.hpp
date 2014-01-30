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

#ifndef wtf_admin_hpp_
#define wtf_admin_hpp_

// C++
#include <iostream>

// WTF
#include <wtf/admin.h>

namespace wtf
{

class Admin
{
    public:
        Admin(const char* coordinator, uint16_t port)
            : m_adm(wtf_admin_create(coordinator, port))
        {
            if (!m_adm)
            {
                throw std::bad_alloc();
            }
        }
        ~Admin() throw ()
        {
            wtf_admin_destroy(m_adm);
        }

    public:
        int64_t dump_config(enum wtf_admin_returncode* status,
                            const char** config)
            { return wtf_admin_dump_config(m_adm, status, config); }
        int64_t server_register(uint64_t token, const char* address,
                                enum wtf_admin_returncode* status)
            { return wtf_admin_server_register(m_adm, token, address, status); }
        int64_t server_online(uint64_t token, enum wtf_admin_returncode* status)
            { return wtf_admin_server_online(m_adm, token, status); }
        int64_t server_offline(uint64_t token, enum wtf_admin_returncode* status)
            { return wtf_admin_server_offline(m_adm, token, status); }
        int64_t server_forget(uint64_t token, enum wtf_admin_returncode* status)
            { return wtf_admin_server_forget(m_adm, token, status); }
        int64_t server_kill(uint64_t token, enum wtf_admin_returncode* status)
            { return wtf_admin_server_kill(m_adm, token, status); }
    public:
        int64_t loop(int timeout, enum wtf_admin_returncode* status)
            { return wtf_admin_loop(m_adm, timeout, status); }
        std::string error_message()
            { return wtf_admin_error_message(m_adm); }
        std::string error_location()
            { return wtf_admin_error_location(m_adm); }

    private:
        Admin(const Admin&);
        Admin& operator = (const Admin&);

    private:
        wtf_admin* m_adm;
};

} // namespace wtf

std::ostream&
operator << (std::ostream& lhs, wtf_admin_returncode rhs);

#endif // wtf_admin_hpp_
