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

#ifndef wtf_configuration_h_
#define wtf_configuration_h_

// WTF
#include "common/server.h"

namespace wtf __attribute__ ((visibility("hidden")))
{

class block_location;

class configuration
{
    public:
        configuration();
        configuration(const configuration& other);
        ~configuration() throw ();

    // metadata
    public:
        uint64_t cluster() const { return m_cluster; }
        uint64_t version() const { return m_version; }

    // invariants
    public:
        bool validate() const;

    // membership
    public:
        bool has_token(uint64_t token) const;
        bool exists(const server_id& node) const;
        server::state_t get_state(const server_id& id) const;
        po6::net::location get_address(const server_id& id) const;
        const server* server_from_id(server_id id) const;
        const server* get_random_server(uint64_t id) const;

    public:
        void assign_random_block_locations(std::vector<block_location>& bl) const;

    // iterators
    public:
        const server* servers_begin() const;
        const server* servers_end() const;

    // modify the configuration
    public:
        void bump_version();
        void add_server(const server& node);

    public:
        std::string dump() const;

    private:
        friend bool operator == (const configuration& lhs, const configuration& rhs);
        friend e::buffer::packer operator << (e::buffer::packer lhs, const configuration& rhs);
        friend e::unpacker operator >> (e::unpacker lhs, configuration& rhs);
        friend size_t pack_size(const configuration& rhs);

    private:
        uint64_t m_cluster;
        uint64_t m_version;
        uint64_t m_flags;
        std::vector<server> m_servers;
};

bool
operator == (const configuration& lhs, const configuration& rhs);
inline bool
operator != (const configuration& lhs, const configuration& rhs) { return !(lhs == rhs); }

std::ostream&
operator << (std::ostream& lhs, const configuration& rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const configuration& rhs);

e::unpacker
operator >> (e::unpacker lhs, configuration& rhs);

} // namespace wtf __attribute__ ((visibility("hidden")))

#endif // wtf_configuration_h_
