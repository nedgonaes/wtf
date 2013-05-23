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
#include "common/wtf_node.h"

namespace wtf
{

class configuration
{
    public:
        configuration();
        configuration(uint64_t cluster,
                      uint64_t version);
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
        bool is_member(const wtf_node& node) const;
        const wtf_node* node_from_token(uint64_t token) const;
        const wtf_node* get_random_member(uint64_t id);

    // iterators
    public:
        const wtf_node* members_begin() const;
        const wtf_node* members_end() const;

    // modify the configuration
    public:
        void bump_version();
        void add_member(const wtf_node& node);

    public:
        void debug_dump(std::ostream& out);

    private:
        friend bool operator == (const configuration& lhs, const configuration& rhs);
        friend std::ostream& operator << (std::ostream& lhs, const configuration& rhs);
        friend e::buffer::packer operator << (e::buffer::packer lhs, const configuration& rhs);
        friend e::unpacker operator >> (e::unpacker lhs, configuration& rhs);
        friend size_t pack_size(const configuration& rhs);

    private:
        uint64_t m_cluster;
        uint64_t m_version;
        std::vector<wtf_node> m_members;
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

char*
pack_config(const configuration& config, char* ptr);

size_t
pack_size(const configuration& rhs);

size_t
pack_size(const std::vector<configuration>& rhs);

} // namespace wtf

#endif // wtf_configuration_h_
