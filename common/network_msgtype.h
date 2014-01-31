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

#ifndef wtf_network_msgtype_h_
#define wtf_network_msgtype_h_

// e
#include <e/buffer.h>

namespace wtf
{

enum wtf_network_msgtype
{
    REQ_GET = 8,
    RESP_GET = 9,

    REQ_PUT = 16,
    RESP_PUT = 17,

    REQ_UPDATE = 32,
    RESP_UPDATE = 33,

    PACKET_NOP = 254,
    CONFIGMISMATCH = 255
};

std::ostream&
operator << (std::ostream& lhs, wtf_network_msgtype rhs);

e::buffer::packer
operator << (e::buffer::packer lhs, const wtf_network_msgtype& rhs);

e::unpacker
operator >> (e::unpacker lhs, wtf_network_msgtype& rhs);

size_t
pack_size(const wtf_network_msgtype& rhs);

} // namespace wtf

#endif // wtf_network_msgtype_h_
