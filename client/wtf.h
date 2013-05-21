/* Copyright (c) 2013, Sean Ogden
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of WTF nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef wtf_h_
#define wtf_h_

// STL
#include <map>
#include <memory>

// po6
#include <po6/net/hostname.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

//wtf
#include <common/network_msgtype.h>

// wtf_returncode occupies [4864, 5120)
enum wtf_returncode
{
    WTF_SUCCESS   = 4864,
    /* send/wait-specific values */
    WTF_NAME_TOO_LONG = 4880,
    /* loop-specific values */
    WTF_NONE_PENDING  = 4896,
    /* loop/send/wait-specific values */
    WTF_BACKOFF               = 4912,
    WTF_INTERNAL_ERROR        = 4913,
    WTF_INTERRUPTED           = 4914,
    WTF_MISBEHAVING_SERVER    = 4915,
    WTF_NEED_BOOTSTRAP        = 4916,
    WTF_TIMEOUT               = 4917,
    /* command-specific values */
    WTF_BAD_LIBRARY       = 4928,
    WTF_COND_DESTROYED    = 4929,
    WTF_COND_NOT_FOUND    = 4930,
    WTF_FUNC_NOT_FOUND    = 4931,
    WTF_OBJ_EXIST         = 4932,
    WTF_OBJ_NOT_FOUND     = 4933,
    WTF_SERVER_ERROR      = 4934,
    /* predictable uninitialized value */
    WTF_GARBAGE   = 5119
};

void
wtf_destroy_output(const char* output, size_t output_sz);

namespace wtf
{
class chain_node;
class configuration;
class mapper;
} // namespace wtf

class wtf_client
{
    public:
        wtf_client(const char* host, in_port_t port);
        ~wtf_client() throw ();

    public:
        const char* last_error_desc() const { return m_last_error_desc; }
        const char* last_error_file() const { return m_last_error_file; }
        uint64_t last_error_line() const { return m_last_error_line; }

    public:
        int64_t send(wtf::wtf_network_msgtype msg, const char* data, size_t data_sz,
                     wtf_returncode* status,
                     const char** output, size_t* output_sz);
        int64_t wait(const char* object,
                     const char* cond,
                     uint64_t state,
                     wtf_returncode* status);
        int64_t loop(int timeout, wtf_returncode* status);
        int64_t loop(int64_t id, int timeout, wtf_returncode* status);
        void kill(int64_t id);
#ifdef _MSC_VER
        fd_set* poll_fd();
#else
        int poll_fd();
#endif

    private:
        class command;
        typedef std::map<uint64_t, e::intrusive_ptr<command> > command_map;

    private:
        wtf_client(const wtf_client& other);

    private:
        int64_t inner_loop(wtf_returncode* status);
        // Work the state machine to keep connected to the replicated service
        int64_t maintain_connection(wtf_returncode* status);
        // Send commands and receive responses
        int64_t send_to_chain_head(std::auto_ptr<e::buffer> msg,
                                   wtf_returncode* status);
        int64_t send_to_preferred_chain_position(e::intrusive_ptr<command> cmd,
                                                 wtf_returncode* status);
        void handle_disruption(const wtf::chain_node& node,
                               wtf_returncode* status);
        int64_t handle_command_response(const po6::net::location& from,
                                        std::auto_ptr<e::buffer> msg,
                                        e::unpacker up,
                                        wtf_returncode* status);
        // Utilities
        uint64_t generate_token();
        void reset_to_disconnected();

    private:
        wtf_client& operator = (const wtf_client& rhs);

    private:
        std::auto_ptr<wtf::mapper> m_busybee_mapper;
        std::auto_ptr<class busybee_st> m_busybee;
        std::auto_ptr<wtf::configuration> m_config;
        uint64_t m_token;
        uint64_t m_nonce;
        command_map m_commands;
        command_map m_complete;
        command_map m_resend;
        const char* m_last_error_desc;
        const char* m_last_error_file;
        uint64_t m_last_error_line;
        po6::net::location m_last_error_host;
};

std::ostream&
operator << (std::ostream& lhs, wtf_returncode rhs);

#endif /* wtf_h_ */
