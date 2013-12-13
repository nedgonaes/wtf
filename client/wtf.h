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

#define CHUNKSIZE (16)
#define ROUNDUP(X,Y)   (((int)X + Y - 1) & ~(Y-1)) /* Y must be a power of 2 */
#define MIN(X,Y) ((X) < (Y) ? (X) : (Y))
#define MAX(X,Y) ((X) > (Y) ? (X) : (Y))

// STL
#include <map>
#include <memory>

// po6
#include <po6/net/hostname.h>
#include <po6/pathname.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

// HyperDex
#include <hyperdex/client.hpp>

//wtf
#include <common/network_msgtype.h>

enum wtf_returncode
{
    WTF_SUCCESS      = 8448,
    WTF_NOTFOUND     = 8449,
    WTF_READONLY     = 8452,

    /* Error conditions */
    WTF_UNKNOWNSPACE = 8512,
    WTF_COORDFAIL    = 8513,
    WTF_SERVERERROR  = 8514,
    WTF_POLLFAILED   = 8515,
    WTF_OVERFLOW     = 8516,
    WTF_RECONFIGURE  = 8517,
    WTF_TIMEOUT      = 8519,
    WTF_NONEPENDING  = 8523,
    WTF_NOMEM        = 8526,
    WTF_BADCONFIG    = 8527,
    WTF_DUPLICATE    = 8529,
    WTF_INTERRUPTED  = 8530,
    WTF_CLUSTER_JUMP = 8531,
    WTF_COORD_LOGGED = 8532,
    WTF_BACKOFF      = 8533,

    /* This should never happen.  It indicates a bug */
    WTF_INTERNAL     = 8573,
    WTF_EXCEPTION    = 8574,
    WTF_GARBAGE      = 8575
};

void
wtf_destroy_output(const char* output, size_t output_sz);

namespace wtf
{
class wtf_node;
class configuration;
class coordinator_link;
class mapper;
class tool_wrapper;
} // namespace wtf

class wtf_client
{
    public:
        wtf_client(const char* host, in_port_t port,
                   const char* hyper_host, in_port_t hyper_port);
        ~wtf_client() throw ();

    public:
        const char* last_error_desc() const { return m_last_error_desc; }
        const char* last_error_file() const { return m_last_error_file; }
        uint64_t last_error_line() const { return m_last_error_line; }

    public:

        int64_t canon_path(char* rel, char* abspath, size_t abspath_sz);
        int64_t getcwd(char* c, size_t len);
        int64_t chdir(char* path);
        int64_t open(const char* path, int flags);
        int64_t open(const char* path, int flags, mode_t mode);

        void lseek(int64_t fd, uint64_t offset);
        void begin_tx();
        int64_t end_tx();
        int64_t mkdir(const char* path, mode_t mode); 
        int64_t chmod(const char* path, mode_t mode); 
        int64_t write(int64_t fd,
                      const char* data,
                      uint32_t data_sz,
                      uint32_t replicas,
                      wtf_returncode* status);
        int64_t read(int64_t fd,
                     char* data,
                     uint32_t *data_sz,
                     wtf_returncode* status);
        int64_t close(int64_t fd, wtf_returncode* status);
        int64_t flush(int64_t fd,
                      wtf_returncode* status);
        int64_t wait(const char* object,
                     const char* cond,
                     uint64_t state,
                     wtf_returncode* status);
        int64_t loop(int timeout, wtf_returncode* status);
        int64_t loop(int64_t id, int timeout, wtf_returncode* status);

    friend class wtf::tool_wrapper;

    private:
        class command;
        class file;

    // these are the only private things that tool_wrapper should touch
    private:
        wtf_returncode initialize_cluster(uint64_t cluster, const char* path);
        wtf_returncode show_config(std::ostream& out);
        wtf_returncode kill(uint64_t server_id);

    private:
        int64_t maintain_coord_connection(wtf_returncode* status);
#ifdef _MSC_VER
        fd_set* poll_fd();
#else
        int poll_fd();
#endif
        int64_t send(e::intrusive_ptr<command>& cmd,
                     wtf_returncode* status);

    private:
        friend e::unpacker 
            operator >> (e::unpacker up, wtf_client::file& rhs);
        friend e::buffer::packer 
            operator << (e::buffer::packer pa, const wtf_client::file& rhs);
        typedef std::map<uint64_t, e::intrusive_ptr<command> > command_map;
        typedef std::map<uint64_t, e::intrusive_ptr<file> > file_map;

    private:
        wtf_client(const wtf_client& other);

    private:
        int64_t inner_loop(wtf_returncode* status);

        // Send commands and receive responses
        int64_t send_to_blockserver(e::intrusive_ptr<command> cmd,
                                    wtf_returncode* status);
        void handle_disruption(const wtf::wtf_node& node,
                               wtf_returncode* status);
        int64_t handle_command_response(const po6::net::location& from,
                                        std::auto_ptr<e::buffer> msg,
                                        e::unpacker up,
                                        wtf_returncode* status);
        void handle_update(e::intrusive_ptr<command>& cmd, 
                        e::intrusive_ptr<file>& f);
        void handle_put(e::intrusive_ptr<command>& cmd, 
                        e::intrusive_ptr<file>& f);
        void handle_get(e::intrusive_ptr<command>& cmd, 
                        e::intrusive_ptr<file>& f);
        // Utilities
        uint64_t generate_token();
        void reset_to_disconnected();

        //communicate with hyperdex
        int64_t update_hyperdex(e::intrusive_ptr<file>& f);
        int64_t update_file_cache(const char* path, e::intrusive_ptr<file>& f, bool create);
        hyperdex_client_returncode hyperdex_wait_for_result(int64_t reqid, hyperdex_client_returncode& status);

    private:
        wtf_client& operator = (const wtf_client& rhs);

    private:
        std::auto_ptr<wtf::configuration> m_config;
        std::auto_ptr<wtf::mapper> m_busybee_mapper;
        std::auto_ptr<class busybee_st> m_busybee;
        const std::auto_ptr<wtf::coordinator_link> m_coord;
        uint64_t m_nonce;
        uint64_t m_fileno;
        bool m_have_seen_config;
        command_map m_commands;
        command_map m_complete;
        command_map m_resend;
        file_map m_fds;
        const char* m_last_error_desc;
        const char* m_last_error_file;
        uint64_t m_last_error_line;
        po6::net::location m_last_error_host;
        hyperdex::Client m_hyperdex_client;
        std::string m_cwd;
};

std::ostream&
operator << (std::ostream& lhs, wtf_returncode rhs);

#endif /* wtf_h_ */
