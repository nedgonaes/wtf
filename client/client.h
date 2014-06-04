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

#ifndef client_client_h_
#define client_client_h_

#define CHUNKSIZE (512)
#define ROUNDUP(X,Y)   (((int)X + Y - 1) & ~(Y-1)) /* Y must be a power of 2 */
#define MIN(X,Y) ((X) < (Y) ? (X) : (Y))
#define MAX(X,Y) ((X) > (Y) ? (X) : (Y))

// STL
#include <map>
#include <vector>
#include <string>
#include <memory>

// po6
#include <po6/net/hostname.h>
#include <po6/pathname.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

// busybee
#include <busybee_st.h>

//hyperdex
#include <hyperdex/client.hpp>

//wtf
#include <wtf/client.h>
#include "common/configuration.h"
#include "common/coordinator_link.h"
#include "common/ids.h"
#include "common/block.h"
#include "common/mapper.h"
#include "client/pending.h"
#include "client/pending_aggregation.h"
#include "client/file.h"
#include "common/block_location.h"

void
wtf_destroy_output(const char* output, size_t output_sz);

namespace wtf __attribute__ ((visibility("hidden")))
{
class server;
class configuration;
class coordinator_link;
class mapper;
class file;
class pending_read;

class client
{
    public:
        client(const char* host, in_port_t port,
                   const char* hyper_host, in_port_t hyper_port);
        ~client() throw ();

    public:
        int64_t canon_path(const char* rel, char* abspath, size_t abspath_sz);
        int64_t getcwd(char* c, size_t len);
        int64_t chdir(char* path);
        int64_t open(const char* path, int flags, mode_t mode, size_t num_replicas, size_t block_size);
        int64_t unlink(const char* path, wtf_client_returncode* status);
        int64_t rename(const char* src, const char* dst, wtf_client_returncode* status);
        int64_t getattr(const char* path, struct wtf_file_attrs* fa);
        std::vector<std::string> ls(const char* path);

        int64_t lseek(int64_t fd, uint64_t offset, int whence, wtf_client_returncode* status);
        void begin_tx();
        int64_t end_tx();
        int64_t mkdir(const char* path, mode_t mode); 
        int64_t chmod(const char* path, mode_t mode); 
        int64_t write(int64_t fd,
                      const char* buf,
                      size_t* buf_sz,
                      wtf_client_returncode* status);
        int64_t read(int64_t fd,
                     char* data,
                     size_t *data_sz,
                     wtf_client_returncode* status);
        int64_t close(int64_t fd, wtf_client_returncode* status);
        int64_t loop(int timeout, wtf_client_returncode* status);
        int64_t loop(int64_t id, int timeout, wtf_client_returncode* status);
        int64_t truncate(int fd, off_t length);
        int64_t opendir(const char* path);
        int64_t closedir(int fd);
        int64_t readdir(int fd, char* entry);
        const char* error_message();
        const char* error_location();
        void set_error_message(const char* msg);

    public:
        int64_t write_sync(int64_t fd,
                      const char* buf,
                      size_t* buf_sz,
                      wtf_client_returncode* status);
        int64_t read_sync(int64_t fd,
                     char* data,
                     size_t *data_sz,
                     wtf_client_returncode* status);
    private:
        struct pending_server_pair
        {
            pending_server_pair()
                : si(), op() {}
            pending_server_pair(const server_id& s,
                                const e::intrusive_ptr<pending>& o)
                : si(s), op(o) {}
            ~pending_server_pair() throw () {}
            server_id si;
            e::intrusive_ptr<pending> op;
        };

        friend class pending_read;
        friend class pending_write;
        friend class pending_readdir;
        friend class rereplicate;
        typedef std::map<uint64_t, pending_server_pair> pending_map_t;
        typedef std::map<uint64_t, e::intrusive_ptr<pending> > yieldable_map_t;
        typedef std::list<pending_server_pair> pending_queue_t;
        typedef std::map<uint64_t, e::intrusive_ptr<file> > file_map_t;

    private:
        bool maintain_coord_connection(wtf_client_returncode* status);
        bool send(wtf_network_msgtype mt,
                  const server_id& to,
                  uint64_t nonce,
                  std::auto_ptr<e::buffer> msg,
                  e::intrusive_ptr<pending> op,
                  wtf_client_returncode* status);

        int64_t perform_aggregation(const std::vector<server_id>& servers,
                              e::intrusive_ptr<pending_aggregation> _op,
                              wtf_network_msgtype mt,
                              std::auto_ptr<e::buffer> msg,
                              wtf_client_returncode* status);

        void prepare_write_op(e::intrusive_ptr<file> f, 
                              size_t& rem, 
                              std::vector<block_location>& bl,
                              size_t& buf_offset,
                              uint32_t& block_offset,
                              uint32_t& block_capacity,
                              uint64_t& file_offset,
                              size_t& slice_len);
        void prepare_read_op(e::intrusive_ptr<file> f, 
                              size_t& rem, 
                              size_t& buf_offset,
                              block_location& bl, 
                              uint32_t& block_size,
                              e::intrusive_ptr<pending_read> op, 
                              std::vector<server_id>& servers);
    private:
        friend e::unpacker 
            operator >> (e::unpacker up, file& rhs);
        friend e::buffer::packer 
            operator << (e::buffer::packer pa, const file& rhs);

    private:
        client(const client& other);
        client& operator = (const client& rhs);

    private:
        int64_t inner_loop(int timeout,
                           wtf_client_returncode* status,
                           int64_t wait_for);

        void handle_disruption(const wtf::server_id& node);

        // Utilities
        uint64_t generate_token();

        //communicate with hyperdex
        int64_t apply_changeset(e::intrusive_ptr<file> f, 
            std::map<uint64_t, e::intrusive_ptr<block> >& changeset);
        int64_t put_file_metadata(e::intrusive_ptr<file> f, wtf_client_returncode* status);
        int64_t update_file_metadata(e::intrusive_ptr<file> f, 
                                     const char* old_blockmap, 
                                     size_t old_blockmap_sz,
                                     wtf_client_returncode* status);
        int64_t get_file_metadata(const char* path, e::intrusive_ptr<file> f, bool create);
        hyperdex_client_returncode hyperdex_wait_for_result(int64_t reqid, hyperdex_client_returncode& status);

    private:
        wtf::coordinator_link m_coord;
        e::garbage_collector m_gc;
        e::garbage_collector::thread_state m_gc_ts;
        wtf::mapper m_busybee_mapper;
        busybee_st m_busybee;
        int64_t m_next_client_id;
        uint64_t m_next_server_nonce;
        pending_map_t m_pending_ops;
        pending_map_t m_pending_hyperdex_ops;
        pending_queue_t m_failed;
        yieldable_map_t m_yieldable;
        e::intrusive_ptr<pending> m_yielding;
        e::intrusive_ptr<pending> m_yielded;
        e::error m_last_error;
        hyperdex::Client m_hyperdex_client;
        uint64_t m_next_fileno;
        file_map_t m_fds;
        std::string m_cwd;
};

std::ostream&
operator << (std::ostream& lhs, wtf_client_returncode rhs);

} // namespace wtf __attribute__ ((visibility("hidden")))
#endif /* client_client_h_ */
