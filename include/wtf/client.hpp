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
//     * Neither the name of wtf nor the names of its contributors may be
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

#ifndef wtf_client_hpp_
#define wtf_client_hpp_

// C++
#include <iostream>
#include <vector>

//WTF 
#include <wtf/client.h>

#ifndef SWIG
namespace wtf
{
#endif


class Client
{
    public:
        Client(const char* host, uint16_t port, const char* hyper_host, uint16_t hyper_port)
            : m_cl(wtf_client_create(host, port, hyper_host, hyper_port))
        {
            if (!m_cl)
            {
                throw std::bad_alloc();
            }
        }
        ~Client() throw ()
        {
            wtf_client_destroy(m_cl);
        }

    public:
        int64_t getcwd(char* c, size_t len, wtf_client_returncode* status)
            { return wtf_client_getcwd(m_cl, c, len, status); }
        int64_t chdir(const char* path, wtf_client_returncode* status)
            { return wtf_client_chdir(m_cl, path, status); }
        int64_t open(const char* path, int flags, mode_t mode, size_t num_replicas, 
                     size_t block_length, int64_t* fd, wtf_client_returncode* status)
            { return wtf_client_open(m_cl, path, flags, mode, num_replicas, block_length, fd, status); }
        int64_t unlink(const char* path, wtf_client_returncode* status)
            { return wtf_client_unlink(m_cl, path, status); }
        int64_t rename(const char* src, const char* dst, wtf_client_returncode* status)
            { return wtf_client_rename(m_cl, src, dst, status); }
        int64_t getattr(const char* path, struct wtf_file_attrs* fa, wtf_client_returncode* status)
            { return wtf_client_getattr(m_cl, path, fa, status); }
        std::vector<std::string> ls(const char* path)
            { return wtf_client_ls(m_cl, path); }
        int64_t lseek(int64_t fd, uint64_t offset, int whence, wtf_client_returncode* status)
            { return wtf_client_lseek(m_cl, fd, offset, whence, status); }
        int64_t begin_tx(wtf_client_returncode* status)
            { return wtf_client_begin_tx(m_cl, status); }
        int64_t end_tx(wtf_client_returncode* status)
            { return wtf_client_end_tx(m_cl, status); }
        int64_t mkdir(const char* path, mode_t mode, wtf_client_returncode* status)
            { return wtf_client_mkdir(m_cl, path, mode, status); }
        int64_t chmod(const char* path, mode_t mode, wtf_client_returncode* status) 
            { return wtf_client_chmod(m_cl, path, mode, status); }
        int64_t write(int64_t fd,
                      const char* data,
                      size_t* data_sz,
                      size_t replicas,
                      wtf_client_returncode* status)
            { return wtf_client_write(m_cl, fd, data, data_sz, status); }
        int64_t read(int64_t fd,
                     char* data,
                     size_t *data_sz,
                     wtf_client_returncode* status)
            { return wtf_client_read(m_cl, fd, data, data_sz, status); }
        int64_t close(int64_t fd, wtf_client_returncode* status)
            { return wtf_client_close(m_cl, fd, status); }
        int64_t loop(int64_t id, int timeout, wtf_client_returncode* status)
            { return wtf_client_loop(m_cl, id, timeout, status); }
        int64_t truncate(int fd, size_t length, wtf_client_returncode* status)
            { return wtf_client_truncate(m_cl, fd, length, status); }
        int64_t readdir(const char* path, char** entry, wtf_client_returncode* status)
            { return wtf_client_readdir(m_cl, path, entry, status); }

        int64_t write_sync(int64_t fd,
                      const char* data,
                      size_t* data_sz,
                      size_t replicas,
                      wtf_client_returncode* status)
            { return wtf_client_write_sync(m_cl, fd, data, data_sz, status); }
        int64_t read_sync(int64_t fd,
                     char* data,
                     size_t *data_sz,
                     wtf_client_returncode* status)
            { return wtf_client_read_sync(m_cl, fd, data, data_sz, status); }
    public:
        std::string error_message()
            { return wtf_client_error_message(m_cl); }
        std::string error_location()
            { return wtf_client_error_location(m_cl); }

    private:
        Client(const Client&);
        Client& operator = (const Client&);

    private:
        wtf_client* m_cl;
};
#ifndef SWIG
} // namespace wtf
#endif

#ifndef SWIG
std::ostream&
operator << (std::ostream& lhs, wtf_client_returncode rhs);
#endif

#endif // wtf_client_hpp_
