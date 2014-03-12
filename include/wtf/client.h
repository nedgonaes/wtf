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

#ifndef wtf_client_h_
#define wtf_client_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

    struct wtf_client;

    enum wtf_client_returncode
    {
        WTF_CLIENT_SUCCESS      = 8448,
        WTF_CLIENT_NOTFOUND     = 8449,
        WTF_CLIENT_INVALID      = 8450,
        WTF_CLIENT_BADF         = 8451,
        WTF_CLIENT_READONLY     = 8452,
        WTF_CLIENT_NAMETOOLONG  = 8453,
        WTF_CLIENT_EXIST        = 8454,
        WTF_CLIENT_ISDIR        = 8455,
        WTF_CLIENT_NOTDIR       = 8456,


        /* Error conditions */
        WTF_CLIENT_UNKNOWNSPACE = 8512,
        WTF_CLIENT_COORDFAIL    = 8513,
        WTF_CLIENT_SERVERERROR  = 8514,
        WTF_CLIENT_POLLFAILED   = 8515,
        WTF_CLIENT_OVERFLOW     = 8516,
        WTF_CLIENT_RECONFIGURE  = 8517,
        WTF_CLIENT_TIMEOUT      = 8519,
        WTF_CLIENT_NONEPENDING  = 8523,
        WTF_CLIENT_NOMEM        = 8526,
        WTF_CLIENT_BADCONFIG    = 8527,
        WTF_CLIENT_DUPLICATE    = 8529,
        WTF_CLIENT_INTERRUPTED  = 8530,
        WTF_CLIENT_CLUSTER_JUMP = 8531,
        WTF_CLIENT_COORD_LOGGED = 8532,
        WTF_CLIENT_BACKOFF      = 8533,
        WTF_CLIENT_IO           = 8534,

        /* This should never happen.  It indicates a bug */
        WTF_CLIENT_INTERNAL     = 8573,
        WTF_CLIENT_EXCEPTION    = 8574,
        WTF_CLIENT_GARBAGE      = 8575
    };

    struct wtf_file_attrs
    {
        size_t size;
        mode_t mode;
        int flags;
        int is_dir;
    };

    struct wtf_client* wtf_client_create(const char* host,
                                         uint16_t port,
                                         const char* hyper_host,
                                         uint16_t hyper_port);
    void wtf_client_destroy(struct wtf_client* m_cl);
    int64_t wtf_client_getcwd(struct wtf_client* m_cl, 
            char* path, size_t len, wtf_client_returncode* status);
    int64_t wtf_client_chdir(struct wtf_client* m_cl, 
            char* path, wtf_client_returncode* status);
    int64_t wtf_client_open(struct wtf_client* m_cl, 
            const char* path, int flags, mode_t mode, size_t num_replicas,
            size_t block_size, wtf_client_returncode* status);
    int64_t wtf_client_unlink(struct wtf_client* m_cl, const char* path,
            wtf_client_returncode* status);
    int64_t wtf_client_rename(struct wtf_client* m_cl, const char* src,
            const char* dst, wtf_client_returncode* status);
    int64_t wtf_client_getattr(struct wtf_client* m_cl, 
            const char* path, 
            struct wtf_file_attrs* fa, wtf_client_returncode* status);
    int64_t wtf_client_lseek(struct wtf_client* m_cl, 
            int64_t fd, size_t offset, int whence, wtf_client_returncode* status);
    int64_t wtf_client_begin_tx(struct wtf_client* m_cl, wtf_client_returncode* status);
    int64_t wtf_client_end_tx(struct wtf_client* m_cl, wtf_client_returncode* status);
    int64_t wtf_client_mkdir(struct wtf_client* m_cl, 
            const char* path, mode_t mode, wtf_client_returncode* status);
    int64_t wtf_client_chmod(struct wtf_client* m_cl, 
            const char* path, mode_t mode, wtf_client_returncode* status);
    int64_t wtf_client_write(struct wtf_client* m_cl, 
            int64_t fd, const char* data, 
            size_t* data_sz, 
            wtf_client_returncode* status);
    int64_t wtf_client_read(struct wtf_client* m_cl, 
            int64_t fd, char* data, 
            size_t* data_sz, 
            wtf_client_returncode* status);
    int64_t wtf_client_close(struct wtf_client* m_cl, 
            int64_t fd, wtf_client_returncode* status);
    int64_t wtf_client_loop(struct wtf_client* m_cl, int64_t id, 
            int timeout, wtf_client_returncode* status);
    int64_t wtf_client_truncate(struct wtf_client* m_cl, 
            int64_t fd, size_t length, wtf_client_returncode* status);
    int64_t wtf_client_opendir(struct wtf_client* m_cl, 
            const char* path, wtf_client_returncode* status);
    int64_t wtf_client_closedir(struct wtf_client* m_cl, 
            int64_t fd, wtf_client_returncode* status);
    int64_t wtf_client_readdir(struct wtf_client* m_cl, 
            int64_t fd, char* entry, wtf_client_returncode* status);

    int64_t wtf_client_read_sync(struct wtf_client* m_cl, 
            int64_t fd, char* data, 
            size_t* data_sz, 
            wtf_client_returncode* status);
    int64_t wtf_client_write_sync(struct wtf_client* m_cl, 
            int64_t fd, const char* data, 
            size_t* data_sz, 
            wtf_client_returncode* status);

    const char* wtf_client_error_message(struct wtf_client* m_cl);
    const char* wtf_client_error_location(struct wtf_client* m_cl);


#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* wtf_client_h_ */
