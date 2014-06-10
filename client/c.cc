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

// WTF
#include <wtf/client.h>
#include "visibility.h"
#include "common/macros.h"
#include "client/client.h"

#define C_WRAP_EXCEPT(X) \
    wtf::client* cl = reinterpret_cast<wtf::client*>(_cl); \
    try \
    { \
        X \
    } \
    catch (po6::error& e) \
    { \
        errno = e; \
        *status = WTF_CLIENT_EXCEPTION; \
        cl->set_error_message("unhandled exception was thrown"); \
        return -1; \
    } \
    catch (std::bad_alloc& ba) \
    { \
        errno = ENOMEM; \
        *status = WTF_CLIENT_NOMEM; \
        cl->set_error_message("out of memory"); \
        return -1; \
    } \
    catch (...) \
    { \
        *status = WTF_CLIENT_EXCEPTION; \
        cl->set_error_message("unhandled exception was thrown"); \
        return -1; \
    }

#ifdef __cplusplus
extern "C"
{
#endif // __cplusplus

WTF_API wtf_client*
wtf_client_create(const char* host, uint16_t port, const char* hyper_host, uint16_t hyper_port)
{
    try
    {
        return reinterpret_cast<wtf_client*>(new wtf::client(host, port, hyper_host, hyper_port));
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

WTF_API void
wtf_client_destroy(wtf_client* client)
{
    delete reinterpret_cast<wtf::client*>(client);
}

WTF_API const char*
wtf_client_error_message(wtf_client* _cl)
{
    wtf::client* cl = reinterpret_cast<wtf::client*>(_cl);
    return cl->error_message();
}

WTF_API const char*
wtf_client_error_location(wtf_client* _cl)
{
    wtf::client* cl = reinterpret_cast<wtf::client*>(_cl);
    return cl->error_location();
}

WTF_API const char*
wtf_client_returncode_to_string(enum wtf_client_returncode status)
{
    switch (status)
    {
        CSTRINGIFY(WTF_CLIENT_SUCCESS);
        CSTRINGIFY(WTF_CLIENT_NOTFOUND);
        CSTRINGIFY(WTF_CLIENT_INVALID);
        CSTRINGIFY(WTF_CLIENT_BADF);
        CSTRINGIFY(WTF_CLIENT_READONLY);
        CSTRINGIFY(WTF_CLIENT_NAMETOOLONG);
        CSTRINGIFY(WTF_CLIENT_EXIST);
        CSTRINGIFY(WTF_CLIENT_ISDIR);
        CSTRINGIFY(WTF_CLIENT_NOTDIR);
        CSTRINGIFY(WTF_CLIENT_UNKNOWNSPACE);
        CSTRINGIFY(WTF_CLIENT_COORDFAIL);
        CSTRINGIFY(WTF_CLIENT_SERVERERROR);
        CSTRINGIFY(WTF_CLIENT_POLLFAILED);
        CSTRINGIFY(WTF_CLIENT_OVERFLOW);
        CSTRINGIFY(WTF_CLIENT_RECONFIGURE);
        CSTRINGIFY(WTF_CLIENT_TIMEOUT);
        CSTRINGIFY(WTF_CLIENT_NONEPENDING);
        CSTRINGIFY(WTF_CLIENT_NOMEM);
        CSTRINGIFY(WTF_CLIENT_BADCONFIG);
        CSTRINGIFY(WTF_CLIENT_DUPLICATE);
        CSTRINGIFY(WTF_CLIENT_INTERRUPTED);
        CSTRINGIFY(WTF_CLIENT_CLUSTER_JUMP);
        CSTRINGIFY(WTF_CLIENT_COORD_LOGGED);
        CSTRINGIFY(WTF_CLIENT_BACKOFF);
        CSTRINGIFY(WTF_CLIENT_IO);
        CSTRINGIFY(WTF_CLIENT_INTERNAL);
        CSTRINGIFY(WTF_CLIENT_EXCEPTION);
        CSTRINGIFY(WTF_CLIENT_GARBAGE);
        default:
            return "unknown wtf_client_returncode";
    }
}

WTF_API int64_t wtf_client_getcwd(wtf_client* _cl, 
            char* path, size_t len, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->getcwd(path, len);
    );
}

WTF_API int64_t wtf_client_chdir(wtf_client* _cl, 
            char* path, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->chdir(path);
    );
}

WTF_API int64_t wtf_client_open(wtf_client* _cl, 
            const char* path, int flags, mode_t mode, size_t num_replicas, size_t block_size,
            wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->open(path, flags, mode, num_replicas, block_size);
    );

}

WTF_API int64_t wtf_client_unlink(wtf_client* _cl, 
            const char* path, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->unlink(path, status);
    );

}

WTF_API int64_t wtf_client_rename(wtf_client* _cl, 
            const char* src, const char* dst, 
            wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->rename(src, dst, status);
    );

}

WTF_API int64_t wtf_client_getattr(wtf_client* _cl, 
            const char* path, 
            struct wtf_file_attrs* fa, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->getattr(path, fa);
    );

}

WTF_API std::vector<std::string> wtf_client_ls(wtf_client* _cl,
            const char* path)
{
    wtf::client* cl = reinterpret_cast<wtf::client*>(_cl);
    return cl->ls(path);
}

WTF_API int64_t wtf_client_lseek(wtf_client* _cl, 
            int64_t fd, size_t offset, int whence, wtf_client_returncode* status)
{

    C_WRAP_EXCEPT(
        return cl->lseek(fd, offset, whence, status);
    );

}

WTF_API int64_t wtf_client_begin_tx(wtf_client* _cl, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        cl->begin_tx();
        return 0;
    );

}

WTF_API int64_t wtf_client_end_tx(wtf_client* _cl, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->end_tx();
    );

}

WTF_API int64_t wtf_client_mkdir(wtf_client* _cl, 
            const char* path, mode_t mode, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->mkdir(path, mode, status);
    );

}

WTF_API int64_t wtf_client_chmod(wtf_client* _cl, 
            const char* path, mode_t mode, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->chmod(path, mode, status);
    );

}

WTF_API int64_t wtf_client_write(wtf_client* _cl, 
            int64_t fd, const char* data, 
            size_t* data_sz,
            wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->write(fd, data, data_sz, status);
    );

}

WTF_API int64_t wtf_client_read(wtf_client* _cl, 
            int64_t fd, char* data, 
            size_t* data_sz, 
            wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->read(fd, data, data_sz, status);
    );

}

WTF_API int64_t wtf_client_close(wtf_client* _cl, 
            int64_t fd, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->close(fd, status);
    );

}

WTF_API int64_t wtf_client_loop(wtf_client* _cl, int64_t id, 
            int timeout, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->loop(id, timeout, status);
    );

}

WTF_API int64_t wtf_client_truncate(wtf_client* _cl, 
            int64_t fd, size_t length, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->truncate(fd, length);
    );

}

WTF_API int64_t wtf_client_opendir(wtf_client* _cl, 
            const char* path, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->opendir(path);
    );

}

WTF_API int64_t wtf_client_closedir(wtf_client* _cl, 
            int64_t fd, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->closedir(fd);
    );

}

WTF_API int64_t wtf_client_readdir(wtf_client* _cl, 
            int64_t fd, char* path, char** entry, wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->readdir(fd, path, entry, status);
    );

}


WTF_API int64_t wtf_client_write_sync(wtf_client* _cl, 
            int64_t fd, const char* data, 
            size_t* data_sz,
            wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->write_sync(fd, data, data_sz, status);
    );

}

WTF_API int64_t wtf_client_read_sync(wtf_client* _cl, 
            int64_t fd, char* data, 
            size_t* data_sz, 
            wtf_client_returncode* status)
{
    C_WRAP_EXCEPT(
        return cl->read_sync(fd, data, data_sz, status);
    );

}


#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus
