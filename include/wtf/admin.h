/* Copyright (c) 2013, Cornell University
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

#ifndef wtf_admin_h_
#define wtf_admin_h_

/* C */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct wtf_admin;

struct wtf_admin_perf_counter
{
    uint64_t id;
    uint64_t time;
    const char* property;
    uint64_t measurement;
};

/* wtf_admin_returncode occupies [8704, 8832) */
enum wtf_admin_returncode
{
    WTF_ADMIN_SUCCESS      = 8704,

    /* Error conditions */
    WTF_ADMIN_NOMEM        = 8768,
    WTF_ADMIN_NONEPENDING  = 8769,
    WTF_ADMIN_POLLFAILED   = 8770,
    WTF_ADMIN_TIMEOUT      = 8771,
    WTF_ADMIN_INTERRUPTED  = 8772,
    WTF_ADMIN_SERVERERROR  = 8773,
    WTF_ADMIN_COORDFAIL    = 8774,
    WTF_ADMIN_BADSPACE     = 8775,
    WTF_ADMIN_DUPLICATE    = 8776,
    WTF_ADMIN_NOTFOUND     = 8777,
    WTF_ADMIN_LOCALERROR   = 8778,

    /* This should never happen.  It indicates a bug */
    WTF_ADMIN_INTERNAL     = 8829,
    WTF_ADMIN_EXCEPTION    = 8830,
    WTF_ADMIN_GARBAGE      = 8831
};

struct wtf_admin*
wtf_admin_create(const char* coordinator, uint16_t port);

void
wtf_admin_destroy(struct wtf_admin* admin);

int64_t
wtf_admin_dump_config(struct wtf_admin* admin,
                           enum wtf_admin_returncode* status,
                           const char** config);

int64_t
wtf_admin_server_register(struct wtf_admin* admin,
                               uint64_t token, const char* address,
                               enum wtf_admin_returncode* status);

int64_t
wtf_admin_server_online(struct wtf_admin* admin,
                             uint64_t token,
                             enum wtf_admin_returncode* status);

int64_t
wtf_admin_server_offline(struct wtf_admin* admin,
                              uint64_t token,
                              enum wtf_admin_returncode* status);

int64_t
wtf_admin_server_forget(struct wtf_admin* admin,
                             uint64_t token,
                             enum wtf_admin_returncode* status);

int64_t
wtf_admin_server_kill(struct wtf_admin* admin,
                           uint64_t token,
                           enum wtf_admin_returncode* status);

int64_t
wtf_admin_loop(struct wtf_admin* admin, int timeout,
                    enum wtf_admin_returncode* status);

const char*
wtf_admin_error_message(struct wtf_admin* admin);
const char*
wtf_admin_error_location(struct wtf_admin* admin);

const char*
wtf_admin_returncode_to_string(enum wtf_admin_returncode);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* wtf_admin_h_ */
