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

#ifndef wtf_coordinator_transitions_h_
#define wtf_coordinator_transitions_h_
#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/* Replicant */
#include <replicant_state_machine.h>

extern void*
wtf_coordinator_create(struct replicant_state_machine_context* ctx);

extern void*
wtf_coordinator_recreate(struct replicant_state_machine_context* ctx,
                              const char* data, size_t data_sz);

extern void
wtf_coordinator_destroy(struct replicant_state_machine_context* ctx,
                             void* f);

extern void
wtf_coordinator_snapshot(struct replicant_state_machine_context* ctx,
                              void* obj, const char** data, size_t* sz);

#define TRANSITION(X) void \
    wtf_coordinator_ ## X(struct replicant_state_machine_context* ctx, \
                               void* obj, const char* data, size_t data_sz)
TRANSITION(init);

TRANSITION(read_only);
TRANSITION(fault_tolerance);

TRANSITION(config_get);
TRANSITION(config_ack);
TRANSITION(config_stable);

TRANSITION(server_register);
TRANSITION(server_online);
TRANSITION(server_offline);
TRANSITION(server_shutdown);
TRANSITION(server_kill);
TRANSITION(server_forget);
TRANSITION(server_suspect);
TRANSITION(report_disconnect);
TRANSITION(checkpoint_stable);

TRANSITION(alarm);

TRANSITION(debug_dump);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif // wtf_coordinator_transitions_h_
