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

/* Replicant */
#include <replicant_state_machine.h>

/* WTF */
#include <coordinator/transitions.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-pedantic"


struct replicant_state_machine rsm = {
    wtf_coordinator_create,
    wtf_coordinator_recreate,
    wtf_coordinator_destroy,
    wtf_coordinator_snapshot,
    {{"config_get", wtf_coordinator_config_get},
     {"config_ack", wtf_coordinator_config_ack},
     {"server_register", wtf_coordinator_server_register},
     {"server_online", wtf_coordinator_server_online},
     {"server_offline", wtf_coordinator_server_offline},
     {"server_shutdown", wtf_coordinator_server_shutdown},
     {"server_kill", wtf_coordinator_server_kill},
     {"server_forget", wtf_coordinator_server_forget},
     {"server_suspect", wtf_coordinator_server_suspect},
     {"report_disconnect", wtf_coordinator_report_disconnect},
     {"checkpoint_stable", wtf_coordinator_checkpoint_stable},
     {"alarm", wtf_coordinator_alarm},
     {"read_only", wtf_coordinator_read_only},
     {"debug_dump", wtf_coordinator_debug_dump},
     {"init", wtf_coordinator_init},
     {NULL, NULL}}
};

#pragma GCC diagnostic pop
