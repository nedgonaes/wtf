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

#ifndef wtf_block_storage_manager_h_
#define wtf_block_storage_manager_h_

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

//po6
#include <po6/io/fd.h>
#include <po6/pathname.h>

// WTF
#include "common/ids.h"

namespace wtf
{
    class block_storage_manager
    {
        public:
            block_storage_manager();
            ~block_storage_manager();

        public:
            void setup(uint64_t sid,
                       po6::pathname path);
            void shutdown();

        public:
            ssize_t write_block(const e::slice& data,
                             server_id& sid,
                             block_id& bid);
            ssize_t read_block(server_id& sid,
                            block_id& bid,
                            e::slice& data);
            void stat();

        private:
            server_id m_prefix;
            block_id m_last_block_id;
            po6::pathname m_path;
    };
}

#endif // wtf_block_storage_manager_h_
