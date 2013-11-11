
//sys
#include <sys/mman.h>

//glog
#include <glog/logging.h>
#include <glog/raw_logging.h>

// LevelDB
#include <hyperleveldb/db.h>
#include <hyperleveldb/write_batch.h>
#include <hyperleveldb/filter_policy.h>

// po6
#include <po6/pathname.h>
#include <po6/io/fd.h>

//e
#include <e/slice.h>
#include <e/unpacker.h>

//leveldb
#include <hyperleveldb/db.h>

#include <tr1/memory>

#include <sys/stat.h>
#include <unistd.h>

#include "disk.h"

#ifndef wtf_blockmap_h_
#define wtf_blockmap_h_

namespace wtf
{
    class blockmap
    {
        public:
            blockmap();
            ~blockmap();
            bool setup(const po6::pathname& path,
                  const po6::pathname& backing_path);

            ssize_t write(const e::slice& data,
                        uint64_t& bid);
            ssize_t update(const e::slice& data,
                        uint64_t offset,
                        uint64_t& bid);
            ssize_t read(uint64_t bid,
                        uint8_t* data, 
                        size_t data_sz);
        private:
            typedef std::tr1::shared_ptr<leveldb::DB> leveldb_db_ptr;
            leveldb_db_ptr m_db;
            uint32_t m_backing_size;
            uint32_t m_backing_offset;
            disk* m_disk;
            po6::io::fd m_fd;

    };
}
#endif //wtf_blockmap_h_
