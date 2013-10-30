
// LevelDB
#include <hyperleveldb/db.h>

// po6
#include <po6/pathname.h>

#include <tr1/memory.h>

#include <sys/stat.h>

#ifndef 
namespace wtf
{
    class daemon;

    class blockmap
    {
        typedef std::tr1::shared_ptr<leveldb::DB> leveldb_db_ptr;

        blockmap();
        ~blockmap();

        daemon *m_daemon;
        level_db_ptr m_db;
        uint32_t m_backing_size;
        uint32_t m_backing_offset;
    };
}
