#include "blockmap.h"
#define BACKING_SIZE 2147483648

using wtf::blockmap;

blockmap::blockmap(daemon* d) : m_db()
                              , m_backing_size(BACKING_SIZE)
                              , m_backing_offset()
{
}

size_t
get_filesize(const po6::pathname& path)
{
    struct stat s;
    if (stat(path.get(), &s) == -1)
    {
        LOG(ERROR) << "could not stat file " << path;
        return -1;
    }

    return s.st_size;
}

bool
blockmap :: setup(const po6::pathname& path, const po6::pathname& backing_path)
{
    leveldb::Options opts;
    opts.write_buffer_size = 64ULL * 1024ULL * 1024ULL;
    opts.create_if_missing = true;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    std::string name(path.get());
    leveldb::DB* tmp_db;
    leveldb::Status st = leveldb::DB::Open(opts, name, &tmp_db);

    if (!st.ok())
    {
        LOG(ERROR) << "could not open LevelDB: " << st.ToString();
        return false;
    }

    m_db.reset(tmp_db);
    leveldb::ReadOptions ropts;
    ropts.fill_cache = true;
    ropts.verify_checksums = true;

    leveldb::Slice rk("wtf", 3);
    std::string rbacking;
    st = m_db->Get(ropts, rk, &rbacking);
    bool first_time = false;

    if (st.ok())
    {
        first_time = false;
    }
    else if (st.IsNotFound())
    {
        first_time = true;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "could not restore from LevelDB because of corruption:  "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "could not restore from LevelDB because of an IO error:  "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "could not restore from LevelDB because it returned an "
                   << "unknown error that we don't know how to handle:  "
                   << st.ToString();
        return false;
    }

    leveldb::Slice sk("state", 5);
    std::string sbacking;
    st = m_db->Get(ropts, sk, &sbacking);

    if (st.ok())
    {
        if (first_time)
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution crashed and the database was tampered with; "
                       << "you're on your own with this one";
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        if (!first_time)
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution crashed; run the recovery program and try again";
            return false;
        }
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "could not restore from LevelDB because of corruption:  "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "could not restore from LevelDB because of an IO error:  "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "could not restore from LevelDB because it returned an "
                   << "unknown error that we don't know how to handle:  "
                   << st.ToString();
        return false;
    }

    if (first_time)
    {
        m_fd = open(backing_path.get(), O_RDWR | O_CREAT);
        if (m_fd.get() < 0)
        {
            LOG(ERROR) << "could not open backing file " << backing_path;
            return false;
        }

        if (ftruncate(m_fd.get(), m_backing_size) < 0)
        {
            LOG(ERROR) << "could not extend backing file to size " << m_backing_size;
            return false;
        }

        m_backing = mmap(NULL, m_backing_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

        if (m_backing == MAP_FAILED)
        {
            LOG(ERROR) << "mmap of " << m_backing_size << " bytes to file " << backing_path << " failed.";
            return false;
        }

        return true;
    }
    else
    {
        e::unpacker up(sbacking.data(), sbacking.size());
        up = up >> m_backing_offset >> m_backing_size;
        if (up.error())
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                << "execution saved invalid state.";
            return false;
        }

        e::slice data = up.as_slice();
        m_fd = open(data.data(), O_RDWR);
        if (m_fd.get() < 0)
        {
            LOG(ERROR) << "could not open backing file " << data.data();
            return false;
        }

        if (ftruncate(m_fd.get(), m_backing_size) < 0)
        {
            LOG(ERROR) << "could not extend backing file to size " << m_backing_size;
            return false;
        }

        m_backing = mmap(NULL, m_backing_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

        if (m_backing == MAP_FAILED)
        {
            LOG(ERROR) << "mmap of " << m_backing_size << " bytes to file " << m_backing_file << " failed.";
            return false;
        }

        return true;
    return true;
}
