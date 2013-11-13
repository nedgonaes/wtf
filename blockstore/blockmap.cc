#include "blockmap.h"
#define BACKING_SIZE 2147483648
#define ROUND_UP(X, Y) ((X + Y - 1) & (X))

using wtf::blockmap;
using wtf::vblock;

blockmap::blockmap() : m_db()
                     , m_backing_size(ROUND_UP(BACKING_SIZE, getpagesize()))
                     , m_block_id(0)
{
    LOG(INFO) << "m_backing_size = " << BACKING_SIZE << " + " << getpagesize();
}

blockmap::~blockmap() {};

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

    //create a new disk.
    //XXX: Set to true for testing
    if (true)
    {
        m_fd = open(backing_path.get(), O_RDWR | O_CREAT, 0666);
        if (m_fd.get() < 0)
        {
            PLOG(ERROR) << "could not open backing file " << backing_path;
            return false;
        }

        if (ftruncate(m_fd.get(), m_backing_size) < 0)
        {
            LOG(ERROR) << "could not extend backing file to size " << m_backing_size;
            return false;
        }

        char* backing = (char*)mmap(NULL, m_backing_size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd.get(), 0);

        if (backing == MAP_FAILED)
        {
            LOG(ERROR) << "mmap of " << m_backing_size << " bytes to file " << backing_path << " failed.";
            return false;
        }

        LOG(INFO) << "mmaping region " << (void*)backing << "->" << (void*)(backing + m_backing_size);
        m_disk = new disk(backing, m_backing_size);

        return true;
    }

    //use an existing disk.
    e::unpacker up(sbacking.data(), sbacking.size());
    uint64_t backing_offset;
    up = up >> backing_offset >> m_backing_size;
    if (up.error())
    {
        LOG(ERROR) << "could not restore from LevelDB because a previous "
            << "execution saved invalid state.";
        return false;
    }

    e::slice data = up.as_slice();
    m_fd = open((char*)data.data(), O_RDWR, 0666);
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

    char* backing = (char*)mmap(NULL, m_backing_size, PROT_READ | PROT_WRITE, MAP_SHARED, m_fd.get(), 0);

    if (backing == MAP_FAILED)
    {
        LOG(ERROR) << "mmap of " << m_backing_size << " bytes to file " << backing_path << " failed.";
        return false;
    }

    m_disk = new disk(backing, m_backing_size);

    return true;
}

ssize_t 
blockmap :: read_offset_map(uint64_t bid, vblock& vb)
{
    leveldb::ReadOptions ropts;
    ropts.fill_cache = true;
    ropts.verify_checksums = true;

    leveldb::Slice rk((char*)&bid, sizeof(bid));
    std::string rbacking;
    leveldb::Status st = m_db->Get(ropts, rk, &rbacking);

    if (!st.ok())
    {
        return -1;
    }

    std::auto_ptr<e::buffer> buf(e::buffer::create(rbacking.data(), rbacking.size()));
    e::unpacker up = buf->unpack_from(0);

    up = up >> vb;

    return 0;
}

ssize_t 
blockmap :: update_offset_map(uint64_t bid, vblock& vb, size_t offset, size_t len, size_t disk_offset)
{
    ssize_t status = read_offset_map(bid, vb);
    vb.update(offset, len, disk_offset);
}

ssize_t
blockmap :: write_offset_map(uint64_t bid, vblock& vb)
{

    std::auto_ptr<e::buffer> buf(e::buffer::create(vb.pack_size()));
    e::buffer::packer pa = buf->pack_at(0);
    pa = pa << vb;

    leveldb::WriteBatch updates;

    // create the key
    leveldb::Slice v_block_id((char*)&bid, sizeof(bid));


    // create the value
    leveldb::Slice offset_map((char*)buf->data(), buf->size());

    // put the object
    updates.Put(v_block_id, offset_map);

    // Perform the write
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Status st = m_db->Write(opts, &updates);

    if (st.ok())
    {
        return 0;
    }
    else
    {
        return -1;
    }

}


//Write a completely new block.
ssize_t
blockmap :: write(const e::slice& data,
                 uint64_t& bid)
{
    ssize_t status = -1;
    size_t offset;

    status = m_disk->write(data, offset);
    if (status < 0)
    {
        return status;
    }

    bid = m_block_id++;

    vblock vb;
    vb.update(0, data.size(), offset);

    if (write_offset_map(bid, vb) < 0)
    {
        return -1;
    }
    else
    {
        return status;
    }
}

ssize_t
blockmap :: update(const e::slice& data,
             uint64_t offset,
             uint64_t& bid)
{
    return 0;
}

ssize_t 
blockmap :: read(uint64_t bid,
                 uint8_t* data, 
                 size_t len)
{   
    vblock vb;

    if (read_offset_map(bid, vb) < 0)
    {
        return -1;
    }

    e::intrusive_ptr<vblock::slice> s = vb.slice_at(0);

    size_t readlen = len > s->length() ? s->length() : len;

    return m_disk->read(s->offset(), readlen, (char*)data);
}

