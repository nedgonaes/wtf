#ifndef interval_map_h_
#define interval_map_h_
#include <stdint.h>
#include <map>
#include <vector>
#include "common/block_location.h"
#include <e/buffer.h>

namespace wtf __attribute__ ((visibility("hidden")))
{

class slice
{
    public:
    slice()
        : location()
        , offset()
        , length() {}

    ~slice() {};

    friend e::buffer::packer
        operator << (e::buffer::packer pa, const slice& rhs);
    friend e::unpacker 
        operator >> (e::unpacker up, slice& rhs);

    public:
    int64_t pack_size();

    public:
    std::vector<block_location> location;
    uint64_t offset;
    uint64_t length;
};

class interval_map
{
    public:
        interval_map();
        ~interval_map();

    private:
        interval_map(const interval_map&);

    private:
        void insert_contained (
                uint64_t block_start_address,
                uint64_t block_length,
                uint64_t insert_address,
                uint64_t insert_length);
        void insert_right(
                uint64_t block_start_address,
                uint64_t insert_address);
        void insert_left(
                uint64_t block_start_address,
                uint64_t block_length,
                uint64_t insert_address,
                uint64_t insert_length);
        void insert_overwrite_interval(
                uint64_t block_start_address);
        void insert_interval(
                uint64_t insert_address,
                uint64_t insert_length,
                std::vector<block_location>& insert_location);

    private:
        typedef std::map<uint64_t, slice> slice_map_t;
        typedef std::map<uint64_t, slice>::iterator slice_iter_t;
        interval_map& operator = (const interval_map&);

    private:
        slice_map_t slice_map;

    friend e::buffer::packer 
        operator << (e::buffer::packer pa, const interval_map& rhs);
    friend e::unpacker 
        operator >> (e::unpacker up, interval_map& rhs);

    public:
        void insert(uint64_t insert_address, 
                    uint64_t insert_length,
                    std::vector<block_location>& insert_location);
        void insert(uint64_t insert_address,
                    wtf::slice& slc);
        std::vector<slice> get_slices
          (uint64_t request_address, uint64_t request_length);
        void clear();
        int64_t pack_size();
};

inline e::buffer::packer 
operator << (e::buffer::packer pa, const slice& rhs) 
{
    uint64_t sz = rhs.location.size();
    pa = pa << rhs.offset << rhs.length << sz;

    for (std::vector<block_location>::const_iterator it = rhs.location.begin();
         it != rhs.location.end(); ++it)
    {
        pa = pa << *it;
    }

}

inline e::unpacker 
operator >> (e::unpacker up, slice& rhs) 
{
    uint64_t sz;

    up = up >> rhs.offset >> rhs.length >> sz;

    std::vector<block_location> locations;
    for (uint64_t i = 0; i < sz; ++i)
    {
        block_location bl;
        up = up >> bl;
        locations.push_back(bl);
    }

    rhs.location = locations;
}

inline e::buffer::packer 
operator << (e::buffer::packer pa, const interval_map& rhs) 
{
    uint64_t sz = rhs.slice_map.size();

    pa = pa << sz;

    for (std::map<uint64_t, wtf::slice>::const_iterator it = rhs.slice_map.begin();
         it != rhs.slice_map.end(); ++it)
    {
        pa = pa << it->second;
    }
}

inline e::unpacker 
operator >> (e::unpacker up, interval_map& rhs) 
{
    uint64_t sz;
    uint64_t insert_address = 0;

    up = up >> sz;

    for (uint64_t i = 0; i < sz; ++i)
    {
        wtf::slice slc;
        up = up >> slc;
        rhs.insert(insert_address, slc);
        insert_address += slc.length;
    }
}

}
#endif //interval_map_h_
