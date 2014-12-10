#ifndef interval_map_h_
#define interval_map_h_
#include <stdint.h>
#include <map>
#include <vector>
#include "common/block_location.h"

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

    public:
        void insert(uint64_t insert_address, 
                    uint64_t insert_length,
                    std::vector<block_location>& insert_location);
        std::vector<slice> get_slices
          (uint64_t request_address, uint64_t request_length);
        void clear();
        int64_t pack_size();
};
}
#endif //interval_map_h_
