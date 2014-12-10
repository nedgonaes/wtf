#ifndef interval_map_h_
#define interval_map_h_
#include <stdint.h>
#include <map>
#include <vector>

class block_location
{
    public:
    block_location()
        : sid(0)
          , bid(0) {}

    ~block_location() {};

    public:
    uint64_t sid;
    uint64_t bid;
};

class slice
{
    public:
    slice()
        : location()
        , offset()
        , length() {}

    ~slice() {};

    public:
    block_location location;
    unsigned int offset;
    unsigned int length;
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
                unsigned int block_start_address,
                unsigned int block_length,
                unsigned int insert_address,
                unsigned int insert_length);
        void insert_right(
                unsigned int block_start_address,
                unsigned int insert_address);
        void insert_left(
                unsigned int block_start_address,
                unsigned int block_length,
                unsigned int insert_address,
                unsigned int insert_length);
        void insert_overwrite_interval(
                unsigned int block_start_address);
        void insert_interval(
                unsigned int insert_address,
                unsigned int insert_length,
                block_location insert_location);

    private:
        typedef std::map<unsigned int, slice> slice_map_t;
        typedef std::map<unsigned int, slice>::iterator slice_iter_t;
        interval_map& operator = (const interval_map&);

    private:
        slice_map_t slice_map;

    public:
        void insert(unsigned int insert_address, 
                    unsigned int insert_length,
                    block_location insert_location);
        std::vector<slice> get_slices
          (unsigned int request_address, unsigned int request_length);
        void clear();
};

#endif //interval_map_h_
