#include "interval_map.h"

using wtf::interval_map;

interval_map :: interval_map()
    : slice_map()
{
}

interval_map :: ~interval_map()
{
}

void
interval_map :: insert(unsigned int insert_address, unsigned int insert_length,
                       block_location insert_location)
{

    slice_iter_t it = slice_map.lower_bound(insert_address);

    if(slice_map.size() == 0)
    {
        //inserting into empty map at > 0 offset, fill the hole
        if (insert_address > 0)
        {
            slice empty_slice;
            empty_slice.length = insert_address;
            slice_map.insert(std::make_pair(0, empty_slice));
        }
    }

    else if (it == slice_map.end())
    {
        --it;
        unsigned int block_start = it->first;
        unsigned int length = it->second.length;

        //there's a gap, need to fill
        if (block_start + length < insert_address)
        {
            slice empty_slice;
            empty_slice.length = insert_address - block_start + length;
            slice_map.insert(std::make_pair(block_start + length, empty_slice));
        }

        //new slice is entirely contained within last block of file
        else if (block_start + length > insert_address + insert_length)
        {
            insert_contained(block_start, length, insert_address, insert_length);
        }

        //new slice overlaps right portion of last block of file.
        else if (block_start + length != insert_address)
        {
            insert_right(block_start, insert_address);
        }

        //new slice is exactly at the end of the file, no adjustment

    }

    else
    {
        if (it->first != insert_address)
        {
            //definitely safe because if it == slice_map.begin().
            //then it->first == insert_address
            --it;

            unsigned int block_start = it->first;
            unsigned int length = it->second.length;

            //new slice is entirely contained within an existing block
            if (block_start + length > insert_address + insert_length)
            {
                insert_contained(block_start, length, insert_address, insert_length);
            }

            //new slice overlaps right side of a block, and possibly multiple
            //subsequent blocks, and then possibly left side of another block
            else
            {
                insert_right(block_start, insert_address);

                ++it;

                while (it != slice_map.end() &&
                        it->first + it->second.length < insert_address + insert_length)
                {
                    block_start = it->first;
                    ++it;
                    insert_overwrite_interval(block_start);
                }

                if (it != slice_map.end())
                {
                    block_start = it->first;
                    length = it->second.length;
                    insert_left(block_start, length, insert_address, insert_length);
                }
            }
        }

        //insert directly on an existing block boundary (do not need to consider
        //modifying block to left, or case where slice is fully contained within
        //an existing block).
        else
        {
            while (it != slice_map.end() &&
                    it->first + it->second.length < insert_address + insert_length)
            {
                unsigned int block_start = it->first;
                ++it;
                insert_overwrite_interval(block_start);
            }

            if (it != slice_map.end())
            {
                unsigned int block_start = it->first;
                unsigned int length = it->second.length;
                insert_left(block_start, length, insert_address, insert_length);
            }
        }
    }

    insert_interval(insert_address, insert_length, insert_location);

}

//case 1: wholely contained within a larger block
void
interval_map :: insert_contained(
    unsigned int block_start_address,
    unsigned int block_length,
    unsigned int insert_address,
    unsigned int insert_length)
{
  block_location slice_location = slice_map[block_start_address].location;

  unsigned int new_length = insert_address - block_start_address;
  slice_map[block_start_address].length = new_length;

  unsigned int new_block_start = block_start_address + new_length + insert_length;
  unsigned int new_block_length = block_length - new_length - insert_length;

  slice new_slice;
  new_slice.location = slice_location;
  new_slice.offset = new_length + insert_length;
  new_slice.length = new_block_length;

  slice_map.insert(
      std::pair<unsigned int, slice>(new_block_start, new_slice));
}

//case 2: overlaps right of another block
void
interval_map :: insert_right(
    unsigned int block_start_address,
    unsigned int insert_address)
{
  unsigned int new_length = insert_address - block_start_address;
  slice_map[block_start_address].length = new_length;
};

//case 3: overlaps left of another block
void
interval_map :: insert_left(
    unsigned int block_start_address,
    unsigned int block_length,
    unsigned int insert_address,
    unsigned int insert_length)
{
  unsigned int new_offset = insert_address + insert_length - block_start_address;
  unsigned int new_length = block_length - new_offset;
  unsigned int new_block_start = block_start_address + new_offset;
  block_location location =  slice_map[block_start_address].location;

  slice_map.erase(block_start_address);

  slice new_slice;
  new_slice.location = location;
  new_slice.offset = new_offset;
  new_slice.length = new_length;

  slice_map.insert(std::pair<unsigned int, slice>(new_block_start, new_slice));
}

//case 5: exactly same size as existing block
void
interval_map :: insert_overwrite_interval(
    unsigned int block_start_address)
{
  //delete mapping of block_start_address
  slice_map.erase(block_start_address);
}

//insert the new interval into slice map
void
interval_map :: insert_interval(
    unsigned int insert_address,
    unsigned int insert_length,
    block_location insert_location)
{
  slice new_slice;
  new_slice.location = insert_location;
  new_slice.offset =  0;
  new_slice.length = insert_length;

  slice_map.insert(std::pair<unsigned int, slice>(insert_address, new_slice));
}

//gets the slices associated with the request
std::vector<wtf::slice>
interval_map :: get_slices (
    unsigned int request_address, unsigned int request_length)
{
  std::vector<slice> slice_vector;
  slice_iter_t it = slice_map.lower_bound(request_address);
  //out of range, return empty vector
  if(slice_map.size() == 0 || request_length == 0)
  {
    return slice_vector;
  }
  if( it == slice_map.end())
  {
    //last block, implies that it is contained within this block.
    --it;
    unsigned int block_address = it->first;
    slice s = it->second;
    unsigned int block_offset = s.offset;
    unsigned int block_length = s.length;

    //out of range, return empty vector
    if(block_address + block_length < request_address)
      return slice_vector;

    unsigned int new_offset = request_address - block_address + block_offset;
    s.offset = new_offset;
    s.length = request_length;
    slice_vector.push_back(s);

    return slice_vector;
  }
  if(it->first != request_address)
  {
    --it;
  }
  while(it != slice_map.end() &&
      it->first < request_address + request_length)
  {
    unsigned int block_address = it->first;
    slice s = it->second;
    unsigned int block_length = s.length;
    unsigned int block_offset = s.offset;
    unsigned int new_offset = 0;
    unsigned int new_length = 0;

    //if request is contained within a block
    if( request_address >= block_address &&
        request_address + request_length <= block_address + block_length)
    {
      s.offset = request_address - block_address + block_offset;
      s.length = request_length;
      slice_vector.push_back(s);
      return slice_vector;
    }
    //if request starts after block address
    else if( block_address < request_address)
    {
      new_offset = request_address - block_address + block_offset;
      new_length = block_address + block_length - request_address;
    }
    else
    {
      new_offset = (block_address < request_address) ?
        request_address - block_address : block_offset;
      new_length =
        (block_address + block_length <= request_address + request_length)?
        block_length : (request_address + request_length) - block_address;
    }

    s.offset = new_offset;
    s.length = new_length;
    slice_vector.push_back(s);

    it++;
  }
  return slice_vector;
}

void
interval_map :: clear()
{
    slice_map.clear();
}
