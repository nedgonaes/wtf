#include "interval_map.h"
#include <iostream>

#define TEST_SUCCESS() \
    do { \
        std::cout << "Test " << __func__ << ":  [\x1b[32mOK\x1b[0m]\n"; \
    } while (0)

#define TEST_FAIL() \
    do { \
        std::cout << "Test " << __func__ << ":  [\x1b[31mFAIL\x1b[0m]\n" \
                  << "location: " << __FILE__ << ":" << __LINE__<< "\n" \
                  << "dump:  " << std::endl; \
        print_slices(slices); \
    } while (0)

#define CHECK(INDEX, LEN, OFFSET, LOC) \
    do { \
        if (slices[INDEX].length != LEN \
            || slices[INDEX].offset != OFFSET \
            || slices[INDEX].location.sid != LOC.sid) \
        { \
            TEST_FAIL(); \
            return -1; \
        } \
    } while(0)

#define CHECK_SIZE(SIZE) \
    do { \
    if(slices.size() != SIZE) \
    { \
        TEST_FAIL(); \
        return -1; \
    } \
    } while(0)


interval_map imap;
block_location location1;
block_location location2;
block_location location3;
block_location location4;

void print_slices(std::vector<slice>& slices)
{
    std::cout << "size : " << slices.size() << std::endl;

    for (int i = 0; i < slices.size(); ++i)
    {
        slice s = slices[i];
        std::cout << " location : " << s.location.sid ;
        std::cout << " / length : " << s.length;
        std::cout << " / offset : " << s.offset << std::endl ;
    }
}

int case0()
{
    imap.clear();
    imap.insert(0, 10, location1);
    std::vector<slice> slices = imap.get_slices(0,10);

    CHECK(0,10,0,location1);
    TEST_SUCCESS();
}

int case1()
{
    /*
       imap.insert slice 0-10
       imap.insert slice 3-7
       imap.insert slice 5-6
       get slice 0-10 : should get 5 slices
     */
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(3, 4, location2);
    imap.insert(5, 1, location3);
    std::vector<slice> slices = imap.get_slices(0, 10);

    CHECK(0, 3, 0, location1);
    CHECK(1, 2, 0, location2);
    CHECK(2, 1, 0, location3);
    CHECK(3, 1, 3, location2);
    CHECK(4, 3, 7, location1);
    TEST_SUCCESS();
}

int case11()
{
    /*
       imap.insert slice 0-10
       imap.insert slice 0-6
       imap.insert slice 0-3
       get slice 0-10 : should get 3 slices
     */
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(0, 6, location2);
    imap.insert(0, 3, location3);
    std::vector<slice> slices = imap.get_slices(0, 10);
    CHECK(0,3,0,location3);
    CHECK(1,3,3,location2);
    CHECK(2,4,6,location1);
    TEST_SUCCESS();
}

int case111()
{
    /*
       imap.insert slice 0-10
       imap.insert slice 4-10
       imap.insert slice 7-10
       get slice 0-10 : should get 3 slices
     */
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(4, 6, location2);
    imap.insert(7, 3, location3);
    std::vector<slice> slices = imap.get_slices(0, 10);
    CHECK(0,4,0,location1);
    CHECK(1,3,0,location2);
    CHECK(2,3,0,location3);
    TEST_SUCCESS();
}

int case1111()
{
    /*
       imap.insert slice 0-10
       imap.insert slice 4-10
       imap.insert slice 4-6
       get slice 0-10 : should get 3 slices
     */
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(4, 6, location2);
    imap.insert(4, 2, location3);
    std::vector<slice> slices = imap.get_slices(0, 10);
    CHECK(0,4,0,location1);
    CHECK(1,2,0,location3);
    CHECK(2,4,2,location2);
    TEST_SUCCESS();
}

int case11111()
{
    imap.clear();
    imap.insert(0,10,location1);
    imap.insert(10,10,location2);
    imap.insert(20,10,location3);
    imap.insert(0,5,location4);
    std::vector<slice> slices = imap.get_slices(0,30);
    CHECK(0,5,0,location4);
    CHECK(1,5,5,location1);
    CHECK(2,10,0,location2);
    CHECK(3,10,0,location3);
    TEST_SUCCESS();
}

int case111111()
{
    imap.clear();
    imap.insert(0,10,location1);
    imap.insert(10,10,location2);
    imap.insert(20,10,location3);
    imap.insert(25,15,location4);
    std::vector<slice> slices = imap.get_slices(0,40);
    CHECK(0,10,0,location1);
    CHECK(1,10,0,location2);
    CHECK(2,5,0,location3);
    CHECK(3,15,0,location4);
    TEST_SUCCESS();
}

int case2()
{

    /*
       imap.insert slice 0-10
       imap.insert slice 10-20
       imap.insert slice 5-15
       get slice 0-20 : should get 5 slices
     */
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(10, 20, location2);
    imap.insert(5, 10, location3);
    std::vector<slice> slices = imap.get_slices(0, 20);
    CHECK(0,5,0,location1);
    CHECK(1,10,0,location3);
    CHECK(2,5,5,location2);
    TEST_SUCCESS();

}

int case235()
{

    /*
       imap.insert slice 0-10
       imap.insert slice 10-20
       imap.insert slice 20-30
       imap.insert slice 5-25
       get slice 0-20 : should get 3 slices
     */
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(10, 10, location2);
    imap.insert(20, 10, location3);
    imap.insert(5, 20, location4);
    std::vector<slice> slices = imap.get_slices(0, 30);
    CHECK(0,5,0,location1);
    CHECK(1,20,0,location4);
    CHECK(2,5,5,location3);
    TEST_SUCCESS();

}

int case5()
{
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(10,10, location2);
    imap.insert(20,10, location3);
    imap.insert(10,10, location4);

    std::vector<slice> slices = imap.get_slices(0,30);
    CHECK(0,10,0,location1);
    CHECK(1,10,0,location4);
    CHECK(2,10,0,location3);
    TEST_SUCCESS();
}

int case55()
{
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(10,10, location2);
    imap.insert(20,10, location3);
    imap.insert(10,20, location4);

    std::vector<slice> slices = imap.get_slices(0,30);
    CHECK(0,10,0,location1);
    CHECK(1,20,0,location4);
    TEST_SUCCESS();
}

int case255()
{
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(10,10, location2);
    imap.insert(20,10, location3);
    imap.insert(5,25, location4);

    std::vector<slice> slices = imap.get_slices(0,30);
    CHECK(0,5,0,location1);
    CHECK(1,25,0,location4);
    TEST_SUCCESS();
}

int case253()
{
    imap.clear();
    imap.insert(0, 10, location1);
    imap.insert(10,10, location2);
    imap.insert(20,10, location3);
    imap.insert(5,35, location4);

    std::vector<slice> slices = imap.get_slices(0,40);
    CHECK(0,5,0,location1);
    CHECK(1,35,0,location4);
    TEST_SUCCESS();
}

int read1()
{
    imap.clear();
    imap.insert(0, 10, location1);

    std::vector<slice> slices = imap.get_slices(0,15);

    CHECK_SIZE(1);
    CHECK(0,10,0,location1);
    TEST_SUCCESS();

}

int read2()
{
    imap.clear();
    imap.insert(0, 20, location1);
    imap.insert(0, 10, location2);

    std::vector<slice> slices = imap.get_slices(5,10);
    CHECK_SIZE(2);
    CHECK(0,5,5,location2);
    CHECK(1,5,10,location1);
    TEST_SUCCESS();
}

int read3()
{
    imap.clear();
    imap.insert(0, 20, location1);
    imap.insert(0, 10, location2);

    std::vector<slice> slices = imap.get_slices(15,5);
    CHECK_SIZE(1);
    CHECK(0,5,15,location1);
    TEST_SUCCESS();
}

int read4()
{
    //should be empty
    imap.clear();
    std::vector<slice> slices = imap.get_slices(15,5);
    CHECK_SIZE(0);
    TEST_SUCCESS();
}

int read5()
{
    //should be empty
    imap.clear();
    imap.insert(0, 10, location1);
    std::vector<slice> slices = imap.get_slices(15,5);
    CHECK_SIZE(0);
    TEST_SUCCESS();
}

int main()
{
    location1.sid = 1100;
    location1.bid = 1100;
    location2.sid = 1200;
    location2.bid = 1200;
    location3.sid = 1300;
    location3.bid = 1300;
    location4.sid = 1400;
    location4.bid = 1400;
/*
    case0();
    case1();
    case11();
    case111();
    case1111();
    case11111();
    case111111();
    case2();
    case235();
    case5();
    case55();
    case255();
    case253();
    read1();
    read2();
    read3();
    read4();
  */
    read5();
}

