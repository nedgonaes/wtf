#include <blockstore/blockmap.h>
#include <stdlib.h> 
#include "gtest/gtest.h"

namespace {

    // The fixture for testing class Foo.
    class BlockstoreTest : public ::testing::Test {
        protected:
            // You can remove any or all of the following functions if its body
            // is empty.

            BlockstoreTest() {
                // You can do set-up work for each test here.
                //system("rm -rf /home/sean/src/wtf/wtf-data/daemon/*");
            }

            virtual ~BlockstoreTest() {
                // You can do clean-up work that doesn't throw exceptions here.
            }

            // If the constructor and destructor are not enough for setting up
            // and cleaning up each test, you can define the following methods:

            virtual void SetUp() {
            }

            virtual void TearDown() {
                // Code here will be called immediately after each test (right
                // before the destructor).
            }

            // Objects declared here can be used by all tests in the test case for Blockstore.
    };

    TEST_F(BlockstoreTest, OverwriteMiddleWorks)
    {
        bool status; 
        std::tr1::shared_ptr<wtf::blockmap> bm(new wtf::blockmap());
        status = bm->setup(po6::pathname("/home/sean/src/wtf/wtf-data/daemon/data"),
                po6::pathname("/home/sean/src/wtf/wtf-data/daemon/metadata"));
        ASSERT_EQ(status, true);

        uint64_t bid;
        std::string data("hello");
        e::slice slc(data.data(), data.size());

        std::string data2("deadbeef");
        e::slice slc2(data2.data(), data2.size());


        for (int i = 0; i < 5; ++i)
        {
            ASSERT_GE(bm->write(slc, bid), 0);
        }

        for (int i = 0; i < 5; ++i)
        {
            uint8_t buf[5];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)), 0);
            ASSERT_EQ(std::string("hello"), std::string((char*)buf,sizeof(buf)));
        }

        for (uint64_t i = 0; i < 5; ++i)
        {
            uint64_t j = i;
            ASSERT_GE(bm->update(slc, (uint64_t)5, j), 0);
        }

        for (uint64_t i = 0; i < 5; ++i)
        {
            uint8_t buf[5];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)), 0);
            ASSERT_EQ(std::string("hello"), std::string((char*)buf,sizeof(buf)));
        }

        for (uint64_t i = 5; i < 10; ++i)
        {
            uint8_t buf[10];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)), 0);
            ASSERT_EQ(std::string("hellohello"), std::string((char*)buf,sizeof(buf)));
        }

        for (uint64_t i = 5; i < 10; ++i)
        {
            uint64_t j = i;
            ASSERT_GE(bm->update(slc2, (uint64_t)1, j), 0);
        }

        for (uint64_t i = 10; i < 15; ++i)
        {
            uint8_t buf[10];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)),0);
            ASSERT_EQ(std::string("hdeadbeefo"), std::string((char*)buf,sizeof(buf)));
        }
    }

    TEST_F(BlockstoreTest, UpdateBeginningWorks)
    {
        bool status; 
        std::tr1::shared_ptr<wtf::blockmap> bm(new wtf::blockmap());
        status = bm->setup(po6::pathname("/home/sean/src/wtf/wtf-data/daemon/data"),
                po6::pathname("/home/sean/src/wtf/wtf-data/daemon/metadata"));
        ASSERT_EQ(status, true);

        uint64_t bid;
        std::string data("hello");
        e::slice slc(data.data(), data.size());

        std::string data2("c");
        e::slice slc2(data2.data(), data2.size());


        for (int i = 0; i < 5; ++i)
        {
            ASSERT_GE(bm->write(slc, bid), 0);
        }

        for (int i = 0; i < 5; ++i)
        {
            uint8_t buf[5];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)), 0);
            ASSERT_EQ(std::string("hello"), std::string((char*)buf,sizeof(buf)));
        }

        for (uint64_t i = 0; i < 5; ++i)
        {
            uint64_t j = i;
            ASSERT_GE(bm->update(slc2, (uint64_t)0, j), 0);
        }

        for (uint64_t i = 0; i < 5; ++i)
        {
            uint8_t buf[5];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)), 0);
            ASSERT_EQ(std::string("hello"), std::string((char*)buf,sizeof(buf)));
        }

        for (uint64_t i = 5; i < 10; ++i)
        {
            uint8_t buf[5];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)), 0);
            ASSERT_EQ(std::string("cello"), std::string((char*)buf,sizeof(buf)));
        }
    }

    TEST_F(BlockstoreTest, UpdateWholeWordWorks)
    {
        bool status; 
        std::tr1::shared_ptr<wtf::blockmap> bm(new wtf::blockmap());
        status = bm->setup(po6::pathname("/home/sean/src/wtf/wtf-data/daemon/data"),
                po6::pathname("/home/sean/src/wtf/wtf-data/daemon/metadata"));
        ASSERT_EQ(status, true);

        uint64_t bid;
        std::string data("hello");
        e::slice slc(data.data(), data.size());

        std::string data2("olleh");
        e::slice slc2(data2.data(), data2.size());


        for (int i = 0; i < 5; ++i)
        {
            ASSERT_GE(bm->write(slc, bid), 0);
        }

        for (int i = 0; i < 5; ++i)
        {
            uint8_t buf[5];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)), 0);
            ASSERT_EQ(std::string("hello"), std::string((char*)buf,sizeof(buf)));
        }

        for (uint64_t i = 0; i < 5; ++i)
        {
            uint64_t j = i;
            ASSERT_GE(bm->update(slc2, (uint64_t)0, j), 0);
        }

        for (uint64_t i = 0; i < 5; ++i)
        {
            uint8_t buf[5];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)), 0);
            ASSERT_EQ(std::string("hello"), std::string((char*)buf,sizeof(buf)));
        }

        for (uint64_t i = 5; i < 10; ++i)
        {
            uint8_t buf[5];
            ASSERT_GE(bm->read(i, buf, 0, sizeof(buf)), 0);
            ASSERT_EQ(std::string("olleh"), std::string((char*)buf,sizeof(buf)));
        }
    }


}  // namespace

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
