#include <blockstore/blockmap.h>

int main()
{
    bool status; 
    wtf::blockmap *bm = new wtf::blockmap();
    status = bm->setup(po6::pathname("/home/sean/src/wtf/wtf-data/daemon/data"),
            po6::pathname("/home/sean/src/wtf/wtf-data/daemon/metadata"));
    if(!status)
    {
        std::cout << "Setup failed." << std::endl;
        return -1;
    }

    uint64_t bid;
    std::string data("hello");
    e::slice slc(data.data(), data.size());

    for (int i = 0; i < 20; ++i)
    {
        ssize_t ret = bm->write(slc, bid);

        if(ret < 0)
        {
            PLOG(ERROR) << "Write failed.";
        }

        LOG(INFO) << "bid = " << bid;
    }

    for (int i = 1; i < 21; ++i)
    {
        uint8_t buf[5];
        ssize_t ret = bm->read(i, buf, sizeof(buf));

        if(ret < 0)
        {
            PLOG(ERROR) << "Write failed.";
        }

        LOG(INFO) << "bid = " << i << " value = " << buf;
    }
    return 0;
}

