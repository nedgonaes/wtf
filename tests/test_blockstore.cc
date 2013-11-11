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

    return 0;
}

