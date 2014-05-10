// C/C++
#include <stdio.h>
#include <string.h>

// WTF 
#include "client/rereplicate.h"

using namespace std;

int
main(int argc, const char* argv[])
{
    if (argv[1] == NULL || argv[2] == NULL)
    {
        cout << "insufficient arguments" << endl;
        return EXIT_FAILURE;
    }
    else
    {
        const char* filename = argv[1];
        uint64_t sid = strtoull(argv[2], NULL, 10);

        wtf::rereplicate re("127.0.0.1", 1981, "127.0.0.1", 1982);
        int64_t ret = re.replicate(filename, sid);

        return ret;
    }
}
