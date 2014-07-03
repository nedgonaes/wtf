%module wtf_client 
%{
#include <wtf/client.hpp>
#include <wtf/client.h>
using namespace wtf;

%}
%include cpointer.i
%include various.i
%include stdint.i
%include std_vector.i
%include std_string.i
%include "enums.swg"
%javaconst(1);
%pragma(java) jniclasscode=
%{
    static
    {
        System.loadLibrary("wtf-client-java");
    }
%}

%include std_string.i
%include typemaps.i
%apply int {mode_t}
%apply int {off_t}
%apply long long *INOUT {size_t *data_sz};
%apply char *BYTE {char *data};
%apply long *OUTPUT {wtf_client_returncode *status}
%apply long *OUTPUT {int64_t *fd}
%template(StringVec) std::vector<std::string>;
%ignore "";

%rename("%s") "wtf_file_attrs";
%rename("%s") "testbuf";
%rename("%s") "Client";
%rename("%s", %$ismember) "";

%include "wtf/client.h"
%include "wtf/client.hpp"
%{
void testbuf(char* data)
{
    data[0]='x';
}
%}
void testbuf(char* data);

