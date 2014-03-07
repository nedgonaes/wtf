%module wtf_client 
%{
#include <wtf/client.hpp>
#include <wtf/client.h>
using namespace wtf;
%}
%include cpointer.i
%include various.i
%include stdint.i
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
%apply (char *STRING, size_t LENGTH) { (char *data, size_t data_sz) };
%apply int *OUTPUT {wtf_client_returncode *status}

%ignore "";

%rename("%s") "wtf_file_attrs";
%rename("%s") "Client";
%rename("%s", %$ismember) "";

%include "wtf/client.h"
%include "wtf/client.hpp"
