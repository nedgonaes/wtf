%module test 
%{
#include <wtf/client.hpp>
#include <wtf/client.h>
using namespace wtf;
%}
%include cpointer.i
%include various.i
%include stdint.i

//%pointer_functions(struct wtf_file_attrs,wtf_file_attrs_ptr);

%include std_string.i
%include typemaps.i
%apply int {mode_t}
%apply int {off_t}
%apply unsigned long *INOUT {size_t *data_sz};
%apply int *OUTPUT {wtf_client_returncode *status}
%apply char *BYTE {char *data}

%ignore "";

%rename("%s") "wtf_file_attrs";
%rename("%s") "Client";
%rename("%s", %$ismember) "";

%include "wtf/client.h"
%include "wtf/client.hpp"
