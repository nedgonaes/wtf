# Copyright (c) 2013, Sean Ogden
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of WTF nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}
AM_CXXFLAGS = $(WANAL_CXXFLAGS) $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(LRT_CFLAGS)
AM_LDFLAGS = $(PO6_LDFLAGS) $(E_LDFLAGS) $(BUSYBEE_LDFLAGS)

lib_LTLIBRARIES = libwtf.la

include_HEADERS = \
			client/wtf.h

noinst_HEADERS = \
            client/command.h \
			common/chain_node.h \
			common/macros.h \
			common/mapper.h \
			common/network_msgtype.h \
			common/packing.h \
			common/response_returncode.h \
			common/special_objects.h \
			daemon/connection.h \
			daemon/daemon.h \
			daemon/request_response.h

bin_PROGRAMS = wtf

wtfexecdir = $(exec_prefix)/libexec/$(PACKAGE)-$(VERSION)
wtfexec_PROGRAMS = \
			wtf-daemon

wtf_SOURCES = wtf.cc
wtf_CPPFLAGS = -DWTF_EXEC_DIR="\"$(wtfexecdir)\""

wtf_daemon_SOURCES = \
            common/chain_node.cc \
			common/mapper.cc \
			common/network_msgtype.cc \
			common/packing.cc \
			common/response_returncode.cc \
			daemon/connection.cc \
			daemon/daemon.cc \
			daemon/main.cc
wtf_daemon_LDADD = $(E_LIBS) $(BUSYBEE_LIBS) -lglog -lleveldb -ldl -lpthread

libwtf_la_CXXFLAGS = $(CXXFLAGS) $(AM_CXXFLAGS)
libwtf_la_SOURCES = \
			common/chain_node.cc \
			common/configuration.cc \
			common/mapper.cc \
			common/network_msgtype.cc \
			common/packing.cc \
			common/response_returncode.cc \
			client/command.cc \
			client/wtf.cc
libwtf_la_LIBADD = $(E_LIBS) $(BUSYBEE_LIBS)


pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = wtf.pc
