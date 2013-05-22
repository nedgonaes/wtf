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

lib_LTLIBRARIES =
lib_LTLIBRARIES += libwtf.la
lib_LTLIBRARIES += libwtfcoordinator.la

include_HEADERS = client/wtf.h

noinst_HEADERS = 
noinst_HEADERS += client/command.h
noinst_HEADERS += common/chain_node.h
noinst_HEADERS += common/coordinator_returncode.h
noinst_HEADERS += common/macros.h
noinst_HEADERS += common/mapper.h
noinst_HEADERS += common/network_msgtype.h
noinst_HEADERS += common/packing.h
noinst_HEADERS += common/response_returncode.h
noinst_HEADERS += common/special_objects.h
noinst_HEADERS += common/ids.h
noinst_HEADERS += coordinator/coordinator.h
noinst_HEADERS += coordinator/missing_acks.h
noinst_HEADERS += coordinator/server_state.h
noinst_HEADERS += daemon/connection.h
noinst_HEADERS += daemon/daemon.h
noinst_HEADERS += daemon/request_response.h

bin_PROGRAMS = wtf

noinst_PROGRAMS = wtf-cli

wtfexecdir = $(exec_prefix)/libexec/$(PACKAGE)-$(VERSION)

wtfexec_PROGRAMS =
wtfexec_PROGRAMS += wtf-daemon 
wtfexec_PROGRAMS += wtf-coordinator

wtf_SOURCES = wtf.cc
wtf_CPPFLAGS = -DWTF_EXEC_DIR="\"$(wtfexecdir)\""

################################################################################
################################## Daemon ######################################
################################################################################

wtf_daemon_SOURCES =
wtf_daemon_SOURCES += common/chain_node.cc
wtf_daemon_SOURCES += common/mapper.cc
wtf_daemon_SOURCES += common/network_msgtype.cc
wtf_daemon_SOURCES += common/packing.cc
wtf_daemon_SOURCES += common/response_returncode.cc
wtf_daemon_SOURCES += daemon/connection.cc
wtf_daemon_SOURCES += daemon/daemon.cc
wtf_daemon_SOURCES += daemon/main.cc

wtf_daemon_LDADD = $(E_LIBS) $(BUSYBEE_LIBS) -lglog -lleveldb -ldl -lpthread

################################################################################
################################## Coordinator #################################
################################################################################

libwtfcoordinator_la_SOURCES =
libwtfcoordinator_la_SOURCES += coordinator/coordinator.cc
libwtfcoordinator_la_SOURCES += coordinator/symtable.c

libwtfcoordinator_la_LIBADD = $(E_LIBS)

libwtfcoordinator_la_CPPFLAGS = $(REPLICANT_CFLAGS)

################################################################################
################################## Client ######################################
################################################################################

libwtf_la_CXXFLAGS = $(CXXFLAGS) $(AM_CXXFLAGS)

libwtf_la_SOURCES =
libwtf_la_SOURCES += common/chain_node.cc
libwtf_la_SOURCES += common/configuration.cc
libwtf_la_SOURCES += common/mapper.cc
libwtf_la_SOURCES += common/network_msgtype.cc
libwtf_la_SOURCES += common/packing.cc
libwtf_la_SOURCES += common/response_returncode.cc
libwtf_la_SOURCES += client/command.cc
libwtf_la_SOURCES += client/wtf.cc

libwtf_la_LIBADD = $(E_LIBS) $(BUSYBEE_LIBS)

wtf_cli_SOURCES = tools/cli.cc
wtf_cli_LDADD = libwtf.la

wtf_coordinator_SOURCES = tools/coordinator.cc
wtf_coordinator_LDADD = libwtf.la $(E_LIBS) $(BUSYBEE_LIBS) -lpopt



pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = wtf.pc
