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

AM_CPPFLAGS  = -I${abs_top_srcdir}/include $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(HYPERLEVELDB_CFLAGS) $(REPLICANT_CFLAGS) $(SPARSEHASH_CFLAGS)
AM_CFLAGS = $(WANAL_CFLAGS)
AM_CXXFLAGS = $(WANAL_CXXFLAGS) $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(LRT_CFLAGS)
AM_LDFLAGS = $(PO6_LDFLAGS) $(E_LDFLAGS) $(BUSYBEE_LDFLAGS) $(REPLICANT_FLAGS)

TESTS_ENVIRONMENT = . $(abs_top_srcdir)/test/env.sh "${abs_top_srcdir}" "${abs_top_builddir}" "${VERSION}";
#"${abs_top_srcdir}" "${abs_top_builddir}" "${VERSION}";
wtfexecdir = $(exec_prefix)/libexec/$(PACKAGE)-$(VERSION)
wtfheaderdir = $(includedir)/wtf
pkgconfigdir = $(libdir)/pkgconfig

if ENABLE_DEBUG
    AM_CXXFLAGS += -ggdb -O0
endif

lib_LTLIBRARIES =
wtfexec_LTLIBRARIES =

wtfheader_HEADERS =
noinst_HEADERS = 

bin_PROGRAMS =
wtfexec_PROGRAMS =
noinst_PROGRAMS =
check_PROGRAMS =

pkgconfig_DATA =

jardir = $(datadir)/java
jar_DATA =

EXTRA_DIST =
CLEANFILES =
TESTS = 

noinst_HEADERS += daemon/settings.h
noinst_HEADERS += tools/common.h
noinst_HEADERS += client/constants.h
noinst_HEADERS += common/configuration_flags.h
noinst_HEADERS += client/client.h
noinst_HEADERS += common/block.h
noinst_HEADERS += blockstore/vblock.h
noinst_HEADERS += common/coordinator_link.h
noinst_HEADERS += common/block_location.h
noinst_HEADERS += common/server.h
noinst_HEADERS += common/serialization.h
noinst_HEADERS += common/configuration.h
noinst_HEADERS += common/coordinator_returncode.h
noinst_HEADERS += common/coordinator_link.h
noinst_HEADERS += common/macros.h
noinst_HEADERS += common/mapper.h
noinst_HEADERS += common/network_msgtype.h
noinst_HEADERS += common/packing.h
noinst_HEADERS += common/response_returncode.h
noinst_HEADERS += common/special_objects.h
noinst_HEADERS += common/ids.h
noinst_HEADERS += coordinator/coordinator.h
noinst_HEADERS += coordinator/server_state.h
noinst_HEADERS += daemon/block_storage_manager.h
noinst_HEADERS += daemon/connection.h
noinst_HEADERS += daemon/coordinator_link_wrapper.h
noinst_HEADERS += daemon/daemon.h
noinst_HEADERS += blockstore/blockmap.h
noinst_HEADERS += blockstore/disk.h
noinst_HEADERS += visibility.h

noinst_PROGRAMS += wtf-stat

bin_PROGRAMS += wtf

wtfexec_PROGRAMS += wtf-coordinator
wtfexec_PROGRAMS += wtf-daemon 
wtfexec_PROGRAMS += wtf-mkfs
wtfexec_PROGRAMS += wtf-server-register
wtfexec_PROGRAMS += wtf-server-online
wtfexec_PROGRAMS += wtf-server-offline
wtfexec_PROGRAMS += wtf-server-kill
wtfexec_PROGRAMS += wtf-server-forget
wtfexec_PROGRAMS += wtf-coordinator


################################################################################
################################## Blockstore ##################################
################################################################################

lib_LTLIBRARIES += libwtfblockstore.la

libwtfblockstore_la_SOURCES = 
libwtfblockstore_la_SOURCES += blockstore/vblock.cc
libwtfblockstore_la_SOURCES += blockstore/disk.cc
libwtfblockstore_la_SOURCES += blockstore/blockmap.cc

libwtfblockstore_la_LIBADD = $(E_LIBS) $(HYPERLEVELDB_LIBS) -lglog -ldl

################################################################################
################################## Daemon ######################################
################################################################################

wtf_daemon_SOURCES =
wtf_daemon_SOURCES += common/server.cc
wtf_daemon_SOURCES += common/ids.cc
wtf_daemon_SOURCES += common/block_location.cc
wtf_daemon_SOURCES += common/configuration.cc
wtf_daemon_SOURCES += common/coordinator_link.cc
wtf_daemon_SOURCES += common/coordinator_returncode.cc
wtf_daemon_SOURCES += common/mapper.cc
wtf_daemon_SOURCES += common/network_msgtype.cc
wtf_daemon_SOURCES += common/packing.cc
wtf_daemon_SOURCES += common/response_returncode.cc
wtf_daemon_SOURCES += daemon/block_storage_manager.cc
wtf_daemon_SOURCES += daemon/connection.cc
wtf_daemon_SOURCES += daemon/coordinator_link_wrapper.cc
wtf_daemon_SOURCES += daemon/daemon.cc
wtf_daemon_SOURCES += daemon/main.cc

wtf_daemon_CXXFLAGS = $(CXXFLAGS) $(AM_CXXFLAGS)
wtf_daemon_LDADD = $(E_LIBS) $(BUSYBEE_LIBS) $(REPLICANT_LIBS) \
                   $(HYPERLEVELDB_LIBS) libwtfblockstore.la -lglog -ldl -lpthread

################################################################################
################################## Coordinator #################################
################################################################################

wtfexec_LTLIBRARIES += libwtf-coordinator.la

noinst_HEADERS += coordinator/coordinator.h
noinst_HEADERS += coordinator/server_barrier.h
noinst_HEADERS += coordinator/transitions.h
noinst_HEADERS += coordinator/util.h

libwtf_coordinator_la_SOURCES =
libwtf_coordinator_la_SOURCES += common/ids.cc
libwtf_coordinator_la_SOURCES += common/serialization.cc
libwtf_coordinator_la_SOURCES += common/server.cc
libwtf_coordinator_la_SOURCES += coordinator/server_barrier.cc
libwtf_coordinator_la_SOURCES += coordinator/coordinator.cc
libwtf_coordinator_la_SOURCES += coordinator/symtable.c
libwtf_coordinator_la_SOURCES += coordinator/transitions.cc

wtf_coordinator_SOURCES = tools/coordinator.cc
wtf_coordinator_CPPFLAGS = -DWTF_EXEC_DIR=\""$(wtfexecdir)\"" $(AM_CPPFLAGS) $(CPPFLAGS)
wtf_coordinator_LDADD =
wtf_coordinator_LDADD += $(E_LIBS)
wtf_coordinator_LDADD += $(BUSYBEE_LIBS)
wtf_coordinator_LDADD += -lpopt

################################################################################
################################## Client ######################################
################################################################################


lib_LTLIBRARIES += libwtf-client.la

wtfheader_HEADERS += include/wtf/client.h
wtfheader_HEADERS += include/wtf/client.hpp

pkgconfig_DATA += wtf-client.pc

noinst_HEADERS += include/wtf/client.h
noinst_HEADERS += include/wtf/client.hpp
noinst_HEADERS += client/file.h
noinst_HEADERS += client/pending.h
noinst_HEADERS += client/pending_aggregation.h
noinst_HEADERS += client/pending_read.h
noinst_HEADERS += client/pending_write.h
#noinst_HEADERS += client/pending_readdir.h

libwtf_client_la_CXXFLAGS = $(CXXFLAGS) $(AM_CXXFLAGS)

libwtf_client_la_SOURCES =
libwtf_client_la_SOURCES += common/server.cc
libwtf_client_la_SOURCES += common/ids.cc
libwtf_client_la_SOURCES += common/block_location.cc
libwtf_client_la_SOURCES += common/configuration.cc
libwtf_client_la_SOURCES += common/mapper.cc
libwtf_client_la_SOURCES += common/network_msgtype.cc
libwtf_client_la_SOURCES += common/packing.cc
libwtf_client_la_SOURCES += common/response_returncode.cc
libwtf_client_la_SOURCES += common/block.cc
libwtf_client_la_SOURCES += client/c.cc
libwtf_client_la_SOURCES += client/pending.cc
libwtf_client_la_SOURCES += client/pending_aggregation.cc
libwtf_client_la_SOURCES += client/pending_read.cc
libwtf_client_la_SOURCES += client/pending_write.cc
#libwtf_client_la_SOURCES += client/pending_readdir.cc
libwtf_client_la_SOURCES += common/coordinator_link.cc
libwtf_client_la_SOURCES += client/file.cc
libwtf_client_la_SOURCES += client/client.cc

libwtf_client_la_LIBADD = 
libwtf_client_la_LIBADD += $(E_LIBS) 
libwtf_client_la_LIBADD += $(BUSYBEE_LIBS) 
libwtf_client_la_LIBADD += $(REPLICANT_LIBS) 
libwtf_client_la_LIBADD += $(HYPERCLIENT_LIBS) -lglog

################################################################################
##################################### Admin ####################################
################################################################################


lib_LTLIBRARIES += libwtf-admin.la

wtfheader_HEADERS += include/wtf/admin.h
wtfheader_HEADERS += include/wtf/admin.hpp

pkgconfig_DATA += wtf-admin.pc

noinst_HEADERS += include/wtf/admin.h
noinst_HEADERS += include/wtf/admin.hpp
noinst_HEADERS += admin/admin.h
noinst_HEADERS += admin/constants.h
noinst_HEADERS += admin/coord_rpc.h
noinst_HEADERS += admin/coord_rpc_generic.h
noinst_HEADERS += admin/multi_yieldable.h
noinst_HEADERS += admin/pending.h
noinst_HEADERS += admin/pending_string.h
noinst_HEADERS += admin/yieldable.h

libwtf_admin_la_CXXFLAGS = $(CXXFLAGS) $(AM_CXXFLAGS)

libwtf_admin_la_SOURCES =
libwtf_admin_la_SOURCES += common/block_location.cc
libwtf_admin_la_SOURCES += common/configuration.cc
libwtf_admin_la_SOURCES += common/coordinator_link.cc
libwtf_admin_la_SOURCES += common/ids.cc
libwtf_admin_la_SOURCES += common/mapper.cc
libwtf_admin_la_SOURCES += common/network_msgtype.cc
libwtf_admin_la_SOURCES += common/serialization.cc
libwtf_admin_la_SOURCES += common/server.cc
libwtf_admin_la_SOURCES += admin/admin.cc
libwtf_admin_la_SOURCES += admin/c.cc
libwtf_admin_la_SOURCES += admin/coord_rpc.cc
libwtf_admin_la_SOURCES += admin/coord_rpc_generic.cc
libwtf_admin_la_SOURCES += admin/multi_yieldable.cc
libwtf_admin_la_SOURCES += admin/pending.cc
libwtf_admin_la_SOURCES += admin/pending_string.cc
libwtf_admin_la_SOURCES += admin/yieldable.cc
libwtf_admin_la_LIBADD =
libwtf_admin_la_LIBADD += $(E_LIBS)
libwtf_admin_la_LIBADD += $(BUSYBEE_LIBS)
libwtf_admin_la_LIBADD += $(REPLICANT_LIBS)

################################################################################
################################## other   #####################################
################################################################################

wtf_SOURCES = wtf.cc
wtf_CPPFLAGS = -DWTF_EXEC_DIR="\"$(wtfexecdir)\""

################################################################################
################################## benchmarks ##################################
################################################################################

################################################################################
################################## tests   #####################################
################################################################################
shell_wrappers =
shell_wrappers += test/sh/readwrite_sync_stress_test.basic.sh

TESTS += $(shell_wrappers)

EXTRA_DIST += $(shell_wrappers)
EXTRA_DIST += test/env.sh
EXTRA_DIST += test/runner.py

#wtf-test_readwrite
check_PROGRAMS += test/readwrite-sync-stress-test
test_readwrite_sync_stress_test_SOURCES = test/readwrite_sync_stress_test.cc 
test_readwrite_sync_stress_test_LDADD = libwtf-client.la $(E_LIBS) -lpopt -larmnod

#if ENABLE_GTEST
#wtf_test_blockstore_SOURCES = test/test_blockstore.cc
#wtf_test_blockstore_LDADD = libwtfblockstore.la -lpopt -lglog -lgtest
#endif


################################################################################
################################## tools   #####################################
################################################################################

# wtf-mkfs
wtf_mkfs_SOURCES = tools/mkfs.cc
wtf_mkfs_CPPFLAGS = $(CPPFLAGS) 
wtf_mkfs_LDADD = $(HYPERCLIENT_LIBS) -lpopt

# wtf-server-register
wtf_server_register_SOURCES = tools/server-register.cc
wtf_server_register_LDADD = libwtf-admin.la -lpopt

# wtf-server-online
wtf_server_online_SOURCES = tools/server-online.cc
wtf_server_online_LDADD = libwtf-admin.la -lpopt

# wtf-server-offline
wtf_server_offline_SOURCES = tools/server-offline.cc
wtf_server_offline_LDADD = libwtf-admin.la -lpopt

# wtf-server-forget
wtf_server_forget_SOURCES = tools/server-forget.cc
wtf_server_forget_LDADD = libwtf-admin.la -lpopt

# wtf-server-kill
wtf_server_kill_SOURCES = tools/server-kill.cc
wtf_server_kill_LDADD = libwtf-admin.la -lpopt

# wtf-stat (metadata dump)
wtf_stat_SOURCES =
wtf_stat_SOURCES += common/block.cc
wtf_stat_SOURCES += common/block_location.cc
wtf_stat_SOURCES += client/file.cc
wtf_stat_SOURCES += tools/stat.cc
wtf_stat_LDADD = libwtf-admin.la $(REPLICANT_LIBS) $(HYPERCLIENT_LIBS)

################################################################################
################################## bindings ####################################
################################################################################
swig_verbose = $(swig_verbose_$(V))
swig_verbose_ = $(swig_verbose_$(AM_DEFAULT_VERBOSITY))
swig_verbose_0 = @echo "  SWIG  " $@;
wtf_client_jarfile = bindings/java/org.wtf.client-$(VERSION).jar

CLEANFILES += $(wtf_client_jarfile)

if ENABLE_JAVA_BINDINGS
lib_LTLIBRARIES += libwtf-client-java.la
jar_DATA += $(wtf_client_jarfile)
endif

nodist_libwtf_client_java_la_SOURCES = bindings/java/wtf_client_wrap.cxx
libwtf_client_java_la_CPPFLAGS = $(JNI_CPPFLAGS) $(AM_CPPFLAGS) $(CPPFLAGS)
libwtf_client_java_la_CXXFLAGS = -fno-strict-aliasing $(AM_CXXFLAGS) -fvisibility=default $(CXXFLAGS)
libwtf_client_java_la_LIBADD = libwtf-client.la
bindings/java/wtf_client_wrap.cxx: bindings/java/wtf_client.i include/wtf/client.h
	$(swig_verbose)mkdir -p $(abs_top_builddir)/bindings/java/org/wtf/client &&  \
	${SWIG} -I${abs_srcdir}/include -java -package org.wtf.client -c++ -outdir $(abs_builddir)/bindings/java/org/wtf/client \
		-o $(abs_builddir)/$@ -w518 $(abs_top_srcdir)/$<

$(wtf_client_jarfile): bindings/java/wtf_client_wrap.cxx
	javac bindings/java/org/wtf/client/*.java
	cd bindings/java ; $(JAR) cvf $(JARFLAGS) ../../$(wtf_client_jarfile) org/wtf/client/*.class
