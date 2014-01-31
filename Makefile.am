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

if ENABLE_DEBUG
    AM_CXXFLAGS += -ggdb -O0
endif

lib_LTLIBRARIES =
wtfheader_HEADERS =
bin_PROGRAMS =
noinst_HEADERS = 
noinst_PROGRAMS =
pkgconfig_DATA =

lib_LTLIBRARIES += libwtfblockstore.la

noinst_HEADERS += common/block.h
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
noinst_HEADERS += tools/fusewtf.h

bin_PROGRAMS += wtf

noinst_PROGRAMS += wtf-writetest 
noinst_PROGRAMS += wtf-readtest
noinst_PROGRAMS += wtf-readwritetest
noinst_PROGRAMS += wtf-sync-benchmark
noinst_PROGRAMS += wtf-async-benchmark
#noinst_PROGRAMS += wtf-test-blockstore
if ENABLE_HDFS_BENCHMARKS
noinst_PROGRAMS += wtf-hadoop-benchmark
endif
noinst_PROGRAMS += wtf-shelltest
noinst_PROGRAMS += wtf-fusetest

wtfexec_PROGRAMS =
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

lib_LTLIBRARIES += libwtf-coordinator.la

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

wtfheaderdir = $(includedir)/wtf

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
################################## fusewtf #####################################
################################################################################

wtfexecdir = $(exec_prefix)/libexec/$(PACKAGE)-$(VERSION)

wtf_SOURCES = wtf.cc
wtf_CPPFLAGS = -DWTF_EXEC_DIR="\"$(wtfexecdir)\""

wtf_writetest_SOURCES = tools/writetest.cc
wtf_writetest_LDADD = libwtf.la $(REPLICANT_LIBS) $(HYPERCLIENT_LIBS)

wtf_shelltest_SOURCES = tools/shelltest.cc
wtf_shelltest_LDADD = libwtf.la $(REPLICANT_LIBS) $(HYPERCLIENT_LIBS)

wtf_fusetest_SOURCES = tools/fusetest.cc
wtf_fusetest_LDADD = -lfuse libwtf.la
wtf_fusetest_CPPFLAGS = -D_FILE_OFFSET_BITS=64

wtf_readtest_SOURCES = tools/readtest.cc
wtf_readtest_LDADD = libwtf.la $(REPLICANT_LIBS) $(HYPERCLIENT_LIBS)

if ENABLE_HDFS_BENCHMARKS
wtf_hadoop_benchmark_SOURCES = tools/hadoop-benchmark.cc
wtf_hadoop_benchmark_CPPFLAGS = -I${HADOOP_HOME}/src/c++/libhdfs -I${JAVA_HOME}/include -I${abs_top_srcdir}/client $(CPPFLAGS)
wtf_hadoop_benchmark_LDADD =  -lpopt -lnumbers -larmnod -L${HADOOP_HOME}/build/c++/Linux-amd64-64/lib -L${JAVA_HOME}/jre/lib/amd64/server -lhdfs -ljvm
endif

wtf_sync_benchmark_SOURCES = tools/sync-benchmark.cc
wtf_sync_benchmark_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
wtf_sync_benchmark_LDADD = libwtf.la $(E_LIBS) $(HYPERCLIENT_LIBS) -lpopt -lnumbers -larmnod

wtf_readwritetest_SOURCES = tools/readwritetest.cc
wtf_readwritetest_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
wtf_readwritetest_LDADD = libwtf.la $(E_LIBS) $(HYPERCLIENT_LIBS) -lpopt -lnumbers -larmnod

wtf_async_benchmark_SOURCES = tools/async-benchmark.cc
wtf_async_benchmark_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
wtf_async_benchmark_LDADD = libwtf.la $(E_LIBS) $(HYPERCLIENT_LIBS) -lpopt -lnumbers -larmnod

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

#if ENABLE_GTEST
#wtf_test_blockstore_SOURCES = tests/test_blockstore.cc
#wtf_test_blockstore_LDADD = libwtfblockstore.la -lpopt -lglog -lgtest
#endif


pkgconfigdir = $(libdir)/pkgconfig
