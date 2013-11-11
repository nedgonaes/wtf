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
AM_LDFLAGS = $(PO6_LDFLAGS) $(E_LDFLAGS) $(BUSYBEE_LDFLAGS) $(REPLICANT_FLAGS)

lib_LTLIBRARIES =
lib_LTLIBRARIES += libwtfblockstore.la
lib_LTLIBRARIES += libwtf.la
lib_LTLIBRARIES += libwtfcoordinator.la

include_HEADERS = client/wtf.h

noinst_HEADERS = 
noinst_HEADERS += common/block.h
noinst_HEADERS += client/command.h
noinst_HEADERS += client/coordinator_link.h
noinst_HEADERS += client/file.h
noinst_HEADERS += common/block_id.h
noinst_HEADERS += common/wtf_node.h
noinst_HEADERS += common/configuration.h
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
noinst_HEADERS += daemon/block_storage_manager.h
noinst_HEADERS += daemon/connection.h
noinst_HEADERS += daemon/coordinator_link.h
noinst_HEADERS += daemon/daemon.h
noinst_HEADERS += blockstore/blockmap.h
noinst_HEADERS += blockstore/disk.h

bin_PROGRAMS =
bin_PROGRAMS += wtf

noinst_PROGRAMS =
noinst_PROGRAMS += wtf-cli
noinst_PROGRAMS += wtf-writetest 
noinst_PROGRAMS += wtf-readtest
noinst_PROGRAMS += wtf-sync-benchmark
noinst_PROGRAMS += wtf-async-benchmark
noinst_PROGRAMS += wtf-test-blockstore
if ENABLE_HDFS_BENCHMARKS
noinst_PROGRAMS += wtf-hadoop-benchmark
endif

wtfexec_PROGRAMS =
wtfexec_PROGRAMS += wtf-coordinator
wtfexec_PROGRAMS += wtf-daemon 
wtfexec_PROGRAMS += wtf-initialize-cluster

################################################################################
################################## Blockstore ##################################
################################################################################
libwtfblockstore_la_SOURCES = 
libwtfblockstore_la_SOURCES += blockstore/disk.cc
libwtfblockstore_la_SOURCES += blockstore/blockmap.cc

libwtfblockstore_la_LIBADD = $(E_LIBS) $(HYPERLEVELDB_LIBS) -lglog -ldl

################################################################################
################################## Daemon ######################################
################################################################################

wtf_daemon_SOURCES =
wtf_daemon_SOURCES += common/wtf_node.cc
wtf_daemon_SOURCES += common/configuration.cc
wtf_daemon_SOURCES += common/mapper.cc
wtf_daemon_SOURCES += common/network_msgtype.cc
wtf_daemon_SOURCES += common/packing.cc
wtf_daemon_SOURCES += common/response_returncode.cc
wtf_daemon_SOURCES += daemon/block_storage_manager.cc
wtf_daemon_SOURCES += daemon/connection.cc
wtf_daemon_SOURCES += daemon/coordinator_link.cc
wtf_daemon_SOURCES += daemon/daemon.cc
wtf_daemon_SOURCES += daemon/main.cc

wtf_daemon_LDADD = $(E_LIBS) $(BUSYBEE_LIBS) $(REPLICANT_LIBS) \
                   $(HYPERLEVELDB_LIBS) libwtfblockstore.la -lglog -ldl -lpthread

################################################################################
################################## Coordinator #################################
################################################################################

libwtfcoordinator_la_SOURCES =
libwtfcoordinator_la_SOURCES += common/serialization.cc
libwtfcoordinator_la_SOURCES += coordinator/coordinator.cc
libwtfcoordinator_la_SOURCES += coordinator/symtable.c

libwtfcoordinator_la_LIBADD = $(E_LIBS) $(REPLICANT_LIBS)

libwtfcoordinator_la_CPPFLAGS = $(REPLICANT_CFLAGS)

################################################################################
################################## Client ######################################
################################################################################

libwtf_la_CXXFLAGS = $(CXXFLAGS) $(AM_CXXFLAGS)

libwtf_la_SOURCES =
libwtf_la_SOURCES += common/wtf_node.cc
libwtf_la_SOURCES += common/block_id.cc
libwtf_la_SOURCES += common/configuration.cc
libwtf_la_SOURCES += common/mapper.cc
libwtf_la_SOURCES += common/network_msgtype.cc
libwtf_la_SOURCES += common/packing.cc
libwtf_la_SOURCES += common/response_returncode.cc
libwtf_la_SOURCES += common/block.cc
libwtf_la_SOURCES += client/command.cc
libwtf_la_SOURCES += client/coordinator_link.cc
libwtf_la_SOURCES += client/file.cc
libwtf_la_SOURCES += client/wtf.cc

libwtf_la_LIBADD = $(E_LIBS) $(BUSYBEE_LIBS) $(REPLICANT_LIBS) $(HYPERCLIENT_LIBS) -lglog

wtfexecdir = $(exec_prefix)/libexec/$(PACKAGE)-$(VERSION)

wtf_SOURCES = wtf.cc
wtf_CPPFLAGS = -DWTF_EXEC_DIR="\"$(wtfexecdir)\""

wtf_cli_SOURCES = tools/cli.cc
wtf_cli_LDADD = libwtf.la $(REPLICANT_LIBS) $(HYPERCLIENT_LIBS)

wtf_writetest_SOURCES = tools/writetest.cc
wtf_writetest_LDADD = libwtf.la $(REPLICANT_LIBS) $(HYPERCLIENT_LIBS)

wtf_readtest_SOURCES = tools/readtest.cc
wtf_readtest_LDADD = libwtf.la $(REPLICANT_LIBS) $(HYPERCLIENT_LIBS)

wtf_coordinator_SOURCES = tools/coordinator.cc
wtf_coordinator_LDADD = libwtf.la $(E_LIBS) $(BUSYBEE_LIBS) -lpopt

if ENABLE_HDFS_BENCHMARKS
wtf_hadoop_benchmark_SOURCES = tools/hadoop-benchmark.cc
wtf_hadoop_benchmark_CPPFLAGS = -I${HADOOP_HOME}/src/c++/libhdfs -I${JAVA_HOME}/include -I${abs_top_srcdir}/client $(CPPFLAGS)
wtf_hadoop_benchmark_LDADD =  -lpopt -lnumbers -larmnod -L${HADOOP_HOME}/build/c++/Linux-amd64-64/lib -L${JAVA_HOME}/jre/lib/amd64/server -lhdfs -ljvm
endif

wtf_sync_benchmark_SOURCES = tools/sync-benchmark.cc
wtf_sync_benchmark_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
wtf_sync_benchmark_LDADD = libwtf.la $(E_LIBS) $(HYPERCLIENT_LIBS) -lpopt -lnumbers -larmnod

wtf_async_benchmark_SOURCES = tools/async-benchmark.cc
wtf_async_benchmark_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
wtf_async_benchmark_LDADD = libwtf.la $(E_LIBS) $(HYPERCLIENT_LIBS) -lpopt -lnumbers -larmnod

wtf_initialize_cluster_SOURCES = tools/initialize-cluster.cc
wtf_initialize_cluster_CPPFLAGS = -DWTF_COORD_LIB=\""$(libdir)/libwtfcoordinator\"" $(CPPFLAGS)
wtf_initialize_cluster_LDADD = libwtf.la -lpopt

wtf_test_blockstore_SOURCES = tests/test_blockstore.cc
wtf_test_blockstore_LDADD = libwtfblockstore.la -lpopt


pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = wtf.pc
