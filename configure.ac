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
#
#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([wtf], [0.1.dev], [sean@hyperdex.org])
AM_INIT_AUTOMAKE([foreign subdir-objects dist-bzip2])
AM_SILENT_RULES([yes])
LT_PREREQ([2.2])
LT_INIT
AC_CONFIG_SRCDIR([daemon/daemon.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_LANG(C++)

ANAL_WARNINGS


AC_ARG_ENABLE([java-bindings], [AS_HELP_STRING([--enable-java-bindings],
              [enable the Java bindings @<:@default: yes@:>@])],
              [java_bindings=${enableval}], [java_bindings=no])
AM_CONDITIONAL([ENABLE_JAVA_BINDINGS], [test x"${java_bindings}" = xyes])
need_swig=yes

JNI_CPPFLAGS=
if test x"${java_bindings}" = xyes; then
    AC_PROG_JAR
    AC_PROG_JAVAC
    AC_JNI_INCLUDE_DIR
    found_jnidir=no
    for jnidir in ${JNI_INCLUDE_DIRS}
    do
        if test x"${found_jnidir}" = xno; then
            JNI_CPPFLAGS="-I${jnidir} -I${jnidir}/linux"
            found_jnidir=yes
        fi
    done
    if test x"${found_jnidir}" = xno; then
        AC_MSG_ERROR([
-------------------------------------------------
HyperDex relies upon the JNI for Java bindings.
No JNI include dir could be found.
Set JAVAC to point to your Java compiler.
-------------------------------------------------])
    fi
fi
AC_SUBST([JNI_CPPFLAGS], [${JNI_CPPFLAGS}])

if test x"${need_swig}" = xyes; then
    AX_PKG_SWIG(1.3)
    SWIG_ENABLE_CXX
fi

# Checks for libraries.
AC_CHECK_LIB([popt], [poptGetContext], [], [AC_MSG_ERROR([
---------------------------------------
WTF relies upon the popt library.
Please install popt to continue.
---------------------------------------])])
AC_CHECK_LIB([rt], [clock_gettime], [needs_lrt=yes], [needs_lrt=no])

if test x"${needs_lrt}" = xyes; then
    AC_SUBST([LRT_CFLAGS], ["-lrt"])
else
    AC_SUBST([LRT_CFLAGS], [""])
fi

PKG_CHECK_MODULES([PO6], [libpo6 >= 0.3.1])
PKG_CHECK_MODULES([E], [libe >= 0.3.2])
PKG_CHECK_MODULES([BUSYBEE], [busybee >= 0.3.0])
PKG_CHECK_MODULES([REPLICANT], [replicant >= 0.2.dev])
PKG_CHECK_MODULES([HYPERLEVELDB], [libhyperleveldb])
PKG_CHECK_MODULES([HYPERCLIENT], [hyperdex-client >= 1.0.dev])
PKG_CHECK_MODULES([GLOG], [libglog >= 0.3.3])

# Checks for header files.
AC_CHECK_HEADER([popt.h],,[AC_MSG_ERROR([
---------------------------------------
WTF relies upon the popt library.
Please install popt to continue.
---------------------------------------])])

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_ARG_ENABLE([log-commands], [AS_HELP_STRING([--enable-log-commands],
              [enable code to log all commands @<:@default: no@:>@])],
              [enable_log_commands=${enableval}], [enable_log_commands=no])
if test x"${enable_log_commands}" = xyes; then
    AC_DEFINE([WTF_LOG_COMMANDS], [], [Log all commands that are submitted, issued and acknowledged])
fi

AC_ARG_ENABLE([hdfs-benchmarks], [AS_HELP_STRING([--enable-hdfs-benchmarks],
              [build HDFS benchmarks @<:@default: no@:>@])],
              [enable_hdfs_benchmarks=${enableval}], [enable_hdfs_benchmarks=no])
AM_CONDITIONAL([ENABLE_HDFS_BENCHMARKS], [test x"${enable_hdfs_benchmarks}" = xyes])

AC_ARG_ENABLE([debug], [AS_HELP_STRING([--enable-debug],
              [compile with -ggdb -O0 @<:@default: no@:>@])],
              [enable_debug=${enableval}], [enable_debug=no])
AM_CONDITIONAL([ENABLE_DEBUG], [test x"${enable_debug}" = xyes])

AC_ARG_ENABLE([gtest], [AS_HELP_STRING([--enable-gtest],
              [enable gtest @<:@default: no@:>@])],
              [enable_gtest=${enableval}], [enable_gtest=no])
AM_CONDITIONAL([ENABLE_GTEST], [test x"${enable_gtest}" = xyes])

AC_CHECK_FUNCS([clock_gettime mach_absolute_time])

AC_CONFIG_FILES([Makefile
                 wtf-client.pc wtf-admin.pc])
AC_CONFIG_LINKS([test/java/.exists:test/java/.exists])
AC_CONFIG_LINKS([bindings/java/.exists:bindings/java/.exists])
AC_OUTPUT
