# Copyright (c) 2013, Cornell University
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
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE # FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement


import collections
import glob
import os
import os.path
import random
import shutil
import subprocess
import sys
import tempfile
import time

import argparse


DOTDOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
BUILDDIR = os.getenv('WTF_BUILDDIR') or DOTDOT

sys.path.append(os.path.join(BUILDDIR, './bindings/python'))
sys.path.append(os.path.join(BUILDDIR, './bindings/python/.libs'))


import hyperdex.admin


Coordinator = collections.namedtuple('Coordinator', ('host', 'port'))
Daemon = collections.namedtuple('Daemon', ('host', 'port'))


class WTFCluster(object):

    def __init__(self, wtf_coordinators, wtf_daemons, hyperdex_coordinators, hyperdex_daemons, clean=False, base=None):
        self.processes = []
        self.wtf_coordinators = wtf_coordinators
        self.wtf_daemons = wtf_daemons
        self.hyperdex_coordinators = hyperdex_coordinators
        self.hyperdex_daemons = hyperdex_daemons
        self.clean = clean
        self.base = base
        self.log_output = True 

    def setup(self):
        if self.base is None:
            self.base = tempfile.mkdtemp(prefix='wtf-test-')
        env = {'GLOG_logtostderr': '',
               'GLOG_minloglevel': '0',
               'PATH': ((os.getenv('PATH') or '') + ':' + BUILDDIR).strip(':')}
        env['CLASSPATH'] = ((os.getenv('CLASSPATH') or '') + BUILDDIR + '/*').strip(':')
        env['GLOG_logbufsecs'] = '0'
        if 'WTF_BUILDDIR' in os.environ:
            env['WTF_EXEC_PATH'] = BUILDDIR
            env['WTF_COORD_LIB'] = os.path.join(BUILDDIR, '.libs/libwtf-coordinator')

        #HYPERDEX
        for i in range(self.hyperdex_coordinators):
            cmd = ['hyperdex', 'coordinator',
                   '--foreground', '--listen', '127.0.0.1', '--listen-port', str(1982 + i)]
            if i > 0:
                cmd += ['--connect', '127.0.0.1', '--connect-port', '1982']
            cwd = os.path.join(self.base, 'hyperdex-coord%i' % i)
            if os.path.exists(cwd):
                raise RuntimeError('environment already exists (at least partially)')
            os.makedirs(cwd)
            stdout = open(os.path.join(cwd, 'wtf-test-runner.log'), 'w')
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT, env=env, cwd=cwd)
            self.processes.append(proc)
        time.sleep(1) # XXX use a barrier tool on cluster
        for i in range(self.hyperdex_daemons):
            cmd = ['hyperdex', 'daemon', '-t', '1',
                   '--foreground', '--listen', '127.0.0.1', '--listen-port', str(2012 + i),
                   '--coordinator', '127.0.0.1', '--coordinator-port', '1982']
            cwd = os.path.join(self.base, 'hyperdex-daemon%i' % i)
            if os.path.exists(cwd):
                raise RuntimeError('environment already exists (at least partially)')
            os.makedirs(cwd)
            stdout = open(os.path.join(cwd, 'wtf-test-runner.log'), 'w')
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT, env=env, cwd=cwd)
            self.processes.append(proc)
        time.sleep(1) # XXX use a barrier tool on cluster

        #WTF
        for i in range(self.wtf_coordinators):
            cmd = ['wtf', 'coordinator',
                   '--foreground', '--listen', '127.0.0.1', '--listen-port', str(2982 + i)]
            if i > 0:
                cmd += ['--connect', '127.0.0.1', '--connect-port', '2982']
            cwd = os.path.join(self.base, 'wtf-coord%i' % i)
            if os.path.exists(cwd):
                raise RuntimeError('environment already exists (at least partially)')
            os.makedirs(cwd)
            stdout = open(os.path.join(cwd, 'wtf-test-runner.log'), 'w')
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT, env=env, cwd=cwd)
            self.processes.append(proc)
        time.sleep(1) # XXX use a barrier tool on cluster
        for i in range(self.wtf_daemons):
            cmd = ['wtf', 'daemon', '-t', '1',
                   '--foreground', '--listen', '127.0.0.1', '--listen-port', str(3012 + i),
                   '--coordinator', '127.0.0.1', '--coordinator-port', '2982']
            cwd = os.path.join(self.base, 'wtf-daemon%i' % i)
            if os.path.exists(cwd):
                raise RuntimeError('environment already exists (at least partially)')
            os.makedirs(cwd)
            stdout = open(os.path.join(cwd, 'wtf-test-runner.log'), 'w')
            proc = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT, env=env, cwd=cwd)
            self.processes.append(proc)
        time.sleep(1) # XXX use a barrier tool on cluster

    def cleanup(self):
        for i in range(self.hyperdex_coordinators):
            core = os.path.join(self.base, 'hyperdex-coord%i' % i, 'core*')
            if glob.glob(core):
                print('hyperdex-coordinator', i, 'dumped core')
                self.clean = False
        for i in range(self.hyperdex_daemons):
            core = os.path.join(self.base, 'hyperdex-daemon%i' % i, 'core*')
            if glob.glob(core):
                print('hyperdex-daemon', i, 'dumped core')
                self.clean = False
        for i in range(self.wtf_coordinators):
            core = os.path.join(self.base, 'wtf-coord%i' % i, 'core*')
            if glob.glob(core):
                print('wtf-coordinator', i, 'dumped core')
                self.clean = False
        for i in range(self.wtf_daemons):
            core = os.path.join(self.base, 'wtf-daemon%i' % i, 'core*')
            if glob.glob(core):
                print('wtf-daemon', i, 'dumped core')
        for p in self.processes:
            p.kill()
            p.wait()
        if self.log_output:
            for i in range(self.hyperdex_coordinators):
                log = os.path.join(self.base, 'hyperdex-coord%i' % i, 'wtf-test-runner.log')
                print('hyperdex-coordinator', i)
                print(open(log).read())
                print
            for i in range(self.hyperdex_daemons):
                log = os.path.join(self.base, 'hyperdex-daemon%i' % i, 'wtf-test-runner.log')
                print('hyperdex-daemon', i)
                print(open(log).read())
            for i in range(self.wtf_coordinators):
                log = os.path.join(self.base, 'wtf-coord%i' % i, 'wtf-test-runner.log')
                print('wtf-coordinator', i)
                print(open(log).read())
                print
            for i in range(self.wtf_daemons):
                log = os.path.join(self.base, 'wtf-daemon%i' % i, 'wtf-test-runner.log')
                print('wtf-daemon', i)
                print(open(log).read())
                print
        if self.clean and self.base is not None:
            shutil.rmtree(self.base)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--wtf-coordinators', default=1, type=int)
    parser.add_argument('--wtf-daemons', default=1, type=int)
    parser.add_argument('--hyperdex-coordinators', default=1, type=int)
    parser.add_argument('--hyperdex-daemons', default=1, type=int)
    parser.add_argument('args', nargs='*')
    args = parser.parse_args(argv)
    hdc = WTFCluster(args.wtf_coordinators, args.wtf_daemons, args.hyperdex_coordinators, args.hyperdex_daemons, True)
    try:
        hdc.setup()
        adm = hyperdex.admin.Admin('127.0.0.1', 1982)
        space = str("space wtf key path attributes string blockmap, int directory, int mode, string owner, string group")
        time.sleep(1) # XXX use a barrier tool on cluster
        adm.add_space(space)
        time.sleep(1) # XXX use a barrier tool on cluster
        ctx = {'WTF_HOST': '127.0.0.1', 'WTF_PORT': 2982,
                'HYPERDEX_HOST': '127.0.0.1', 'HYPERDEX_PORT': 1982}
        cmd_args = [arg.format(**ctx) for arg in args.args]
        print(cmd_args)
        status = subprocess.call(cmd_args)
        if status != 0:
            hdc.log_output = True
        return status
    finally:
        hdc.cleanup()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
