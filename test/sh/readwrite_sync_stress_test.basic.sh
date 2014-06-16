#!/bin/sh
exec python "${WTF_SRCDIR}"/test/runner.py --wtf-daemons=2 --hyperdex-daemons=1 -- \
     libtool --mode=execute gdb --args "${WTF_BUILDDIR}"/test/readwrite-sync-stress-test -h {WTF_HOST} -p {WTF_PORT} \
      -H {HYPERDEX_HOST} -P {HYPERDEX_PORT} --file-length=64 --file-charset='hex' \
      --value-length=1000 -n 1 -b 10 --min-read-length=1 --max-read-length=30 \
      --min-write-length=1 --max-write-length=30
