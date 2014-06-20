#!/bin/sh
exec python "${WTF_SRCDIR}"/test/runner.py --wtf-daemons=2 --hyperdex-daemons=1 -- \
     "${WTF_BUILDDIR}"/test/readwrite-sync-stress-test -h {WTF_HOST} -p {WTF_PORT} \
      -H {HYPERDEX_HOST} -P {HYPERDEX_PORT} --file-length=64 --file-charset='hex' \
      --value-length=10 -n 1 -b 10 --min-read-length=10 --max-read-length=10 \
      --min-write-length=10 --max-write-length=10
     #libtool --mode=execute gdbtui --args \
