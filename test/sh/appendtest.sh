#!/bin/sh
exec python "${WTF_SRCDIR}"/test/runner.py --wtf-daemons=2 --hyperdex-daemons=1 -- \
     "${WTF_BUILDDIR}"/test/appendtest -h {WTF_HOST} -p {WTF_PORT} \
      -H {HYPERDEX_HOST} -P {HYPERDEX_PORT} --file-length=64 --file-charset='hex' \
      --value-length=10000000 -n 10 -b 100000 --min-read-length=1 --max-read-length=10000000 \
      --min-write-length=1 --max-write-length=10000000
     #libtool --mode=execute gdbtui --args \
