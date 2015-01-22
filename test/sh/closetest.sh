#!/bin/sh
exec python "${WTF_SRCDIR}"/test/runner.py --wtf-daemons=1 --hyperdex-daemons=1 -- \
     "${WTF_BUILDDIR}"/test/closetest -h {WTF_HOST} -p {WTF_PORT} \
      -H {HYPERDEX_HOST} -P {HYPERDEX_PORT} --file-length=64 --file-charset='hex' \
      --value-length=40 -n 1 -b 10 --min-read-length=20 --max-read-length=20 \
      --min-write-length=20 --max-write-length=20
     #libtool --mode=execute gdbtui --args \
