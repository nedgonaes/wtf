#!/bin/sh
exec python "${WTF_SRCDIR}"/test/runner.py --wtf-daemons=2 --hyperdex-daemons=1 -- \
     "${WTF_BUILDDIR}"/test/getattr-test -h {WTF_HOST} -p {WTF_PORT} \
      -H {HYPERDEX_HOST} -P {HYPERDEX_PORT} --file-length=64 --file-charset='hex' \
      -n 1
     #libtool --mode=execute gdbtui --args \
