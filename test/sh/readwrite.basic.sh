#!/bin/sh
exec python "${WTF_SRCDIR}"/test/runner.py --wtf-daemons=1 --hyperdex-daemons=1 -- \
     "${WTF_BUILDDIR}"/test/readwrite-stress-test -h {WTF_HOST} -p {WTF_PORT} \
      -H {HYPERDEX_HOST} -P {HYPERDEX_PORT} --file-length=64 --file-charset='hex' \
      --value-length=15 -n 1 -O 5 -b 10
