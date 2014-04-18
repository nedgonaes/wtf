#!/bin/sh
javac -d "${WTF_BUILDDIR}"/test/java "${WTF_SRCDIR}"/test/java/Basic.java

javac -version
java -version
exec python "${WTF_SRCDIR}"/test/runner.py --wtf-daemons=2 --hyperdex-daemons=1 -- \
    java -ea -Djava.library.path="${WTF_BUILDDIR}"/.libs:/usr/local/lib:/usr/local/lib64:/usr/lib:/usr/lib64 Basic {WTF_HOST} {WTF_PORT} \
    {HYPERDEX_HOST} {HYPERDEX_PORT}
