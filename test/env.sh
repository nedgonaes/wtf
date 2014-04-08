export WTF_SRCDIR="$1"
export WTF_BUILDDIR="$2"
export WTF_VERSION="$3"

export CLASSPATH="${WTF_SRCDIR}"/bindings/java/org.wtf.client-${WTF_VERSION}.jar:"${WTF_BUILDDIR}"/test/java:
