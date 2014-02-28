export WTF_SRCDIR="$1"
export WTF_BUILDDIR="$2"
export WTF_VERSION="$3"

export CLASSPATH="${WTF_BUILDDIR}"/bindings/java/org.wtf.client-${WTF_VERSION}.jar:"${WTF_BUILDDIR}"/test/java:
