#!/bin/sh
# dagron-gitops, as a uid with a name.
#
# OpenShift's restricted SCCs run a pod as a uid from its namespace's range,
# which no passwd of this image names, in group 0. git execs ssh for an
# ssh:// repository, and OpenSSH refuses a uid it cannot look up ("No user
# exists for uid"). So such a uid is given an entry through nss_wrapper, in a
# passwd written under $TMPDIR (/tmp: the root filesystem is read-only), with
# that directory as its home. A uid the image names is left as it is.
set -eu
if ! getent passwd "$(id -u)" >/dev/null 2>&1; then
  wrapper=$(ls /usr/lib/libnss_wrapper.so* 2>/dev/null | head -n 1)
  if [ -n "$wrapper" ]; then
    home="${TMPDIR:-/tmp}"
    passwd="$home/.dagron-gitops-passwd"
    { cat /etc/passwd; echo "gitops:x:$(id -u):$(id -g):gitops:$home:/sbin/nologin"; } > "$passwd"
    export LD_PRELOAD="$wrapper" NSS_WRAPPER_PASSWD="$passwd" NSS_WRAPPER_GROUP=/etc/group HOME="$home"
  fi
fi
exec /usr/local/bin/dagron-gitops "$@"
