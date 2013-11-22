#!/usr/bin/sh

mkdir -p fusemnt2
fusermount -u fusemnt2
./wtf-fusetest fusemnt2
echo 'ls -lh fusemnt2'
ls -lh fusemnt2
