#!/bin/sh

# . ../../bin/lisaenv

GENDATA="python ../../tools/gen_data.py"

for config in *.conf ; do
    base=`echo ${config} | sed -e 's/\.conf//'`
    filename=${base}.db
    if test ! -f ${filename} ; then
        rm -f temp.db
        ${GENDATA} ${config} | sqlite3 temp.db
        mv temp.db ${filename}
    fi
done
