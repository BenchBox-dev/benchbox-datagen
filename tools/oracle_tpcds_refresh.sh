#!/bin/sh
set -eu
if [ "$#" -ne 4 ]; then
    echo "usage: $0 <dir holding dsdgen and tpcds.idx> <scale> <sets> <output dir>" >&2
    exit 2
fi
bin=$1
scale=$2
sets=$3
out=$4
mkdir -p "$out"
cp "$bin/tpcds.idx" "$out/"
cd "$out"
n=1
while [ "$n" -le "$sets" ]; do
    "$bin/dsdgen" -verbose n -force y -terminate n -scale "$scale" -update "$n" -dir . >/dev/null
    n=$((n + 1))
done
rm -f tpcds.idx
