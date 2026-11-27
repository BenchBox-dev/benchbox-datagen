#!/bin/sh
set -eu
if [ "$#" -lt 4 ] || [ "$#" -gt 5 ]; then
    echo "usage: $0 <dir holding dsdgen and tpcds.idx> <scale> <sets> <output dir> [rng seed]" >&2
    exit 2
fi
bin=$1
scale=$2
sets=$3
out=$4
seed=${5:-}
mkdir -p "$out"
cp "$bin/tpcds.idx" "$out/"
cd "$out"
n=1
while [ "$n" -le "$sets" ]; do
    if [ -n "$seed" ]; then
        "$bin/dsdgen" -verbose n -force y -terminate n -scale "$scale" -update "$n" -rngseed "$seed" -dir . >/dev/null
    else
        "$bin/dsdgen" -verbose n -force y -terminate n -scale "$scale" -update "$n" -dir . >/dev/null
    fi
    n=$((n + 1))
done
rm -f tpcds.idx
