#!/bin/bash

benchmarks=(clientReadOne clientRead1000 clientReadTwo clientReadWave)

echo "Building starts..."

for b in ${benchmarks[@]}; do
  echo "Building the benchmark ${b}"
  go build -o "run-${b}" "${b}/${b}.go" 
done

echo "Finish building benchmarks"
echo
echo "    Usage: ./run-<some-benchmark> -label <some-label> -conf <some-non-default-config-file>.json"
echo "    The benchmarks (run as clients) use \"config-client.json\" by default"
echo
