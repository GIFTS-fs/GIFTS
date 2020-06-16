#!/bin/bash

docs="docs"

echo "Builidng html documents to ${docs}"

subpackages=(algorithm bench client config generate master storage structure test)

mkdir -p "${docs}"

godoc -html . > "${docs}/index.html"

for package in "${subpackages[@]}"
do
    echo "    Building ${package}"
    mkdir -p "${docs}/${package}"
    godoc -html "./${package}" > "${docs}/${package}/index.html"
done

echo "Finished building ${docs}"
ls -lhA "${docs}"
