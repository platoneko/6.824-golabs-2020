#!/bin/sh
x=50
while [ $x -gt 0 ]
do
    echo "left test $x"
    go test -run 32C
    let x--
done