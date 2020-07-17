#!/bin/sh
x=1
while [ $x -gt 0 ]
do
    echo "left test $x"
    go test -run 3B
    echo ""
    let x--
done