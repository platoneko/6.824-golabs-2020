#!/bin/sh
x=1
while [ $x -gt 0 ]
do
    echo "left test $x"
    go test -run 3A
    go test -run 3B
    let x--
done