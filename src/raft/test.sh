#!/bin/sh
x=1
while [ $x -gt 0 ]
do
    echo "left test $x"
    go test -run 2A
    go test -run 2B
    go test -run 2C
    let x--
done