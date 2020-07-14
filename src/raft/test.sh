#!/bin/sh
x=50
while [ $x -gt 0 ]
do
    echo "left test $x"
    go test -run Figure8Unreliable2C
    let x--
done