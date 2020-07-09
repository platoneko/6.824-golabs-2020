#!/bin/sh
x=50
while [ $x -gt 0 ]
do
    echo "left test $x"
    go test -run Backup2B
    let x--
done